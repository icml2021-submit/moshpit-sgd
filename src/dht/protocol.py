from __future__ import annotations
import asyncio
from typing import Optional, List, Tuple, Dict, Any, Sequence, Union, Collection
import grpc
from src.dht.routing import RoutingTable, DHTID, BinaryDHTValue, DHTExpiration, Subkey
from src.dht.storage import DHTLocalStorage, DictionaryDHTValue
from src.proto import dht_pb2, dht_pb2_grpc as dht_grpc
from src.utils import Endpoint, get_logger, replace_port, MSGPackSerializer, ChannelCache, ValueWithExpiration
from src.utils import get_dht_time, GRPC_KEEPALIVE_OPTIONS, MAX_DHT_TIME_DISCREPANCY_SECONDS

logger = get_logger(__name__)


class DHTProtocol(dht_grpc.DHTServicer):
    node_id: DHTID
    port: int
    bucket_size: int
    num_replicas: int
    wait_timeout: float
    node_info: dht_pb2.NodeInfo
    channel_options: Tuple[Tuple[str, Any]]
    server: grpc.aio.Serve
    storage: DHTLocalStorage
    cache: DHTLocalStorage
    routing_table: RoutingTable
    rpc_semaphore: asyncio.Semaphore
    serializer = MSGPackSerializer
    RESERVED_SUBKEYS = IS_REGULAR_VALUE, IS_DICTIONARY = serializer.dumps(None), b''

    @classmethod
    async def create(cls, node_id: DHTID, bucket_size: int, depth_modulo: int, num_replicas: int, wait_timeout: float,
                     parallel_rpc: Optional[int] = None, cache_size: Optional[int] = None, listen=True,
                     listen_on='0.0.0.0:*', endpoint: Optional[Endpoint] = None,
                     channel_options: Sequence[Tuple[str, Any]] = (), **kwargs) -> DHTProtocol:
        self = cls(_initialized_with_create=True)
        self.node_id, self.bucket_size, self.num_replicas = node_id, bucket_size, num_replicas
        self.wait_timeout, self.channel_options = wait_timeout, tuple(channel_options)
        self.storage, self.cache = DHTLocalStorage(), DHTLocalStorage(maxsize=cache_size)
        self.routing_table = RoutingTable(node_id, bucket_size, depth_modulo)
        self.rpc_semaphore = asyncio.Semaphore(parallel_rpc if parallel_rpc is not None else float('inf'))
        if listen:
            grpc.aio.init_grpc_aio()
            self.server = grpc.aio.server(**kwargs, options=GRPC_KEEPALIVE_OPTIONS)
            dht_grpc.add_DHTServicer_to_server(self, self.server)
            self.port = self.server.add_insecure_port(listen_on)
            assert self.port != 0, f"Failed to listen to {listen_on}"
            if endpoint is not None and endpoint.endswith('*'):
                endpoint = replace_port(endpoint, self.port)
            self.node_info = dht_pb2.NodeInfo(node_id=node_id.to_bytes(), rpc_port=self.port,
                                              endpoint=endpoint or dht_pb2.NodeInfo.endpoint.DESCRIPTOR.default_value)
            await self.server.start()
        else:
            self.node_info, self.server, self.port = dht_pb2.NodeInfo(), None, None
            if listen_on != '0.0.0.0:*' or len(kwargs) != 0:
                logger.warning(
                    f"DHTProtocol has no server (due to listen=False), listen_on" f"and kwargs have no effect (unused kwargs: {kwargs})")
        return self

    def __init__(self, *, _initialized_with_create=False):
        assert _initialized_with_create, " Please use DHTProtocol.create coroutine to spawn new protocol instances "
        super().__init__()

    async def shutdown(self, timeout=None):
        if self.server:
            await self.server.stop(timeout)
        else:
            logger.warning("DHTProtocol has no server (due to listen=False), it doesn't need to be shut down")

    def _get_dht_stub(self, peer: Endpoint) -> dht_grpc.DHTStub:
        return ChannelCache.get_stub(peer, dht_grpc.DHTStub, aio=True, options=self.channel_options)

    async def call_ping(self, peer: Endpoint, validate: bool = False, strict: bool = True) -> Optional[DHTID]:
        try:
            async with self.rpc_semaphore:
                ping_request = dht_pb2.PingRequest(peer=self.node_info, validate=validate)
                time_requested = get_dht_time()
                response = await self._get_dht_stub(peer).rpc_ping(ping_request, timeout=self.wait_timeout)
                time_responded = get_dht_time()
        except grpc.aio.AioRpcError as error:
            logger.debug(f"DHTProtocol failed to ping {peer}: {error.code()}")
            response = None
        responded = bool(response and response.peer and response.peer.node_id)
        if responded and validate:
            try:
                if self.server is not None and not response.available:
                    raise ValidationError(
                        f"Peer {peer} couldn't access this node at {response.sender_endpoint} . " f"Make sure that this port is open for incoming requests.")
                if response.dht_time != dht_pb2.PingResponse.dht_time.DESCRIPTOR.default_value:
                    if response.dht_time < time_requested - MAX_DHT_TIME_DISCREPANCY_SECONDS or response.dht_time > time_responded + MAX_DHT_TIME_DISCREPANCY_SECONDS:
                        raise ValidationError(
                            f"local time must be within {MAX_DHT_TIME_DISCREPANCY_SECONDS} seconds " f" of others(local: {time_requested:.5f}, peer: {response.dht_time:.5f})")
            except ValidationError as e:
                if strict:
                    raise
                else:
                    logger.warning(repr(e))
        peer_id = DHTID.from_bytes(response.peer.node_id) if responded else None
        asyncio.create_task(self.update_routing_table(peer_id, peer, responded=responded))
        return peer_id

    async def get_outgoing_request_endpoint(self, peer: Endpoint) -> Optional[Endpoint]:
        try:
            async with self.rpc_semaphore:
                ping_request = dht_pb2.PingRequest(peer=None, validate=False)
                response = await self._get_dht_stub(peer).rpc_ping(ping_request, timeout=self.wait_timeout)
                if response.sender_endpoint != dht_pb2.PingResponse.sender_endpoint.DESCRIPTOR.default_value:
                    return response.sender_endpoint
        except grpc.aio.AioRpcError as error:
            logger.debug(f"DHTProtocol failed to ping {peer}: {error.code()}")

    async def rpc_ping(self, request: dht_pb2.PingRequest, context: grpc.ServicerContext):
        response = dht_pb2.PingResponse(peer=self.node_info, sender_endpoint=context.peer(), dht_time=get_dht_time(),
                                        available=False)
        if request.peer and request.peer.node_id and request.peer.rpc_port:
            sender_id = DHTID.from_bytes(request.peer.node_id)
            if request.peer.endpoint != dht_pb2.NodeInfo.endpoint.DESCRIPTOR.default_value:
                sender_endpoint = request.peer.endpoint
            else:
                sender_endpoint = replace_port(context.peer(), new_port=request.peer.rpc_port)
            response.sender_endpoint = sender_endpoint
            if request.validate:
                response.available = await self.call_ping(response.sender_endpoint, validate=False) == sender_id
            asyncio.create_task(self.update_routing_table(sender_id, sender_endpoint,
                                                          responded=response.available or not request.validate))
        return response

    async def call_store(self, peer: Endpoint, keys: Sequence[DHTID],
                         values: Sequence[Union[BinaryDHTValue, DictionaryDHTValue]],
                         expiration_time: Union[DHTExpiration, Sequence[DHTExpiration]],
                         subkeys: Optional[Union[Subkey, Sequence[Optional[Subkey]]]] = None,
                         in_cache: Optional[Union[bool, Sequence[bool]]] = None) -> Optional[List[bool]]:
        if isinstance(expiration_time, DHTExpiration):
            expiration_time = [expiration_time] * len(keys)
        if subkeys is None:
            subkeys = [None] * len(keys)
        in_cache = in_cache if in_cache is not None else [False] * len(keys)
        in_cache = [in_cache] * len(keys) if isinstance(in_cache, bool) else in_cache
        keys, subkeys, values, expiration_time, in_cache = map(list, [keys, subkeys, values, expiration_time, in_cache])
        for i in range(len(keys)):
            if subkeys[i] is None:
                subkeys[i] = self.IS_DICTIONARY if isinstance(values[i], DictionaryDHTValue) else self.IS_REGULAR_VALUE
            else:
                subkeys[i] = self.serializer.dumps(subkeys[i])
            if isinstance(values[i], DictionaryDHTValue):
                assert subkeys[i] == self.IS_DICTIONARY, "Please don't specify subkey when storing an entire dictionary"
                values[i] = self.serializer.dumps(values[i])
        assert len(keys) == len(values) == len(expiration_time) == len(in_cache), "Data is not aligned"
        store_request = dht_pb2.StoreRequest(keys=list(map(DHTID.to_bytes, keys)), subkeys=subkeys, values=values,
                                             expiration_time=expiration_time, in_cache=in_cache, peer=self.node_info)
        try:
            async with self.rpc_semaphore:
                response = await self._get_dht_stub(peer).rpc_store(store_request, timeout=self.wait_timeout)
            if response.peer and response.peer.node_id:
                peer_id = DHTID.from_bytes(response.peer.node_id)
                asyncio.create_task(self.update_routing_table(peer_id, peer, responded=True))
            return response.store_ok
        except grpc.aio.AioRpcError as error:
            logger.debug(f"DHTProtocol failed to store at {peer}: {error.code()}")
            asyncio.create_task(self.update_routing_table(self.routing_table.get(endpoint=peer), peer, responded=False))
            return None

    async def rpc_store(self, request: dht_pb2.StoreRequest, context: grpc.ServicerContext) -> dht_pb2.StoreResponse:
        if request.peer:
            asyncio.create_task(self.rpc_ping(dht_pb2.PingRequest(peer=request.peer), context))
        assert len(request.keys) == len(request.values) == len(request.expiration_time) == len(request.in_cache)
        response = dht_pb2.StoreResponse(store_ok=[], peer=self.node_info)
        keys = map(DHTID.from_bytes, request.keys)
        for key_id, tag, value_bytes, expiration_time, in_cache in zip(keys, request.subkeys, request.values,
                                                                       request.expiration_time, request.in_cache):
            storage = self.cache if in_cache else self.storage
            if tag == self.IS_REGULAR_VALUE:
                response.store_ok.append(storage.store(key_id, value_bytes, expiration_time))
            elif tag == self.IS_DICTIONARY:
                value_dictionary = self.serializer.loads(value_bytes)
                assert isinstance(value_dictionary, DictionaryDHTValue)
                response.store_ok.append(all(
                    storage.store_subkey(key_id, subkey, item.value, item.expiration_time) for subkey, item in
                    value_dictionary.items()))
            else:
                subkey = self.serializer.loads(tag)
                response.store_ok.append(storage.store_subkey(key_id, subkey, value_bytes, expiration_time))
        return response

    async def call_find(self, peer: Endpoint, keys: Collection[DHTID]) -> Optional[Dict[
        DHTID, Tuple[Optional[ValueWithExpiration[Union[BinaryDHTValue, DictionaryDHTValue]]], Dict[DHTID, Endpoint]]]]:
        keys = list(keys)
        find_request = dht_pb2.FindRequest(keys=list(map(DHTID.to_bytes, keys)), peer=self.node_info)
        try:
            async with self.rpc_semaphore:
                response = await self._get_dht_stub(peer).rpc_find(find_request, timeout=self.wait_timeout)
            if response.peer and response.peer.node_id:
                peer_id = DHTID.from_bytes(response.peer.node_id)
                asyncio.create_task(self.update_routing_table(peer_id, peer, responded=True))
            assert len(keys) == len(response.results), "DHTProtocol: response is not aligned with keys"
            output = {}
            for key, result in zip(keys, response.results):
                nearest = dict(zip(map(DHTID.from_bytes, result.nearest_node_ids), result.nearest_endpoints))
                if result.type == dht_pb2.NOT_FOUND:
                    output[key] = None, nearest
                elif result.type == dht_pb2.FOUND_REGULAR:
                    output[key] = ValueWithExpiration(result.value, result.expiration_time), nearest
                elif result.type == dht_pb2.FOUND_DICTIONARY:
                    deserialized_dictionary = self.serializer.loads(result.value)
                    output[key] = ValueWithExpiration(deserialized_dictionary, result.expiration_time), nearest
                else:
                    logger.error(f"Unknown result type: {result.type}")
            return output
        except grpc.aio.AioRpcError as error:
            logger.debug(f"DHTProtocol failed to find at {peer}: {error.code()}")
            asyncio.create_task(self.update_routing_table(self.routing_table.get(endpoint=peer), peer, responded=False))

    async def rpc_find(self, request: dht_pb2.FindRequest, context: grpc.ServicerContext) -> dht_pb2.FindResponse:
        if request.peer:
            asyncio.create_task(self.rpc_ping(dht_pb2.PingRequest(peer=request.peer), context))
        response = dht_pb2.FindResponse(results=[], peer=self.node_info)
        for i, key_id in enumerate(map(DHTID.from_bytes, request.keys)):
            maybe_item = self.storage.get(key_id)
            cached_item = self.cache.get(key_id)
            if cached_item is not None and (
                    maybe_item is None or cached_item.expiration_time > maybe_item.expiration_time):
                maybe_item = cached_item
            if maybe_item is None:
                item = dht_pb2.FindResult(type=dht_pb2.NOT_FOUND)
            elif isinstance(maybe_item.value, DictionaryDHTValue):
                item = dht_pb2.FindResult(type=dht_pb2.FOUND_DICTIONARY, value=self.serializer.dumps(maybe_item.value),
                                          expiration_time=maybe_item.expiration_time)
            else:
                item = dht_pb2.FindResult(type=dht_pb2.FOUND_REGULAR, value=maybe_item.value,
                                          expiration_time=maybe_item.expiration_time)
            for node_id, endpoint in self.routing_table.get_nearest_neighbors(key_id, k=self.bucket_size,
                                                                              exclude=DHTID.from_bytes(
                                                                                      request.peer.node_id)):
                item.nearest_node_ids.append(node_id.to_bytes())
                item.nearest_endpoints.append(endpoint)
            response.results.append(item)
        return response

    async def update_routing_table(self, node_id: Optional[DHTID], peer_endpoint: Endpoint, responded=True):
        node_id = node_id if node_id is not None else self.routing_table.get(endpoint=peer_endpoint)
        if responded:
            if node_id not in self.routing_table:
                data_to_send: List[Tuple[DHTID, BinaryDHTValue, DHTExpiration]] = []
                for key, item in list(self.storage.items()):
                    neighbors = self.routing_table.get_nearest_neighbors(key, self.num_replicas, exclude=self.node_id)
                    if neighbors:
                        nearest_distance = neighbors[0][0].xor_distance(key)
                        farthest_distance = neighbors[-1][0].xor_distance(key)
                        new_node_should_store = node_id.xor_distance(key) < farthest_distance
                        this_node_is_responsible = self.node_id.xor_distance(key) < nearest_distance
                    if not neighbors or (new_node_should_store and this_node_is_responsible):
                        data_to_send.append((key, item.value, item.expiration_time))
                if data_to_send:
                    asyncio.create_task(self.call_store(peer_endpoint, *zip(*data_to_send), in_cache=False))
            maybe_node_to_ping = self.routing_table.add_or_update_node(node_id, peer_endpoint)
            if maybe_node_to_ping is not None:
                asyncio.create_task(self.call_ping(maybe_node_to_ping[1]))
        else:
            if node_id is not None and node_id in self.routing_table:
                del self.routing_table[node_id]


class ValidationError(Exception):
    pass
