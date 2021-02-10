from __future__ import annotations

import asyncio
import ctypes
import multiprocessing as mp
import re
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Optional, Sequence, Union, Dict, NamedTuple

from src.dht.node import DHTNode, DHTID, DHTExpiration
from src.dht.routing import get_dht_time, DHTValue, DHTKey, Subkey
from src.utils import MPFuture, Endpoint, Hostname, get_logger, switch_to_uvloop, strip_port, ValueWithExpiration

logger = get_logger(__name__)

ExpertUID, ExpertPrefix, Coordinate = str, str, int
UidEndpoint = NamedTuple("UidEndpoint", [('uid', ExpertUID), ('endpoint', Endpoint)])
UID_DELIMITER = '.'
UID_PATTERN = re.compile('^(([^.])+)([.](?:[0]|([1-9]([0-9]*))))+$')
PREFIX_PATTERN = re.compile('^(([^.])+)([.](?:[0]|([1-9]([0-9]*))))*[.]$')


def is_valid_prefix(maybe_prefix: str) -> bool:
    return bool(PREFIX_PATTERN.fullmatch(maybe_prefix))


class DHT(mp.Process):

    def __init__(self, listen_on: Endpoint = "0.0.0.0:*", initial_peers: Sequence[Endpoint] = (), *, start: bool,
                 daemon: bool = True, max_workers: Optional[int] = None, parallel_rpc: Optional[int] = None,
                 receiver_threads: int = 1, negative_caching: bool = True, expiration: float = 300, **kwargs):
        super().__init__()
        assert not isinstance(initial_peers, str), "please specify a list/tuple of initial peers (even if there's one)"
        self.listen_on, self.initial_peers, self.kwargs = listen_on, initial_peers, kwargs
        self.receiver_threads, self.max_workers, self.parallel_rpc = receiver_threads, max_workers, parallel_rpc
        self.expiration, self.negative_caching = expiration, negative_caching
        self._port = mp.Value(ctypes.c_int32, 0)  # initialized after dht starts
        self._pipe, self.pipe = mp.Pipe(duplex=True)
        self.ready = mp.Event()
        self.daemon = daemon
        if start:
            self.run_in_background(await_ready=True)

    def run(self) -> None:
        loop = switch_to_uvloop()
        pipe_awaiter = ThreadPoolExecutor(self.receiver_threads)

        async def _run():
            node = await DHTNode.create(
                initial_peers=list(self.initial_peers), listen_on=self.listen_on, parallel_rpc=self.parallel_rpc,
                num_workers=self.max_workers or 1, **self.kwargs)
            if node.port is not None:
                self._port.value = node.port
            self.ready.set()

            while True:
                method, args, kwargs = await loop.run_in_executor(pipe_awaiter, self._pipe.recv)
                asyncio.create_task(getattr(self, method)(node, *args, **kwargs))

        try:
            loop.run_until_complete(_run())
        except KeyboardInterrupt:
            logger.debug("Caught KeyboardInterrupt, shutting down")

    def run_in_background(self, await_ready=True, timeout=None):
        self.start()
        if await_ready and not self.ready.wait(timeout=timeout):
            raise TimeoutError("Server didn't notify .ready in {timeout} seconds")

    def shutdown(self) -> None:
        if self.is_alive():
            self.terminate()
        else:
            logger.warning("DHT shutdown has no effect: dht process is already not alive")

    @property
    def port(self) -> Optional[int]:
        return self._port.value if self._port.value != 0 else None

    def get(self, key: DHTKey, latest: bool = False, return_future: bool = False, **kwargs
            ) -> Union[Optional[ValueWithExpiration[DHTValue]], MPFuture]:
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_get', [], dict(key=key, latest=latest, future=_future, **kwargs)))
        return future if return_future else future.result()

    async def _get(self, node: DHTNode, key: DHTKey, latest: bool, future: MPFuture, **kwargs):
        try:
            result = await node.get(key, latest=latest, **kwargs)
            if not future.done():
                future.set_result(result)
        except BaseException as e:
            if not future.done():
                future.set_exception(e)
            raise

    def store(self, key: DHTKey, value: DHTValue, expiration_time: DHTExpiration,
              subkey: Optional[Subkey] = None, return_future: bool = False, **kwargs) -> Union[bool, MPFuture]:
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_store', [], dict(key=key, value=value, expiration_time=expiration_time, subkey=subkey,
                                           future=_future, **kwargs)))
        return future if return_future else future.result()

    async def _store(self, node: DHTNode, key: DHTKey, value: DHTValue, expiration_time: DHTExpiration,
                     subkey: Optional[Subkey], future: MPFuture, **kwargs):
        try:
            result = await node.store(key, value, expiration_time, subkey=subkey, **kwargs)
            if not future.done():
                future.set_result(result)
        except BaseException as e:
            if not future.done():
                future.set_exception(e)
            raise

    def get_visible_address(self, num_peers: Optional[int] = None, peers: Sequence[Endpoint] = ()) -> Hostname:
        assert num_peers is None or peers == (), "please specify either a num_peers or the list of peers, not both"
        assert not isinstance(peers, str) and isinstance(peers, Sequence), "Please send a list / tuple of endpoints"
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_get_visible_address', [], dict(num_peers=num_peers, peers=peers, future=_future)))
        return future.result()

    async def _get_visible_address(self, node: DHTNode, num_peers: Optional[int], peers: Sequence[Endpoint],
                                   future: Optional[MPFuture]):
        if not peers and (num_peers or not node.protocol.node_info.endpoint):
            peers_and_endpoints = node.protocol.routing_table.get_nearest_neighbors(
                DHTID.generate(), num_peers or 1, exclude=node.node_id)
            peers = tuple(endpoint for node_id, endpoint in peers_and_endpoints)

        chosen_address = None
        if peers:
            possible_endpoints: Sequence[Optional[Endpoint]] = await asyncio.gather(*(
                node.protocol.get_outgoing_request_endpoint(peer) for peer in peers))

            for endpoint in possible_endpoints:
                if endpoint is None:
                    continue
                address = strip_port(endpoint)
                if chosen_address is not None and address != chosen_address:
                    logger.warning("At least two peers returned different visible addresses for this node:"
                                   f"{address} and {chosen_address} (keeping the former one)")
                else:
                    chosen_address = address

            if chosen_address is None:
                logger.warning(f"None of the selected peers responded with an address ({peers})")

        if node.protocol.node_info.endpoint:
            address = strip_port(node.protocol.node_info.endpoint)
            if chosen_address is not None and address != chosen_address:
                logger.warning(f"Node was manually given endpoint {address} , but other peers report {chosen_address}")
            chosen_address = chosen_address or address

        if chosen_address:
            future.set_result(chosen_address)
        else:
            future.set_exception(ValueError(f"Can't get address: DHT node has no peers and no public endpoint."
                                            f" Please ensure the node is connected or specify peers=... manually."))

    def get_active_successors(self, prefixes: List[ExpertPrefix], grid_size: Optional[int] = None,
                              num_workers: Optional[int] = None, return_future: bool = False
                              ) -> Dict[ExpertPrefix, Dict[Coordinate, UidEndpoint]]:
        assert not isinstance(prefixes, str), "Please send a list / tuple of expert prefixes."
        for prefix in prefixes:
            assert is_valid_prefix(prefix), f"prefix '{prefix}' is invalid, it must follow {PREFIX_PATTERN.pattern}"
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_get_active_successors', [], dict(
            prefixes=list(prefixes), grid_size=grid_size, num_workers=num_workers, future=_future)))
        return future if return_future else future.result()

    async def _get_active_successors(self, node: DHTNode, prefixes: List[ExpertPrefix], grid_size: Optional[int] = None,
                                     num_workers: Optional[int] = None, future: Optional[MPFuture] = None
                                     ) -> Dict[ExpertPrefix, Dict[Coordinate, UidEndpoint]]:
        grid_size = grid_size or float('inf')
        num_workers = num_workers or min(len(prefixes), self.max_workers or len(prefixes))
        dht_responses = await node.get_many(keys=prefixes, num_workers=num_workers)
        successors: Dict[ExpertPrefix, Dict[Coordinate, UidEndpoint]] = {}
        for prefix, found in dht_responses.items():
            if found and isinstance(found.value, dict):
                successors[prefix] = {coord: UidEndpoint(*match.value) for coord, match in found.value.items()
                                      if isinstance(coord, Coordinate) and 0 <= coord < grid_size
                                      and isinstance(getattr(match, 'value', None), list) and len(match.value) == 2}
            else:
                successors[prefix] = {}
                if found is None and self.negative_caching:
                    logger.debug(f"DHT negative caching: storing a 'no prefix' entry for {prefix}")
                    asyncio.create_task(node.store(prefix, subkey=-1, value=None,
                                                   expiration_time=get_dht_time() + self.expiration))
        if future:
            future.set_result(successors)
        return successors
