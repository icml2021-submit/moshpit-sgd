from __future__ import annotations
import asyncio
import random
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from functools import partial
from typing import Optional, Tuple, List, Dict, DefaultDict, Collection, Union, Set, Awaitable, Callable, Any
from sortedcontainers import SortedSet
from src.dht.protocol import DHTProtocol
from src.dht.routing import DHTID, DHTExpiration, DHTKey, get_dht_time, DHTValue, BinaryDHTValue, Subkey
from src.dht.storage import DictionaryDHTValue
from src.dht.traverse import traverse_dht
from src.utils import Endpoint, LOCALHOST, MSGPackSerializer, get_logger, SerializerBase
from src.utils.timed_storage import TimedStorage, ValueWithExpiration

logger = get_logger(__name__)


class DHTNode:
    node_id: DHTID;
    is_alive: bool;
    port: int;
    num_replicas: int;
    num_workers: int;
    protocol: DHTProtocol
    chunk_size: int;
    refresh_timeout: float;
    cache_locally: bool;
    cache_nearest: int;
    cache_refresh_before_expiry: float
    cache_on_store: bool;
    reuse_get_requests: bool;
    pending_get_requests: DefaultDict[DHTID, SortedSet[_SearchState]]
    cache_refresh_task: Optional[asyncio.Task];
    cache_refresh_evt: asyncio.Event;
    cache_refresh_queue: CacheRefreshQueue
    blacklist: Blacklist

    @classmethod
    async def create(cls, node_id: Optional[DHTID] = None, initial_peers: List[Endpoint] = (), bucket_size: int = 20,
                     num_replicas: int = 5, depth_modulo: int = 5, parallel_rpc: int = None, wait_timeout: float = 3,
                     refresh_timeout: Optional[float] = None, bootstrap_timeout: Optional[float] = None,
                     cache_locally: bool = True, cache_nearest: int = 1, cache_size=None,
                     cache_refresh_before_expiry: float = 5, cache_on_store: bool = True,
                     reuse_get_requests: bool = True, num_workers: int = 1, chunk_size: int = 16,
                     blacklist_time: float = 5.0, backoff_rate: float = 2.0, listen: bool = True,
                     listen_on: Endpoint = "0.0.0.0:*", endpoint: Optional[Endpoint] = None, validate: bool = True,
                     strict: bool = True, **kwargs) -> DHTNode:
        self = cls(_initialized_with_create=True)
        self.node_id = node_id = node_id if node_id is not None else DHTID.generate()
        self.num_replicas, self.num_workers, self.chunk_size = num_replicas, num_workers, chunk_size
        self.is_alive = True
        self.reuse_get_requests = reuse_get_requests
        self.pending_get_requests = defaultdict(partial(SortedSet, key=lambda _res: -_res.sufficient_expiration_time))
        self.refresh_timeout = refresh_timeout
        self.cache_locally, self.cache_nearest, self.cache_on_store = cache_locally, cache_nearest, cache_on_store
        self.cache_refresh_before_expiry = cache_refresh_before_expiry
        self.blacklist = Blacklist(blacklist_time, backoff_rate)
        self.cache_refresh_queue = CacheRefreshQueue()
        self.cache_refresh_evt = asyncio.Event()
        self.cache_refresh_task = None
        self.protocol = await DHTProtocol.create(self.node_id, bucket_size, depth_modulo, num_replicas, wait_timeout,
                                                 parallel_rpc, cache_size, listen, listen_on, endpoint, **kwargs)
        self.port = self.protocol.port
        if initial_peers:
            bootstrap_timeout = bootstrap_timeout if bootstrap_timeout is not None else wait_timeout
            start_time = get_dht_time()
            ping_tasks = set(
                asyncio.create_task(self.protocol.call_ping(peer, validate=validate, strict=strict)) for peer in
                initial_peers)
            finished_pings, unfinished_pings = await asyncio.wait(ping_tasks, return_when=asyncio.FIRST_COMPLETED)
            if unfinished_pings:
                finished_in_time, stragglers = await asyncio.wait(unfinished_pings,
                                                                  timeout=bootstrap_timeout - get_dht_time() + start_time)
                for straggler in stragglers:
                    straggler.cancel()
                finished_pings |= finished_in_time
            if not finished_pings or all(ping.result() is None for ping in finished_pings):
                logger.warning("DHTNode bootstrap failed: none of the initial_peers responded to a ping.")
            if strict:
                for task in asyncio.as_completed(finished_pings):
                    await task
            await asyncio.wait([asyncio.create_task(self.find_nearest_nodes([self.node_id])),
                                asyncio.sleep(bootstrap_timeout - get_dht_time() + start_time)],
                               return_when=asyncio.FIRST_COMPLETED)
        if self.refresh_timeout is not None:
            asyncio.create_task(self._refresh_routing_table(period=self.refresh_timeout))
        return self

    def __init__(self, *, _initialized_with_create=False):
        assert _initialized_with_create, " Please use DHTNode.create coroutine to spawn new node instances "
        super().__init__()

    async def shutdown(self, timeout=None):
        self.is_alive = False
        if self.protocol.server:
            await self.protocol.shutdown(timeout)

    async def find_nearest_nodes(self, queries: Collection[DHTID], k_nearest: Optional[int] = None,
                                 beam_size: Optional[int] = None, num_workers: Optional[int] = None,
                                 node_to_endpoint: Optional[Dict[DHTID, Endpoint]] = None, exclude_self: bool = False,
                                 **kwargs) -> Dict[DHTID, Dict[DHTID, Endpoint]]:
        queries = tuple(queries)
        k_nearest = k_nearest if k_nearest is not None else self.protocol.bucket_size
        num_workers = num_workers if num_workers is not None else self.num_workers
        beam_size = beam_size if beam_size is not None else max(self.protocol.bucket_size, k_nearest)
        if k_nearest > beam_size:
            logger.warning("Warning: beam_size is too small, beam search is not guaranteed to find enough nodes")
        if node_to_endpoint is None:
            node_to_endpoint: Dict[DHTID, Endpoint] = dict()
            for query in queries:
                neighbors = self.protocol.routing_table.get_nearest_neighbors(query, beam_size, exclude=self.node_id)
                node_to_endpoint.update(self._filter_blacklisted(dict(neighbors)))

        async def get_neighbors(peer: DHTID, queries: Collection[DHTID]) -> Dict[DHTID, Tuple[Tuple[DHTID], bool]]:
            response = await self._call_find_with_blacklist(node_to_endpoint[peer], queries)
            if not response:
                return {query: ([], False) for query in queries}
            output: Dict[DHTID, Tuple[Tuple[DHTID], bool]] = {}
            for query, (_, peers) in response.items():
                node_to_endpoint.update(peers)
                output[query] = tuple(peers.keys()), False
            return output

        nearest_nodes_per_query, visited_nodes = await traverse_dht(queries, initial_nodes=list(node_to_endpoint),
                                                                    beam_size=beam_size, num_workers=num_workers,
                                                                    queries_per_call=int(len(queries) ** 0.5),
                                                                    get_neighbors=get_neighbors,
                                                                    visited_nodes={query: {self.node_id} for query in
                                                                                   queries}, **kwargs)
        nearest_nodes_with_endpoints = {}
        for query, nearest_nodes in nearest_nodes_per_query.items():
            if not exclude_self:
                nearest_nodes = sorted(nearest_nodes + [self.node_id], key=query.xor_distance)
                node_to_endpoint[self.node_id] = f"{LOCALHOST}:{self.port}"
            nearest_nodes_with_endpoints[query] = {node: node_to_endpoint[node] for node in nearest_nodes[:k_nearest]}
        return nearest_nodes_with_endpoints

    async def store(self, key: DHTKey, value: DHTValue, expiration_time: DHTExpiration, subkey: Optional[Subkey] = None,
                    **kwargs) -> bool:
        store_ok = await self.store_many([key], [value], [expiration_time], subkeys=[subkey], **kwargs)
        return store_ok[(key, subkey) if subkey is not None else key]

    async def store_many(self, keys: List[DHTKey], values: List[DHTValue],
                         expiration_time: Union[DHTExpiration, List[DHTExpiration]],
                         subkeys: Optional[Union[Subkey, List[Optional[Subkey]]]] = None, exclude_self: bool = False,
                         await_all_replicas=True, **kwargs) -> Dict[DHTKey, bool]:
        if isinstance(expiration_time, DHTExpiration):
            expiration_time = [expiration_time] * len(keys)
        if subkeys is None:
            subkeys = [None] * len(keys)
        assert len(keys) == len(subkeys) == len(values) == len(
            expiration_time), "Either of keys, values, subkeys or expiration timestamps have different sequence lengths."
        key_id_to_data: DefaultDict[DHTID, List[Tuple[DHTKey, Subkey, DHTValue, DHTExpiration]]] = defaultdict(list)
        for key, subkey, value, expiration in zip(keys, subkeys, values, expiration_time):
            key_id_to_data[DHTID.generate(source=key)].append((key, subkey, value, expiration))
        unfinished_key_ids = set(key_id_to_data.keys())
        store_ok = {(key, subkey): None for key, subkey in zip(keys, subkeys)}
        store_finished_events = {(key, subkey): asyncio.Event() for key, subkey in zip(keys, subkeys)}
        node_to_endpoint: Dict[DHTID, Endpoint] = dict()
        for key_id in unfinished_key_ids:
            node_to_endpoint.update(self.protocol.routing_table.get_nearest_neighbors(key_id, self.protocol.bucket_size,
                                                                                      exclude=self.node_id))

        async def on_found(key_id: DHTID, nearest_nodes: List[DHTID], visited_nodes: Set[DHTID]) -> None:
            assert key_id in unfinished_key_ids, "Internal error: traverse_dht finished the same query twice"
            assert self.node_id not in nearest_nodes
            unfinished_key_ids.remove(key_id)
            num_successful_stores = 0
            pending_store_tasks = set()
            store_candidates = sorted(nearest_nodes + ([] if exclude_self else [self.node_id]), key=key_id.xor_distance,
                                      reverse=True)
            [original_key, *_], current_subkeys, current_values, current_expirations = zip(*key_id_to_data[key_id])
            binary_values: List[bytes] = list(map(self.protocol.serializer.dumps, current_values))
            while num_successful_stores < self.num_replicas and (store_candidates or pending_store_tasks):
                while store_candidates and num_successful_stores + len(pending_store_tasks) < self.num_replicas:
                    node_id: DHTID = store_candidates.pop()
                    if node_id == self.node_id:
                        num_successful_stores += 1
                        for subkey, value, expiration_time in zip(current_subkeys, binary_values, current_expirations):
                            store_ok[original_key, subkey] = self.protocol.storage.store(key_id, value, expiration_time,
                                                                                         subkey=subkey)
                            if not await_all_replicas:
                                store_finished_events[original_key, subkey].set()
                    else:
                        pending_store_tasks.add(asyncio.create_task(
                            self.protocol.call_store(node_to_endpoint[node_id], keys=[key_id] * len(current_values),
                                                     values=binary_values, expiration_time=current_expirations,
                                                     subkeys=current_subkeys)))
                if pending_store_tasks:
                    finished_store_tasks, pending_store_tasks = await asyncio.wait(pending_store_tasks,
                                                                                   return_when=asyncio.FIRST_COMPLETED)
                    for task in finished_store_tasks:
                        if task.result() is not None:
                            num_successful_stores += 1
                            for subkey, store_status in zip(current_subkeys, task.result()):
                                store_ok[original_key, subkey] = store_status
                                if not await_all_replicas:
                                    store_finished_events[original_key, subkey].set()
            if self.cache_on_store:
                self._update_cache_on_store(key_id, current_subkeys, binary_values, current_expirations,
                                            store_ok=[store_ok[original_key, subkey] for subkey in current_subkeys])
            for subkey, value_bytes, expiration in zip(current_subkeys, binary_values, current_expirations):
                store_finished_events[original_key, subkey].set()

        store_task = asyncio.create_task(
            self.find_nearest_nodes(queries=set(unfinished_key_ids), k_nearest=self.num_replicas,
                                    node_to_endpoint=node_to_endpoint, found_callback=on_found,
                                    exclude_self=exclude_self, **kwargs))
        try:
            await asyncio.wait([evt.wait() for evt in store_finished_events.values()])
            assert len(unfinished_key_ids) == 0, "Internal error: traverse_dht didn't finish search"
            return {(key, subkey) if subkey else key: status or False for (key, subkey), status in store_ok.items()}
        except asyncio.CancelledError as e:
            store_task.cancel()
            raise e

    def _update_cache_on_store(self, key_id: DHTID, subkeys: List[Subkey], binary_values: List[bytes],
                               expirations: List[DHTExpiration], store_ok: List[bool]):
        store_succeeded = any(store_ok)
        is_dictionary = any(subkey is not None for subkey in subkeys)
        if store_succeeded and not is_dictionary:
            stored_value_bytes, stored_expiration = max(zip(binary_values, expirations), key=lambda p: p[1])
            self.protocol.cache.store(key_id, stored_value_bytes, stored_expiration)
        elif not store_succeeded and not is_dictionary:
            rejected_value, rejected_expiration = max(zip(binary_values, expirations), key=lambda p: p[1])
            if (self.protocol.cache.get(key_id)[1] or float("inf")) <= rejected_expiration:
                self._schedule_for_refresh(key_id, refresh_time=get_dht_time())
        elif is_dictionary and key_id in self.protocol.cache:
            for subkey, stored_value_bytes, expiration_time in zip(subkeys, binary_values, expirations):
                self.protocol.cache.store_subkey(key_id, subkey, stored_value_bytes, expiration_time)
            self._schedule_for_refresh(key_id, refresh_time=get_dht_time())

    async def get(self, key: DHTKey, latest=False, **kwargs) -> Optional[ValueWithExpiration[DHTValue]]:
        if latest:
            kwargs["sufficient_expiration_time"] = float('inf')
        result = await self.get_many([key], **kwargs)
        return result[key]

    async def get_many(self, keys: Collection[DHTKey], sufficient_expiration_time: Optional[DHTExpiration] = None,
                       **kwargs) -> Dict[
        DHTKey, Union[Optional[ValueWithExpiration[DHTValue]], Awaitable[Optional[ValueWithExpiration[DHTValue]]]]]:
        keys = tuple(keys)
        key_ids = [DHTID.generate(key) for key in keys]
        id_to_original_key = dict(zip(key_ids, keys))
        results_by_id = await self.get_many_by_id(key_ids, sufficient_expiration_time, **kwargs)
        return {id_to_original_key[key]: result_or_future for key, result_or_future in results_by_id.items()}

    async def get_many_by_id(self, key_ids: Collection[DHTID],
                             sufficient_expiration_time: Optional[DHTExpiration] = None,
                             num_workers: Optional[int] = None, beam_size: Optional[int] = None,
                             return_futures: bool = False, _is_refresh=False) -> Dict[
        DHTID, Union[Optional[ValueWithExpiration[DHTValue]], Awaitable[Optional[ValueWithExpiration[DHTValue]]]]]:
        sufficient_expiration_time = sufficient_expiration_time or get_dht_time()
        beam_size = beam_size if beam_size is not None else self.protocol.bucket_size
        num_workers = num_workers if num_workers is not None else self.num_workers
        search_results: Dict[DHTID, _SearchState] = {
            key_id: _SearchState(key_id, sufficient_expiration_time, serializer=self.protocol.serializer) for key_id in
            key_ids}
        if not _is_refresh:
            for key_id in key_ids:
                search_results[key_id].add_done_callback(self._trigger_cache_refresh)
        if self.reuse_get_requests:
            for key_id, search_result in search_results.items():
                self.pending_get_requests[key_id].add(search_result)
                search_result.add_done_callback(self._reuse_finished_search_result)
        for key_id in key_ids:
            search_results[key_id].add_candidate(self.protocol.storage.get(key_id), source_node_id=self.node_id)
            if not _is_refresh:
                search_results[key_id].add_candidate(self.protocol.cache.get(key_id), source_node_id=self.node_id)
        unfinished_key_ids = [key_id for key_id in key_ids if not search_results[key_id].finished]
        node_to_endpoint: Dict[DHTID, Endpoint] = dict()
        for key_id in unfinished_key_ids:
            node_to_endpoint.update(self.protocol.routing_table.get_nearest_neighbors(key_id, self.protocol.bucket_size,
                                                                                      exclude=self.node_id))

        async def get_neighbors(peer: DHTID, queries: Collection[DHTID]) -> Dict[DHTID, Tuple[Tuple[DHTID], bool]]:
            queries = list(queries)
            response = await self._call_find_with_blacklist(node_to_endpoint[peer], queries)
            if not response:
                return {query: ([], False) for query in queries}
            output: Dict[DHTID, Tuple[Tuple[DHTID], bool]] = {}
            for key_id, (maybe_value_with_expiration, peers) in response.items():
                node_to_endpoint.update(peers)
                search_results[key_id].add_candidate(maybe_value_with_expiration, source_node_id=peer)
                output[key_id] = tuple(peers.keys()), search_results[key_id].finished
            return output

        async def found_callback(key_id: DHTID, nearest_nodes: List[DHTID], _visited: Set[DHTID]):
            search_results[key_id].finish_search()
            self._cache_new_result(search_results[key_id], nearest_nodes, node_to_endpoint, _is_refresh=_is_refresh)

        asyncio.create_task(
            traverse_dht(queries=list(unfinished_key_ids), initial_nodes=list(node_to_endpoint), beam_size=beam_size,
                         num_workers=num_workers,
                         queries_per_call=min(int(len(unfinished_key_ids) ** 0.5), self.chunk_size),
                         get_neighbors=get_neighbors,
                         visited_nodes={key_id: {self.node_id} for key_id in unfinished_key_ids},
                         found_callback=found_callback, await_all_tasks=False))
        if return_futures:
            return {key_id: search_result.future for key_id, search_result in search_results.items()}
        else:
            try:
                return {key_id: await search_result.future for key_id, search_result in search_results.items()}
            except asyncio.CancelledError as e:
                for key_id, search_result in search_results.items():
                    search_result.future.cancel()
                raise e

    def _reuse_finished_search_result(self, finished: _SearchState):
        pending_requests = self.pending_get_requests[finished.key_id]
        if finished.found_something:
            search_result = ValueWithExpiration(finished.binary_value, finished.expiration_time)
            expiration_time_threshold = max(finished.expiration_time, finished.sufficient_expiration_time)
            while pending_requests and expiration_time_threshold >= pending_requests[-1].sufficient_expiration_time:
                pending_requests[-1].add_candidate(search_result, source_node_id=finished.source_node_id)
                pending_requests[-1].finish_search()
                pending_requests.pop()
        else:
            pending_requests.discard(finished)

    async def _call_find_with_blacklist(self, endpoint: Endpoint, keys: Collection[DHTID]):
        if endpoint in self.blacklist:
            return None
        response = await self.protocol.call_find(endpoint, keys)
        if response:
            self.blacklist.register_success(endpoint)
            return {key: (maybe_value, self._filter_blacklisted(nearest_peers)) for key, (maybe_value, nearest_peers) in
                    response.items()}
        else:
            self.blacklist.register_failure(endpoint)
            return None

    def _filter_blacklisted(self, peer_endpoints: Dict[DHTID, Endpoint]):
        return {peer: endpoint for peer, endpoint in peer_endpoints.items() if endpoint not in self.blacklist}

    def _trigger_cache_refresh(self, search: _SearchState):
        if search.found_something and search.source_node_id == self.node_id:
            if self.cache_refresh_before_expiry and search.key_id in self.protocol.cache:
                self._schedule_for_refresh(search.key_id, search.expiration_time - self.cache_refresh_before_expiry)

    def _schedule_for_refresh(self, key_id: DHTID, refresh_time: DHTExpiration):
        if self.cache_refresh_task is None or self.cache_refresh_task.done() or self.cache_refresh_task.cancelled():
            self.cache_refresh_task = asyncio.create_task(self._refresh_stale_cache_entries())
            logger.debug("Spawned cache refresh task.")
        earliest_key, earliest_item = self.cache_refresh_queue.top()
        if earliest_item is None or refresh_time < earliest_item.expiration_time:
            self.cache_refresh_evt.set()
        self.cache_refresh_queue.store(key_id, value=refresh_time, expiration_time=refresh_time)

    async def _refresh_stale_cache_entries(self):
        while self.is_alive:
            while len(self.cache_refresh_queue) == 0:
                await self.cache_refresh_evt.wait()
                self.cache_refresh_evt.clear()
            key_id, (_, nearest_refresh_time) = self.cache_refresh_queue.top()
            try:
                time_to_wait = nearest_refresh_time - get_dht_time()
                await asyncio.wait_for(self.cache_refresh_evt.wait(), timeout=time_to_wait)
                self.cache_refresh_evt.clear()
                continue
            except asyncio.TimeoutError:
                current_time = get_dht_time()
                keys_to_refresh = {key_id}
                max_expiration_time = nearest_refresh_time
                del self.cache_refresh_queue[key_id]
                while self.cache_refresh_queue and len(keys_to_refresh) < self.chunk_size:
                    key_id, (_, nearest_refresh_time) = self.cache_refresh_queue.top()
                    if nearest_refresh_time > current_time:
                        break
                    del self.cache_refresh_queue[key_id]
                    keys_to_refresh.add(key_id)
                    cached_item = self.protocol.cache.get(key_id)
                    if cached_item is not None and cached_item.expiration_time > max_expiration_time:
                        max_expiration_time = cached_item.expiration_time
                sufficient_expiration_time = max_expiration_time + self.cache_refresh_before_expiry + 1
                await self.get_many_by_id(keys_to_refresh, sufficient_expiration_time, _is_refresh=True)

    def _cache_new_result(self, search: _SearchState, nearest_nodes: List[DHTID],
                          node_to_endpoint: Dict[DHTID, Endpoint], _is_refresh: bool = False):
        if search.found_something:
            _, storage_expiration_time = self.protocol.storage.get(search.key_id) or (None, -float('inf'))
            _, cache_expiration_time = self.protocol.cache.get(search.key_id) or (None, -float('inf'))
            if search.expiration_time > max(storage_expiration_time, cache_expiration_time):
                if self.cache_locally or _is_refresh:
                    self.protocol.cache.store(search.key_id, search.binary_value, search.expiration_time)
                if self.cache_nearest:
                    num_cached_nodes = 0
                    for node_id in nearest_nodes:
                        if node_id == search.source_node_id:
                            continue
                        asyncio.create_task(
                            self.protocol.call_store(node_to_endpoint[node_id], [search.key_id], [search.binary_value],
                                                     [search.expiration_time], in_cache=True))
                        num_cached_nodes += 1
                        if num_cached_nodes >= self.cache_nearest:
                            break

    async def _refresh_routing_table(self, *, period: Optional[float]) -> None:
        while self.is_alive and period is not None:
            refresh_time = get_dht_time()
            staleness_threshold = refresh_time - period
            stale_buckets = [bucket for bucket in self.protocol.routing_table.buckets if
                             bucket.last_updated < staleness_threshold]
            for bucket in stale_buckets:
                refresh_id = DHTID(random.randint(bucket.lower, bucket.upper - 1))
                await self.find_nearest_nodes(refresh_id)
            await asyncio.sleep(max(0.0, period - (get_dht_time() - refresh_time)))


@dataclass(init=True, repr=True, frozen=False, order=False)
class _SearchState:
    key_id: DHTID
    sufficient_expiration_time: DHTExpiration
    binary_value: Optional[Union[BinaryDHTValue, DictionaryDHTValue]] = None
    expiration_time: Optional[DHTExpiration] = None
    source_node_id: Optional[DHTID] = None
    future: asyncio.Future[Optional[ValueWithExpiration[DHTValue]]] = field(default_factory=asyncio.Future)
    serializer: type(SerializerBase) = MSGPackSerializer

    def add_candidate(self, candidate: Optional[ValueWithExpiration[Union[BinaryDHTValue, DictionaryDHTValue]]],
                      source_node_id: Optional[DHTID]):
        if self.finished or candidate is None:
            return
        elif isinstance(candidate.value, DictionaryDHTValue) and isinstance(self.binary_value, DictionaryDHTValue):
            self.binary_value.maxsize = max(self.binary_value.maxsize, candidate.value.maxsize)
            for subkey, subentry in candidate.value.items():
                self.binary_value.store(subkey, subentry.value, subentry.expiration_time)
        elif candidate.expiration_time > (self.expiration_time or float('-inf')):
            self.binary_value = candidate.value
        if candidate.expiration_time > (self.expiration_time or float('-inf')):
            self.expiration_time = candidate.expiration_time
            self.source_node_id = source_node_id
            if self.expiration_time >= self.sufficient_expiration_time:
                self.finish_search()

    def add_done_callback(self, callback: Callable[[_SearchState], Any]):
        self.future.add_done_callback(lambda _future: callback(self))

    def finish_search(self):
        if self.future.done():
            return
        elif not self.found_something:
            self.future.set_result(None)
        elif isinstance(self.binary_value, BinaryDHTValue):
            self.future.set_result(ValueWithExpiration(self.serializer.loads(self.binary_value), self.expiration_time))
        elif isinstance(self.binary_value, DictionaryDHTValue):
            dict_with_subkeys = {key: ValueWithExpiration(self.serializer.loads(value), item_expiration_time) for
                                 key, (value, item_expiration_time) in self.binary_value.items()}
            self.future.set_result(ValueWithExpiration(dict_with_subkeys, self.expiration_time))
        else:
            logger.error(f"Invalid value type: {type(self.binary_value)}")

    @property
    def found_something(self) -> bool:
        return self.expiration_time is not None

    @property
    def finished(self) -> bool:
        return self.future.done()

    def __lt__(self, other: _SearchState):
        return self.sufficient_expiration_time < other.sufficient_expiration_time

    def __hash__(self):
        return hash(self.key_id)


class Blacklist:
    def __init__(self, base_time: float, backoff_rate: float, **kwargs):
        self.base_time, self.backoff = base_time, backoff_rate
        self.banned_peers = TimedStorage[Endpoint, int](**kwargs)
        self.ban_counter = Counter()

    def register_failure(self, peer: Endpoint):
        if peer not in self.banned_peers and self.base_time > 0:
            ban_duration = self.base_time * self.backoff ** self.ban_counter[peer]
            self.banned_peers.store(peer, self.ban_counter[peer], expiration_time=get_dht_time() + ban_duration)
            self.ban_counter[peer] += 1

    def register_success(self, peer):
        del self.banned_peers[peer], self.ban_counter[peer]

    def __contains__(self, peer: Endpoint) -> bool:
        return peer in self.banned_peers

    def __repr__(self):
        return f"{self.__class__.__name__}(base_time={self.base_time}, backoff={self.backoff}, " f"banned_peers={len(self.banned_peers)})"

    def clear(self):
        self.banned_peers.clear()
        self.ban_counter.clear()


class CacheRefreshQueue(TimedStorage[DHTID, DHTExpiration]):
    frozen = True
