from __future__ import annotations
import hashlib
import heapq
import os
import random
from collections.abc import Iterable
from itertools import chain
from typing import Tuple, Optional, List, Dict, Set, Union, Any, Sequence
from src.utils import Endpoint, PickleSerializer, get_dht_time, DHTExpiration

DHTKey, Subkey, DHTValue, BinaryDHTID, BinaryDHTValue, = Any, Any, Any, bytes, bytes


class RoutingTable:
    def __init__(self, node_id: DHTID, bucket_size: int, depth_modulo: int):
        self.node_id, self.bucket_size, self.depth_modulo = node_id, bucket_size, depth_modulo
        self.buckets = [KBucket(node_id.MIN, node_id.MAX, bucket_size)]
        self.endpoint_to_uid: Dict[Endpoint, DHTID] = dict()
        self.uid_to_endpoint: Dict[DHTID, Endpoint] = dict()

    def get_bucket_index(self, node_id: DHTID) -> int:
        lower_index, upper_index = 0, len(self.buckets)
        while upper_index - lower_index > 1:
            pivot_index = (lower_index + upper_index + 1) // 2
            if node_id >= self.buckets[pivot_index].lower:
                lower_index = pivot_index
            else:
                upper_index = pivot_index
        assert upper_index - lower_index == 1
        return lower_index

    def add_or_update_node(self, node_id: DHTID, endpoint: Endpoint) -> Optional[Tuple[DHTID, Endpoint]]:
        bucket_index = self.get_bucket_index(node_id)
        bucket = self.buckets[bucket_index]
        store_success = bucket.add_or_update_node(node_id, endpoint)
        if node_id in bucket.nodes_to_endpoint or node_id in bucket.replacement_nodes:
            self.uid_to_endpoint[node_id] = endpoint
            self.endpoint_to_uid[endpoint] = node_id
        if not store_success:
            if bucket.has_in_range(self.node_id) or bucket.depth % self.depth_modulo != 0:
                self.split_bucket(bucket_index)
                return self.add_or_update_node(node_id, endpoint)
            return bucket.request_ping_node()

    def split_bucket(self, index: int) -> None:
        first, second = self.buckets[index].split()
        self.buckets[index] = first
        self.buckets.insert(index + 1, second)

    def get(self, *, node_id: Optional[DHTID] = None, endpoint: Optional[Endpoint] = None, default=None):
        assert (node_id is None) != (endpoint is None), "Please specify either node_id or endpoint, but not both"
        if node_id is not None:
            return self.uid_to_endpoint.get(node_id, default)
        else:
            return self.endpoint_to_uid.get(endpoint, default)

    def __getitem__(self, item: Union[DHTID, Endpoint]) -> Union[Endpoint, DHTID]:
        return self.uid_to_endpoint[item] if isinstance(item, DHTID) else self.endpoint_to_uid[item]

    def __setitem__(self, node_id: DHTID, endpoint: Endpoint) -> NotImplementedError:
        raise NotImplementedError("RoutingTable doesn't support direct item assignment. Use table.try_add_node instead")

    def __contains__(self, item: Union[DHTID, Endpoint]) -> bool:
        return (item in self.uid_to_endpoint) if isinstance(item, DHTID) else (item in self.endpoint_to_uid)

    def __delitem__(self, node_id: DHTID):
        del self.buckets[self.get_bucket_index(node_id)][node_id]
        node_endpoint = self.uid_to_endpoint.pop(node_id)
        if self.endpoint_to_uid.get(node_endpoint) == node_id:
            del self.endpoint_to_uid[node_endpoint]

    def get_nearest_neighbors(self, query_id: DHTID, k: int, exclude: Optional[DHTID] = None) -> List[
        Tuple[DHTID, Endpoint]]:
        candidates: List[Tuple[int, DHTID, Endpoint]] = []
        nearest_index = self.get_bucket_index(query_id)
        nearest_bucket = self.buckets[nearest_index]
        for node_id, endpoint in nearest_bucket.nodes_to_endpoint.items():
            heapq.heappush(candidates, (query_id.xor_distance(node_id), node_id, endpoint))
        left_index, right_index = nearest_index, nearest_index + 1
        current_lower, current_upper, current_depth = nearest_bucket.lower, nearest_bucket.upper, nearest_bucket.depth
        while current_depth > 0 and len(candidates) < k + int(exclude is not None):
            split_direction = current_lower // 2 ** (DHTID.HASH_NBYTES * 8 - current_depth) % 2
            current_depth -= 1
            if split_direction == 0:
                current_upper += current_upper - current_lower
                while right_index < len(self.buckets) and self.buckets[right_index].upper <= current_upper:
                    for node_id, endpoint in self.buckets[right_index].nodes_to_endpoint.items():
                        heapq.heappush(candidates, (query_id.xor_distance(node_id), node_id, endpoint))
                    right_index += 1
                assert self.buckets[right_index - 1].upper == current_upper
            else:
                current_lower -= current_upper - current_lower
                while left_index > 0 and self.buckets[left_index - 1].lower >= current_lower:
                    left_index -= 1
                    for node_id, endpoint in self.buckets[left_index].nodes_to_endpoint.items():
                        heapq.heappush(candidates, (query_id.xor_distance(node_id), node_id, endpoint))
                assert self.buckets[left_index].lower == current_lower
        heap_top: List[Tuple[int, DHTID, Endpoint]] = heapq.nsmallest(k + int(exclude is not None), candidates)
        return [(node, endpoint) for _, node, endpoint in heap_top if node != exclude][:k]

    def __repr__(self):
        bucket_info = "\n".join(repr(bucket) for bucket in self.buckets)
        return f"{self.__class__.__name__}(node_id={self.node_id}, bucket_size={self.bucket_size}," f" modulo={self.depth_modulo},\nbuckets=[\n{bucket_info})"


class KBucket:
    def __init__(self, lower: int, upper: int, size: int, depth: int = 0):
        assert upper - lower == 2 ** (DHTID.HASH_NBYTES * 8 - depth)
        self.lower, self.upper, self.size, self.depth = lower, upper, size, depth
        self.nodes_to_endpoint: Dict[DHTID, Endpoint] = {}
        self.replacement_nodes: Dict[DHTID, Endpoint] = {}
        self.nodes_requested_for_ping: Set[DHTID] = set()
        self.last_updated = get_dht_time()

    def has_in_range(self, node_id: DHTID):
        return self.lower <= node_id < self.upper

    def add_or_update_node(self, node_id: DHTID, endpoint: Endpoint) -> bool:
        if node_id in self.nodes_requested_for_ping:
            self.nodes_requested_for_ping.remove(node_id)
        self.last_updated = get_dht_time()
        if node_id in self.nodes_to_endpoint:
            del self.nodes_to_endpoint[node_id]
            self.nodes_to_endpoint[node_id] = endpoint
        elif len(self.nodes_to_endpoint) < self.size:
            self.nodes_to_endpoint[node_id] = endpoint
        else:
            if node_id in self.replacement_nodes:
                del self.replacement_nodes[node_id]
            self.replacement_nodes[node_id] = endpoint
            return False
        return True

    def request_ping_node(self) -> Optional[Tuple[DHTID, Endpoint]]:
        for uid, endpoint in self.nodes_to_endpoint.items():
            if uid not in self.nodes_requested_for_ping:
                self.nodes_requested_for_ping.add(uid)
                return uid, endpoint

    def __getitem__(self, node_id: DHTID) -> Endpoint:
        return self.nodes_to_endpoint[node_id] if node_id in self.nodes_to_endpoint else self.replacement_nodes[node_id]

    def __delitem__(self, node_id: DHTID):
        if not (node_id in self.nodes_to_endpoint or node_id in self.replacement_nodes):
            raise KeyError(f"KBucket does not contain node id={node_id}.")
        if node_id in self.replacement_nodes:
            del self.replacement_nodes[node_id]
        if node_id in self.nodes_to_endpoint:
            del self.nodes_to_endpoint[node_id]
            if self.replacement_nodes:
                newnode_id, newnode = self.replacement_nodes.popitem()
                self.nodes_to_endpoint[newnode_id] = newnode

    def split(self) -> Tuple[KBucket, KBucket]:
        midpoint = (self.lower + self.upper) // 2
        assert self.lower < midpoint < self.upper, f"Bucket to small to be split: [{self.lower}: {self.upper})"
        left = KBucket(self.lower, midpoint, self.size, depth=self.depth + 1)
        right = KBucket(midpoint, self.upper, self.size, depth=self.depth + 1)
        for node_id, endpoint in chain(self.nodes_to_endpoint.items(), self.replacement_nodes.items()):
            bucket = left if int(node_id) <= midpoint else right
            bucket.add_or_update_node(node_id, endpoint)
        return left, right

    def __repr__(self):
        return f"{self.__class__.__name__}({len(self.nodes_to_endpoint)} nodes" f" with {len(self.replacement_nodes)} replacements, depth={self.depth}, max size={self.size}" f" lower={hex(self.lower)}, upper={hex(self.upper)})"


class DHTID(int):
    HASH_FUNC = hashlib.sha1
    HASH_NBYTES = 20
    RANGE = MIN, MAX = 0, 2 ** (HASH_NBYTES * 8)

    def __new__(cls, value: int):
        assert cls.MIN <= value < cls.MAX, f"DHTID must be in [{cls.MIN}, {cls.MAX}) but got {value}"
        return super().__new__(cls, value)

    @classmethod
    def generate(cls, source: Optional[Any] = None, nbits: int = 255):
        source = random.getrandbits(nbits).to_bytes(nbits, byteorder='big') if source is None else source
        source = PickleSerializer.dumps(source) if not isinstance(source, bytes) else source
        raw_uid = cls.HASH_FUNC(source).digest()
        return cls(int(raw_uid.hex(), 16))

    def xor_distance(self, other: Union[DHTID, Sequence[DHTID]]) -> Union[int, List[int]]:
        if isinstance(other, Iterable):
            return list(map(self.xor_distance, other))
        return int(self) ^ int(other)

    @classmethod
    def longest_common_prefix_length(cls, *ids: DHTID) -> int:
        ids_bits = [bin(uid)[2:].rjust(8 * cls.HASH_NBYTES, '0') for uid in ids]
        return len(os.path.commonprefix(ids_bits))

    def to_bytes(self, length=HASH_NBYTES, byteorder='big', *, signed=False) -> bytes:
        return super().to_bytes(length, byteorder, signed=signed)

    @classmethod
    def from_bytes(cls, raw: bytes, byteorder='big', *, signed=False) -> DHTID:
        return DHTID(super().from_bytes(raw, byteorder=byteorder, signed=signed))

    def __repr__(self):
        return f"{self.__class__.__name__}({hex(self)})"

    def __bytes__(self):
        return self.to_bytes()
