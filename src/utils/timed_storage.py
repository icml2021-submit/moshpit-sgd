from __future__ import annotations

import heapq
import time
from contextlib import contextmanager
from typing import TypeVar, NamedTuple, Generic, Optional, Dict, List, Iterator, Tuple

KeyType = TypeVar('KeyType')
ValueType = TypeVar('ValueType')
get_dht_time = time.time
MAX_DHT_TIME_DISCREPANCY_SECONDS = 3
DHTExpiration = float
ROOT = 0


class ValueWithExpiration(NamedTuple, Generic[ValueType]):
    value: ValueType
    expiration_time: DHTExpiration


class HeapEntry(NamedTuple, Generic[KeyType]):
    expiration_time: DHTExpiration
    key: KeyType


class TimedStorage(Generic[KeyType, ValueType]):
    frozen = False

    def __init__(self, maxsize: Optional[int] = None):
        self.maxsize = maxsize or float("inf")
        self.data: Dict[KeyType, ValueWithExpiration[ValueType]] = dict()
        self.expiration_heap: List[HeapEntry[KeyType]] = []
        self.key_to_heap: Dict[KeyType, HeapEntry[KeyType]] = dict()

    def _remove_outdated(self):
        while not self.frozen and self.expiration_heap and (
                self.expiration_heap[ROOT].expiration_time < get_dht_time() or len(self.data) > self.maxsize):
            heap_entry = heapq.heappop(self.expiration_heap)
            if self.key_to_heap.get(heap_entry.key) == heap_entry:
                del self.data[heap_entry.key], self.key_to_heap[heap_entry.key]

    def store(self, key: KeyType, value: ValueType, expiration_time: DHTExpiration) -> bool:
        if expiration_time < get_dht_time() and not self.frozen:
            return False
        self.key_to_heap[key] = HeapEntry(expiration_time, key)
        heapq.heappush(self.expiration_heap, self.key_to_heap[key])
        if key in self.data:
            if self.data[key].expiration_time < expiration_time:
                self.data[key] = ValueWithExpiration(value, expiration_time)
                return True
            return False
        self.data[key] = ValueWithExpiration(value, expiration_time)
        self._remove_outdated()
        return True

    def get(self, key: KeyType) -> Optional[ValueWithExpiration[ValueType]]:
        self._remove_outdated()
        if key in self.data:
            return self.data[key]
        return None

    def items(self) -> Iterator[Tuple[KeyType, ValueWithExpiration[ValueType]]]:
        self._remove_outdated()
        return ((key, value_and_expiration) for key, value_and_expiration in self.data.items())

    def top(self) -> Tuple[Optional[KeyType], Optional[ValueWithExpiration[ValueType]]]:
        self._remove_outdated()
        if self.data:
            while self.key_to_heap.get(self.expiration_heap[ROOT].key) != self.expiration_heap[ROOT]:
                heapq.heappop(self.expiration_heap)
            top_key = self.expiration_heap[ROOT].key
            return top_key, self.data[top_key]
        return None, None

    def clear(self):
        self.data.clear()
        self.key_to_heap.clear()
        self.expiration_heap.clear()

    def __contains__(self, key: KeyType):
        self._remove_outdated()
        return key in self.data

    def __len__(self):
        self._remove_outdated()
        return len(self.data)

    def __delitem__(self, key: KeyType):
        if key in self.key_to_heap:
            del self.data[key], self.key_to_heap[key]

    def __bool__(self):
        return bool(self.data)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data})"

    @contextmanager
    def freeze(self):
        prev_frozen, self.frozen = self.frozen, True
        try:
            yield self
        finally:
            self.frozen = prev_frozen
