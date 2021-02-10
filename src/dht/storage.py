from __future__ import annotations
from typing import Optional, Union
from src.dht.routing import DHTID, DHTExpiration, BinaryDHTValue, Subkey
from src.utils.serializer import MSGPackSerializer
from src.utils.timed_storage import KeyType, ValueType, TimedStorage


@MSGPackSerializer.ext_serializable(0x50)
class DictionaryDHTValue(TimedStorage[Subkey, BinaryDHTValue]):
    latest_expiration_time = float('-inf')

    def store(self, key: KeyType, value: ValueType, expiration_time: DHTExpiration) -> bool:
        self.latest_expiration_time = max(self.latest_expiration_time, expiration_time)
        return super().store(key, value, expiration_time)

    def packb(self) -> bytes:
        packed_items = [[key, value, expiration_time] for key, (value, expiration_time) in self.items()]
        return MSGPackSerializer.dumps([self.maxsize, self.latest_expiration_time, packed_items])

    @classmethod
    def unpackb(cls, raw: bytes) -> DictionaryDHTValue:
        maxsize, latest_expiration_time, items = MSGPackSerializer.loads(raw)
        with DictionaryDHTValue(maxsize).freeze()as new_dict:
            for key, value, expiration_time in items:
                new_dict.store(key, value, expiration_time)
            new_dict.latest_expiration_time = latest_expiration_time
            return new_dict


class DHTLocalStorage(TimedStorage[DHTID, Union[BinaryDHTValue, DictionaryDHTValue]]):
    def store(self, key: DHTID, value: BinaryDHTValue, expiration_time: DHTExpiration,
              subkey: Optional[Subkey] = None) -> bool:
        if subkey is not None:
            return self.store_subkey(key, subkey, value, expiration_time)
        else:
            return super().store(key, value, expiration_time)

    def store_subkey(self, key: DHTID, subkey: Subkey, value: BinaryDHTValue, expiration_time: DHTExpiration) -> bool:
        previous_value, previous_expiration_time = self.get(key) or (b'', -float('inf'))
        if isinstance(previous_value, BinaryDHTValue) and expiration_time > previous_expiration_time:
            new_storage = DictionaryDHTValue()
            new_storage.store(subkey, value, expiration_time)
            return super().store(key, new_storage, new_storage.latest_expiration_time)
        elif isinstance(previous_value, DictionaryDHTValue):
            if expiration_time > previous_value.latest_expiration_time:
                super().store(key, previous_value, expiration_time)
            return previous_value.store(subkey, value, expiration_time)
        else:
            return False
