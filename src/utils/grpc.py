from __future__ import annotations
import os
import threading
from typing import NamedTuple, Tuple, Optional, Union, Any, Dict, TypeVar, Type, Iterator, Iterable
import grpc
import numpy as np
import torch
from src.proto.runtime_pb2 import CompressionType
from src.proto import runtime_pb2
from src.utils.logging import get_logger
from src.utils.networking import Endpoint
from src.utils.timed_storage import TimedStorage, get_dht_time, ValueWithExpiration

logger = get_logger(__name__)
Stub = TypeVar("Stub")
GRPC_KEEPALIVE_OPTIONS = (('grpc.keepalive_time_ms', 60 * 1000), ('grpc.keepalive_timeout_ms', 60 * 1000),
                          ('grpc.keepalive_permit_without_calls', True), ('grpc.http2.max_pings_without_data', 0),
                          ('grpc.http2.min_time_between_pings_ms', 30 * 1000),
                          ('grpc.http2.min_ping_interval_without_data_ms', 10 * 1000),)


class ChannelInfo(NamedTuple):
    target: Endpoint
    aio: bool
    options: Tuple[Tuple[str, str], ...]
    credentials: Optional[grpc.ChannelCredentials]
    compression: Optional[grpc.Compression]


class ChannelCache(TimedStorage[ChannelInfo, Tuple[Union[grpc.Channel, grpc.aio.Channel], Dict]]):
    MAXIMUM_CHANNELS = os.environ.get("GRPC_PYTHON_MANAGED_CHANNEL_MAXIMUM", 4096)
    EVICTION_PERIOD_SECONDS = os.environ.get("GRPC_PYTHON_MANAGED_CHANNEL_EVICTION_SECONDS", 10 * 60)
    logger.debug(f"Eviction period = {EVICTION_PERIOD_SECONDS}s, max channels = {MAXIMUM_CHANNELS}")
    _singleton: Optional[ChannelCache] = None
    _singleton_pid: int = os.getpid()
    _lock: threading.RLock = threading.RLock()
    _update_eviction_evt: threading.Event = threading.Event()

    def __init__(self, _created_as_singleton=False):
        assert _created_as_singleton, f"Please use {self.__class__.__name__}.get_singleton()"
        super().__init__(maxsize=self.MAXIMUM_CHANNELS)
        self._is_active = True
        self._nearest_expiration_time = float('inf')
        self._eviction_thread = threading.Thread(target=self._evict_stale_channels_in_background, daemon=True)
        self._eviction_thread.start()

    @classmethod
    def get_singleton(cls):
        with cls._lock:
            if cls._singleton is None or cls._singleton_pid != os.getpid():
                if cls._singleton is not None:
                    cls._singleton._stop_background_thread()
                cls._singleton, cls._singleton_pid = cls(_created_as_singleton=True), os.getpid()
            return cls._singleton

    @classmethod
    def get_stub(cls, target: Endpoint, stub_type: Type[Stub], *, aio: bool, options: Tuple[Tuple[str, Any]] = (),
                 channel_credentials: Optional[grpc.ChannelCredentials] = None,
                 compression: Optional[grpc.Compression] = None) -> Stub:
        cache = cls.get_singleton()
        with cls._lock:
            key = ChannelInfo(target, aio, tuple(options), channel_credentials, compression)
            entry: ValueWithExpiration = super(cls, cache).get(key)
            if entry is not None:
                channel, stubs = entry.value
            else:
                channel = cls._create_channel(*key)
                stubs = {}
            channel._channel.check_connectivity_state(True)
            if stub_type not in stubs:
                stubs[stub_type] = stub_type(channel)
            expiration_time = get_dht_time() + cls.EVICTION_PERIOD_SECONDS
            super(cls, cache).store(key, (channel, stubs), expiration_time)
            if expiration_time < cache._nearest_expiration_time:
                cache._nearest_expiration_time = expiration_time
                cls._update_eviction_evt.set()
            return stubs[stub_type]

    @classmethod
    def _create_channel(cls, target: Endpoint, aio: bool, extra_options: Tuple[Tuple[str, Any], ...],
                        channel_credentials: Optional[grpc.ChannelCredentials],
                        compression: Optional[grpc.Compression]) -> Union[grpc.Channel, grpc.aio.Channel]:
        namespace = grpc.aio if aio else grpc
        options = extra_options + GRPC_KEEPALIVE_OPTIONS
        if channel_credentials is None:
            logger.debug(
                f"Creating insecure {namespace} channel with options '{options}' " f"and compression '{compression}'")
            return namespace.insecure_channel(target, options=options, compression=compression)
        else:
            logger.debug(
                f"Creating secure {namespace} channel with credentials '{channel_credentials}', " f"options '{options}' and compression '{compression}'")
            return namespace.secure_channel(target, credentials=channel_credentials, options=options,
                                            compression=compression)

    def _evict_stale_channels_in_background(self):
        while self._is_active:
            now = get_dht_time()
            time_to_wait = max(0.0, self._nearest_expiration_time - now)
            interrupted_early = self._update_eviction_evt.wait(time_to_wait if time_to_wait != float('inf') else None)
            if interrupted_early:
                self._update_eviction_evt.clear()
                continue
            with self._lock:
                self._remove_outdated()
                _, entry = super().top()
                self._nearest_expiration_time = entry.expiration_time if entry is not None else float('inf')

    def _stop_background_thread(self):
        with self._lock:
            self._is_active = False
            self._update_eviction_evt.set()

    def store(self, *args, **kwargs) -> ValueError:
        raise ValueError(f"Please use {self.__class__.__name__}.get_stub to get or create stubs")

    def get(self, *args, **kwargs) -> ValueError:
        raise ValueError(f"Please use {self.__class__.__name__}.get_stub to get or create stubs")

    def top(self) -> ValueError:
        raise ValueError(f"Please use {self.__class__.__name__}.get_stub to get or create stubs")


FP16_MAX = 65_504


def serialize_torch_tensor(tensor: torch.Tensor, compression_type=CompressionType.NONE,
                           allow_inplace=False) -> runtime_pb2.Tensor:
    assert tensor.device == torch.device('cpu')
    if compression_type == CompressionType.MEANSTD_LAST_AXIS_FLOAT16:
        assert tensor.dtype == torch.float32
        tensor = tensor if allow_inplace else tensor.clone()
        means = torch.mean(tensor, dim=-1, keepdim=True)
        tensor.sub_(means)
        stds = torch.square(tensor).sum(dim=-1, keepdim=True).div_(tensor.shape[-1]).sqrt_()
        tensor.div_(stds)
        tensor = tensor.clamp_(-FP16_MAX, FP16_MAX).to(torch.float16)
        data = b''.join((tensor.numpy().tobytes(), means.numpy().tobytes(), stds.numpy().tobytes()))
        proto = runtime_pb2.Tensor(compression=compression_type, buffer=data, size=tensor.shape,
                                   dtype='compressed_float32', requires_grad=tensor.requires_grad)
    elif compression_type == CompressionType.FLOAT16:
        assert tensor.dtype == torch.float32
        tensor = tensor if allow_inplace else tensor.clone()
        tensor = tensor.clamp_(-FP16_MAX, FP16_MAX).to(torch.float16)
        data = tensor.numpy().tobytes()
        proto = runtime_pb2.Tensor(compression=compression_type, buffer=data, size=tensor.shape,
                                   dtype='clamped_float32', requires_grad=tensor.requires_grad)
    elif compression_type == CompressionType.NONE:
        array = tensor.numpy()
        proto = runtime_pb2.Tensor(compression=compression_type, buffer=array.tobytes(), size=array.shape,
                                   dtype=array.dtype.name, requires_grad=tensor.requires_grad)
    else:
        raise ValueError(f"Unknown compression type: {compression_type}")
    return proto


def deserialize_torch_tensor(serialized_tensor: runtime_pb2.Tensor) -> torch.Tensor:
    if serialized_tensor.compression == CompressionType.NONE:
        array = np.frombuffer(serialized_tensor.buffer, dtype=np.dtype(serialized_tensor.dtype)).copy()
        tensor = torch.as_tensor(array).view(*serialized_tensor.size)
    elif serialized_tensor.compression == CompressionType.MEANSTD_LAST_AXIS_FLOAT16:
        stats_size = list(serialized_tensor.size)
        stats_size[-1] = 1
        stats_count = np.prod(stats_size)
        means = serialized_tensor.buffer[-8 * stats_count:-4 * stats_count]
        stds = serialized_tensor.buffer[-4 * stats_count:]
        means = torch.as_tensor(np.frombuffer(means, dtype=np.float32).copy()).view(*stats_size)
        stds = torch.as_tensor(np.frombuffer(stds, dtype=np.float32).copy()).view(*stats_size)
        array = np.frombuffer(serialized_tensor.buffer[:-8 * stats_count], dtype=np.float16).copy()
        tensor = torch.as_tensor(array, dtype=torch.float32).view(*serialized_tensor.size).mul_(stds).add_(means)
    elif serialized_tensor.compression == CompressionType.FLOAT16:
        array = np.frombuffer(serialized_tensor.buffer, dtype=np.float16).copy()
        tensor = torch.as_tensor(array, dtype=torch.float32).view(*serialized_tensor.size)
    else:
        raise ValueError(f"Unknown compression type: {serialized_tensor.compression}")
    tensor.requires_grad_(serialized_tensor.requires_grad)
    return tensor


def split_for_streaming(serialized_tensor: runtime_pb2.Tensor, chunk_size_bytes: int) -> Iterator[runtime_pb2.Tensor]:
    buffer = memoryview(serialized_tensor.buffer)
    num_chunks = len(range(0, len(buffer), chunk_size_bytes))
    yield runtime_pb2.Tensor(compression=serialized_tensor.compression, buffer=buffer[:chunk_size_bytes].tobytes(),
                             chunks=num_chunks, size=serialized_tensor.size, dtype=serialized_tensor.dtype,
                             requires_grad=serialized_tensor.requires_grad)
    for chunk_start in range(chunk_size_bytes, len(buffer), chunk_size_bytes):
        yield runtime_pb2.Tensor(buffer=buffer[chunk_start:chunk_start + chunk_size_bytes].tobytes())


def combine_from_streaming(stream: Iterable[runtime_pb2.Tensor]) -> runtime_pb2.Tensor:
    stream = iter(stream)
    first_chunk = next(stream)
    serialized_tensor = runtime_pb2.Tensor()
    serialized_tensor.CopyFrom(first_chunk)
    buffer_chunks = [first_chunk.buffer]
    for tensor_part in stream:
        buffer_chunks.append(tensor_part.buffer)
    serialized_tensor.buffer = b''.join(buffer_chunks)
    return serialized_tensor
