from __future__ import annotations
import asyncio
import contextlib
import ctypes
import multiprocessing as mp
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Sequence, Optional, Tuple, Any, Union, Dict, AsyncIterator
import grpc
import torch
import numpy as np
import pickle
import src
from src.client.averaging.allreduce import AllReduceRunner, AllreduceException, GroupID, split_into_parts
from src.client.averaging.matchmaking import Matchmaking, MatchmakingException
from src.proto import averaging_pb2, averaging_pb2_grpc, runtime_pb2
from src.utils import get_logger, Endpoint, Port, MPFuture, GRPC_KEEPALIVE_OPTIONS, get_dht_time, \
    MSGPackSerializer, DHTExpiration, ValueWithExpiration, ChannelCache, combine_from_streaming, \
    deserialize_torch_tensor
from src.utils import serialize_torch_tensor, split_for_streaming
from src.utils.asyncio import anext, achain, aiter, switch_to_uvloop

StreamCallToLeader = grpc.aio.UnaryStreamCall[averaging_pb2.JoinRequest, averaging_pb2.MessageFromLeader]
DataForGather = Any
logger = get_logger(__name__)


class DecentralizedAverager(mp.Process, averaging_pb2_grpc.DecentralizedAveragingServicer):
    _matchmaking: Matchmaking
    _pending_group_assembled: asyncio.Event
    serializer = MSGPackSerializer

    def __init__(self, averaged_tensors: Sequence[torch.Tensor], dht: src.dht.DHT, *, start: bool, prefix: str,
                 target_group_size: int, min_group_size: int = 2, initial_group_bits: Optional[str] = None,
                 averaging_expiration: float = 15, request_timeout: float = 3, chunk_size_bytes: int = 2 ** 16,
                 allreduce_timeout: Optional[float] = None, averaging_alpha: float = 1.0,
                 compression_type: runtime_pb2.CompressionType = runtime_pb2.CompressionType.NONE,
                 throughput: Optional[float] = None, min_vector_size: int = 0, listen: bool = True,
                 listen_on: Endpoint = '0.0.0.0:*', receiver_threads: int = 1, daemon: bool = True,
                 channel_options: Optional[Sequence[Tuple[str, Any]]] = None, **kwargs):
        assert '.' not in prefix, "group prefix must be a string without trailing '.'"
        assert throughput is None or (throughput >= 0 and np.isfinite(
            np.float32(throughput))), "throughput must be a non-negative float32"
        if not listen:
            raise NotImplementedError("Client-only averaging is not implemented yet.")
        if not is_power_of_two(target_group_size):
            logger.warning("It is recommended to set target_group_size to a power of 2.")
        assert initial_group_bits is None or all(bit in '01' for bit in initial_group_bits)
        super().__init__()
        self.dht = dht
        self.listen_on, self.receiver_threads, self.kwargs = listen_on, receiver_threads, kwargs
        self.channel_options = channel_options
        self.daemon = daemon
        self._averaged_tensors = tuple(averaged_tensors)
        self.lock_averaged_tensors = mp.Lock()
        self.last_updated: DHTExpiration = -float('inf')
        for tensor in self._averaged_tensors:
            assert tensor.grad_fn is None, "averaged_tensors must be either parameters or leaf tensors"
            tensor.share_memory_()
        self.matchmaking_kwargs = dict(prefix=prefix, initial_group_bits=initial_group_bits,
                                       target_group_size=target_group_size, min_group_size=min_group_size,
                                       averaging_expiration=averaging_expiration, request_timeout=request_timeout,
                                       chunk_size_bytes=chunk_size_bytes, compression_type=compression_type,
                                       throughput=throughput, min_vector_size=min_vector_size)
        self._averaging_alpha, self._allreduce_timeout = averaging_alpha, allreduce_timeout
        self._running_groups: Dict[GroupID, AllReduceRunner] = {}
        self._pipe, self.pipe = mp.Pipe(duplex=True)
        self._port = mp.Value(ctypes.c_uint32, 0)
        self._averager_endpoint: Optional[Endpoint] = None
        self.ready = mp.Event()
        if start:
            self.run_in_background(await_ready=True)
            src.run_in_background(self._background_thread_fetch_current_state_if_asked)

    @property
    def port(self) -> Optional[Port]:
        return self._port.value if self._port.value != 0 else None

    @property
    def endpoint(self) -> Endpoint:
        assert self.port is not None, "Averager is not running yet"
        if self._averager_endpoint is None:
            self._averager_endpoint = f"{self.dht.get_visible_address()}:{self.port}"
            logger.debug(f"Assuming averager endpoint to be {self._averager_endpoint}")
        return self._averager_endpoint

    def __repr__(self):
        return f"{self.__class__.__name__}({self.endpoint})"

    def run(self):
        loop = switch_to_uvloop()
        pipe_awaiter = ThreadPoolExecutor(self.receiver_threads)

        async def _run():
            grpc.aio.init_grpc_aio()
            server = grpc.aio.server(**self.kwargs, options=GRPC_KEEPALIVE_OPTIONS)
            averaging_pb2_grpc.add_DecentralizedAveragingServicer_to_server(self, server)
            found_port = server.add_insecure_port(self.listen_on)
            assert found_port != 0, f"Failed to listen to {self.listen_on}"
            self._port.value = found_port
            self._matchmaking = Matchmaking(self.endpoint, self._averaged_tensors, self.dht, **self.matchmaking_kwargs,
                                            return_deltas=True)
            self._pending_group_assembled = asyncio.Event()
            self._pending_group_assembled.set()
            await server.start()
            self.ready.set()
            asyncio.create_task(self._declare_for_download_periodically())
            while True:
                method, args, kwargs = await loop.run_in_executor(pipe_awaiter, self._pipe.recv)
                asyncio.create_task(getattr(self, method)(*args, **kwargs))

        loop.run_until_complete(_run())

    def run_in_background(self, await_ready=True, timeout=None):
        self.start()
        if await_ready and not self.ready.wait(timeout=timeout):
            raise TimeoutError(f"Server didn't notify .ready in {timeout} seconds")

    def shutdown(self) -> None:
        if self.is_alive():
            self.terminate()
        else:
            logger.warning("DHT shutdown has no effect: the process is not alive")

    def step(self, gather: Optional[DataForGather] = None, allow_retries: bool = True, timeout: Optional[float] = None,
             group_bits: Optional[str] = None, wait=True) -> Union[Optional[Dict[Endpoint, DataForGather]], MPFuture]:
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_step', [], dict(future=_future, gather=gather, allow_retries=allow_retries, timeout=timeout,
                                          group_bits=group_bits)))
        return future.result() if wait else future

    async def _step(self, *, future: MPFuture, gather: DataForGather, allow_retries: bool, timeout: Optional[float],
                    group_bits: Optional[str]):
        if group_bits is not None:
            assert all(bit in '01' for bit in group_bits)
            self._matchmaking.group_key_manager.group_bits = group_bits
        loop = asyncio.get_event_loop()
        start_time = get_dht_time()
        group_id = None
        while not future.done():
            try:
                self._pending_group_assembled.clear()
                gather_binary = self.serializer.dumps(gather)
                allreduce_group = await self._matchmaking.look_for_group(timeout=timeout, data_for_gather=gather_binary)
                if allreduce_group is None:
                    raise AllreduceException("Averaging step failed: could not find a group.")
                group_id = allreduce_group.group_id
                self._running_groups[group_id] = allreduce_group
                self._pending_group_assembled.set()
                await asyncio.wait_for(allreduce_group.run(), self._allreduce_timeout)
                await loop.run_in_executor(None, self.update_tensors, allreduce_group)
                gathered_items = map(self.serializer.loads, allreduce_group.gathered)
                gathered_data_by_peer = dict(zip(allreduce_group.ordered_group_endpoints, gathered_items))
                future.set_result(gathered_data_by_peer)
            except(AllreduceException, MatchmakingException):
                time_elapsed = get_dht_time() - start_time
                if not allow_retries or (timeout is not None and timeout < time_elapsed):
                    future.set_result(None)
            except Exception as e:
                future.set_exception(e)
                raise
            finally:
                _ = self._running_groups.pop(group_id, None)
                self._pending_group_assembled.set()

    def update_tensors(self, allreduce_group: AllReduceRunner):
        assert allreduce_group.return_deltas and allreduce_group.future.done()
        averaging_deltas = allreduce_group.future.result()
        with torch.no_grad(), self.get_tensors()as local_tensors:
            assert len(local_tensors) == len(self._averaged_tensors)
            for tensor, update in zip(local_tensors, averaging_deltas):
                tensor.add_(update, alpha=self._averaging_alpha)
        self.last_updated = get_dht_time()

    @contextlib.contextmanager
    def get_tensors(self) -> Sequence[torch.Tensor]:
        with self.lock_averaged_tensors:
            yield self._averaged_tensors
        self.last_updated = get_dht_time()

    async def rpc_join_group(self, request: averaging_pb2.JoinRequest, context: grpc.ServicerContext) -> AsyncIterator[
        averaging_pb2.MessageFromLeader]:
        async for response in self._matchmaking.rpc_join_group(request, context):
            yield response

    async def rpc_aggregate_part(self, stream: AsyncIterator[averaging_pb2.AveragingData],
                                 context: grpc.ServicerContext) -> AsyncIterator[averaging_pb2.AveragingData]:
        request = await anext(stream)
        if request.group_id not in self._running_groups:
            await self._pending_group_assembled.wait()
        group = self._running_groups.get(request.group_id)
        if group is None:
            yield averaging_pb2.AveragingData(code=averaging_pb2.BAD_GROUP_ID)
            return
        async for message in group.rpc_aggregate_part(achain(aiter(request), stream), context):
            yield message

    async def _declare_for_download_periodically(self):
        download_key = f'{self._matchmaking.group_key_manager.prefix}.all_averagers'
        while True:
            asyncio.create_task(asyncio.wait_for(
                self.dht.store(download_key, subkey=self.endpoint, value=self.last_updated,
                               expiration_time=get_dht_time() + self._matchmaking.averaging_expiration,
                               return_future=True), timeout=self._matchmaking.averaging_expiration))
            await asyncio.sleep(self._matchmaking.averaging_expiration)

    async def rpc_download_state(self, request: averaging_pb2.DownloadRequest, context: grpc.ServicerContext) -> \
            AsyncIterator[averaging_pb2.DownloadData]:
        chunk_size_bytes = self.matchmaking_kwargs.get('chunk_size_bytes', 2 ** 16)
        metadata, tensors = await self._get_current_state_from_host_process()
        metadata_to_send = pickle.dumps(metadata)
        for tensor in tensors:
            for part in split_for_streaming(serialize_torch_tensor(tensor), chunk_size_bytes):
                if metadata_to_send is not None:
                    yield averaging_pb2.DownloadData(tensor_part=part, metadata=metadata_to_send)
                    metadata_to_send = None
                else:
                    yield averaging_pb2.DownloadData(tensor_part=part)

    def get_current_state(self) -> Tuple[Any, Sequence[torch.Tensor]]:
        with self.get_tensors()as tensors:
            return dict(group_key=self.get_current_group_key()), tensors

    async def _get_current_state_from_host_process(self):
        future, _future = MPFuture.make_pair()
        self._pipe.send(('_TRIGGER_GET_CURRENT_STATE', _future))
        return await future

    def _background_thread_fetch_current_state_if_asked(self):
        while True:
            trigger, future = self.pipe.recv()
            assert trigger == '_TRIGGER_GET_CURRENT_STATE'
            try:
                future.set_result(self.get_current_state())
            except BaseException as e:
                future.set_exception(e)
                raise

    def load_state_from_peers(self, wait=True) -> Optional[Any]:
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_load_state_from_peers', [], dict(future=_future)))
        return future.result() if wait else future

    async def _load_state_from_peers(self, future: MPFuture):
        key_manager = self._matchmaking.group_key_manager
        peers_info, _ = self.dht.get(f"{key_manager.prefix}.all_averagers", latest=True) or ({}, None)
        metadata = None
        if not isinstance(peers_info, dict):
            logger.info("Averager could not load state from peers: peer dict is corrupted.")
            future.set_result(None)
            return

        def get_priority(peer):
            info = peers_info[peer]
            if not isinstance(info, ValueWithExpiration) or not isinstance(info.value, (float, int)):
                logger.warning(f"Skipping averager {peer} - bad peer info {info.value} (expected a number).")
                return -float('inf')
            return float(info.value)

        for peer in sorted(peers_info.keys(), key=get_priority, reverse=True):
            if peer != self.endpoint:
                logger.info(f"Downloading parameters from peer {peer}")
                stream = None
                try:
                    leader_stub = ChannelCache.get_stub(peer, averaging_pb2_grpc.DecentralizedAveragingStub, aio=True)
                    stream = leader_stub.rpc_download_state(averaging_pb2.DownloadRequest())
                    current_tensor_parts, tensors = [], []
                    async for message in stream:
                        if message.metadata:
                            metadata = pickle.loads(message.metadata)
                        if message.tensor_part.dtype and current_tensor_parts:
                            tensors.append(deserialize_torch_tensor(combine_from_streaming(current_tensor_parts)))
                            current_tensor_parts = []
                        current_tensor_parts.append(message.tensor_part)
                    if current_tensor_parts:
                        tensors.append(deserialize_torch_tensor(combine_from_streaming(current_tensor_parts)))
                    future.set_result((metadata, tensors))
                    self.last_updated = get_dht_time()
                    break
                except grpc.aio.AioRpcError as e:
                    logger.info(f"Failed to download state from {peer} - {e}")
                finally:
                    if stream is not None:
                        await stream.code()
        else:
            logger.warning("Averager could not load state from peers: found no active peers.")
            future.set_result(None)

    def get_current_group_key(self, wait: bool = True):
        future, _future = MPFuture.make_pair()
        self.pipe.send(('_get_current_group_key', [], dict(future=_future)))
        return future.result() if wait else future

    async def _get_current_group_key(self, future: MPFuture):
        future.set_result(self._matchmaking.group_key_manager.group_bits)


def is_power_of_two(n):
    return (n != 0) and (n & (n - 1) == 0)
