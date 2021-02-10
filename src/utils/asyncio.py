from typing import TypeVar, AsyncIterator, Union, AsyncIterable
import asyncio
import uvloop

T = TypeVar('T')


def switch_to_uvloop() -> asyncio.AbstractEventLoop:
    try:
        asyncio.get_event_loop().stop()
    except RuntimeError as error_no_event_loop:
        pass
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


async def anext(aiter: AsyncIterator[T]) -> Union[T, StopAsyncIteration]:
    return await aiter.__anext__()


async def aiter(*args: T) -> AsyncIterator[T]:
    for arg in args:
        yield arg


async def achain(*async_iters: AsyncIterable[T]) -> AsyncIterator[T]:
    for aiter in async_iters:
        async for elem in aiter:
            yield elem
