import asyncio
from typing import AsyncGenerator, Generic, Iterable, List, Optional, TypeVar

from ray.llm._internal.serve.configs.constants import (
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class Batcher(Generic[T]):
    """This class batches multiple responses from a generator into a list of
    single responses, at some time interval.

    Args:
        generator: the async generator that this class pulls responses
            from.
        interval_ms: the interval at which this class yields the current batch.
            If None, this class will batch all responses from the generator
            together and yield the entire batch once.
    """

    def __init__(
        self,
        generator: AsyncGenerator[T, None],
        interval_ms: Optional[float] = MODEL_RESPONSE_BATCH_TIMEOUT_MS,
    ):
        self.generator = generator
        self.queue: asyncio.Queue = asyncio.Queue()

        if interval_ms is None:
            self.interval_s = None
        else:
            self.interval_s = interval_ms / 1000

        if interval_ms == 0:
            return

        self.done_event: asyncio.Event = asyncio.Event()

        # We are okay with this task getting cancelled (to propagate cancellations)
        self.read_task = asyncio.create_task(self.read())

    def _merge_results(self, results: List[T]) -> Iterable[T]:
        return results

    async def stream(self) -> AsyncGenerator[Iterable[T], None]:
        """Drain from the queue every interval_ms and yield the merged results"""

        if self.interval_s == 0:
            async for item in self.generator:
                yield [item]

            return

        try:
            while True:
                # Wait for the interval or until we finish, whichever is faster.
                # We use an event to avoid asyncio.wait_for cancelling the real task on timeout.
                try:
                    if self.interval_s is None:
                        await self.done_event.wait()
                    else:
                        await asyncio.wait_for(
                            self.done_event.wait(), timeout=self.interval_s
                        )
                except asyncio.TimeoutError:
                    pass

                # Get all elements from the queue
                results, is_done = self.check_done_and_drain()

                # If there are results, merge and yield them
                if results:
                    output = self._merge_results(results)
                    yield output

                # If the read task is done, exit the stream task
                if is_done:
                    # Raise exception, if any
                    self.read_task.result()
                    break
        finally:
            # If the stream task is done, make sure to exit the read task
            if not self.read_task.done():
                self.read_task.cancel()

    def check_done_and_drain(self):
        results = self.drain_queue()
        return results, self.read_task.done()

    async def read(self):
        """Read from the generator and put into the queue in a tight loop"""
        try:
            async for x in self.generator:
                self.queue.put_nowait(x)
        finally:
            self.done_event.set()

    def drain_queue(self):
        """Drain all results currently in the queue"""
        results = []
        try:
            while True:
                results.append(self.queue.get_nowait())
        except asyncio.QueueEmpty:
            pass
        return results
