import asyncio
import sys
from typing import AsyncGenerator

import pytest

from ray.serve._private.replica_response_generator import ReplicaResponseGenerator


@pytest.mark.asyncio
class TestReplicaResponseGenerator:
    async def test_disconnect_awaits_next_result_task_before_aclose(self):
        """When disconnected_task completes, ReplicaResponseGenerator must cancel
        and await the in-flight __anext__ task before raising CancelledError so
        that an immediate `await result_gen.aclose()` in the caller's finally
        block does not fail with `RuntimeError: aclose(): asynchronous generator
        is already running`.
        """
        started = asyncio.Event()
        cleaned_up = False

        async def slow_gen() -> AsyncGenerator[str, None]:
            nonlocal cleaned_up
            started.set()
            try:
                await asyncio.sleep(10)
                yield "item"
            finally:
                cleaned_up = True

        disconnect_fut = asyncio.get_running_loop().create_future()
        result_gen = slow_gen()
        replica_response_generator = ReplicaResponseGenerator(
            result_gen,
            disconnected_task=disconnect_fut,
        )

        async def trigger_disconnect():
            await started.wait()
            disconnect_fut.set_result(None)

        asyncio.create_task(trigger_disconnect())

        with pytest.raises(asyncio.CancelledError):
            try:
                async for _ in replica_response_generator:
                    pass
            finally:
                await result_gen.aclose()

        assert cleaned_up
        assert not result_gen.ag_running

    async def test_timeout_awaits_next_result_task_before_aclose(self):
        """When timeout_s expires while disconnected_task is pending,
        ReplicaResponseGenerator must cancel and await the in-flight __anext__
        task before raising TimeoutError so that an immediate `await
        result_gen.aclose()` in the caller's finally block succeeds.
        """
        cleaned_up = False

        async def slow_gen() -> AsyncGenerator[str, None]:
            nonlocal cleaned_up
            try:
                await asyncio.sleep(10)
                yield "item"
            finally:
                cleaned_up = True

        disconnect_fut = asyncio.get_running_loop().create_future()
        result_gen = slow_gen()
        replica_response_generator = ReplicaResponseGenerator(
            result_gen,
            timeout_s=0.01,
            disconnected_task=disconnect_fut,
        )

        with pytest.raises(TimeoutError):
            try:
                async for _ in replica_response_generator:
                    pass
            finally:
                await result_gen.aclose()

        assert cleaned_up
        assert not result_gen.ag_running

    async def test_external_cancellation_cancels_next_result_task_before_aclose(self):
        """When the caller task awaiting ReplicaResponseGenerator is cancelled
        externally while disconnected_task is present, the inner __anext__ task
        must be cancelled and awaited before propagating CancelledError so that
        `await result_gen.aclose()` does not raise RuntimeError.
        """
        started = asyncio.Event()
        cleaned_up = False

        async def slow_gen() -> AsyncGenerator[str, None]:
            nonlocal cleaned_up
            started.set()
            try:
                await asyncio.sleep(10)
                yield "item"
            finally:
                cleaned_up = True

        disconnect_fut = asyncio.get_running_loop().create_future()
        result_gen = slow_gen()
        replica_response_generator = ReplicaResponseGenerator(
            result_gen,
            disconnected_task=disconnect_fut,
        )

        async def consume():
            try:
                async for _ in replica_response_generator:
                    pass
            finally:
                await result_gen.aclose()

        consumer_task = asyncio.create_task(consume())
        await started.wait()
        consumer_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await consumer_task

        assert cleaned_up
        assert not result_gen.ag_running

    async def test_generator_catching_cancelled_error_and_returning(self):
        """If the wrapped async generator catches CancelledError and returns
        (raising StopAsyncIteration from __anext__), cancellation cleanup should
        suppress StopAsyncIteration and still raise CancelledError on disconnect.
        """
        started = asyncio.Event()

        async def gen_swallowing_cancel() -> AsyncGenerator[str, None]:
            started.set()
            try:
                await asyncio.sleep(10)
                yield "item"
            except asyncio.CancelledError:
                return

        disconnect_fut = asyncio.get_running_loop().create_future()
        result_gen = gen_swallowing_cancel()
        replica_response_generator = ReplicaResponseGenerator(
            result_gen,
            disconnected_task=disconnect_fut,
        )

        async def trigger_disconnect():
            await started.wait()
            disconnect_fut.set_result(None)

        asyncio.create_task(trigger_disconnect())

        with pytest.raises(asyncio.CancelledError):
            try:
                async for _ in replica_response_generator:
                    pass
            finally:
                await result_gen.aclose()

        assert not result_gen.ag_running


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
