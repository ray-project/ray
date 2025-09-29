import asyncio
import sys
import time
from typing import List, Optional

import numpy as np
import pytest

from ray.llm._internal.serve.configs.constants import MODEL_RESPONSE_BATCH_TIMEOUT_MS
from ray.llm._internal.serve.deployments.utils.batcher import Batcher

TEXT_VALUE = "foo"
FINAL_TEXT_VALUE = "bar"


async def fake_generator():
    """Returns 100 responses with no delay"""
    for _i in range(100):
        yield dict(num_generated_tokens=1, generated_text=TEXT_VALUE)


async def fake_generator_slow(num_batches: int):
    """Returns 100 responses with small delay.

    Delay is set such that the responses are batched into roughly num_batches
    batches.
    """

    for _i in range(100):
        await asyncio.sleep(MODEL_RESPONSE_BATCH_TIMEOUT_MS / 1000 / num_batches)
        yield dict(num_generated_tokens=1, generated_text=TEXT_VALUE)


async def fake_generator_slow_last_return_immediate():
    """Returns 11 responses with small delay, aside from the last one which is immediate"""
    for _i in range(10):
        await asyncio.sleep(MODEL_RESPONSE_BATCH_TIMEOUT_MS / 1000)
        yield dict(num_generated_tokens=1, generated_text=TEXT_VALUE)
    yield dict(num_generated_tokens=1, generated_text=FINAL_TEXT_VALUE)


async def count_interval_ms_from_stream(stream) -> list[float]:
    output_intervals: list[float] = []
    start = None
    async for _ in stream:
        if start is None:
            start = time.perf_counter()
        else:
            end = time.perf_counter()
            output_intervals.append((end - start) * 1e3)
            start = end
    return output_intervals


class TestBatcher(Batcher):
    def _merge_results(self, results: List[dict]) -> dict:
        merged_result = {"num_generated_tokens": 0, "generated_text": ""}
        for result in results:
            for key, value in result.items():
                merged_result[key] += value
        return merged_result


class TestBatching:
    @pytest.mark.asyncio
    async def test_batch(self):
        count = 0
        batcher = TestBatcher(fake_generator())
        async for x in batcher.stream():
            count += 1
            assert x["num_generated_tokens"] == 100
            assert x["generated_text"] == TEXT_VALUE * 100

        # Should only have been called once
        assert count == 1
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    async def test_batch_timing(self):
        count = 0
        batcher = TestBatcher(fake_generator_slow(num_batches=10))
        async for _x in batcher.stream():
            count += 1

        assert 9 <= count <= 12, (
            "Count should have been called between 9 and 12 times, "
            "because each iteration takes 1/10th of an interval to yield."
        )
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    async def test_batch_last_return_is_immediate(self):
        """Test that we don't wait the entire interval for
        the last response if it returns quickly."""
        count = 0
        token_count = 0
        batcher = TestBatcher(fake_generator_slow_last_return_immediate())
        last_response = None
        async for _x in batcher.stream():
            count += 1
            token_count += _x["num_generated_tokens"]
            last_response = _x

        assert (
            last_response["generated_text"] == TEXT_VALUE + FINAL_TEXT_VALUE
        ), "the last generated response should be batched with previous one"
        assert token_count == 11, "token_count should be exactly 11"
        assert (
            count == 10
        ), "Count should have been called exactly 10 times (as many as we generated - 1)"
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    async def test_batch_no_interval(self):
        """Check that the class creates only one batch if there's no interval."""

        batcher = TestBatcher(fake_generator_slow(num_batches=10), interval_ms=None)

        count = 0
        async for _x in batcher.stream():
            count += 1

        assert count == 1
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("interval_ms", [100, None])
    async def test_exception_propagation(self, interval_ms: Optional[float]):
        """Test that exceptions are propagated correctly to parent."""

        async def generator_should_raise():
            for _i in range(100):
                await asyncio.sleep(0.01)
                yield dict(num_generated_tokens=1, generated_text=TEXT_VALUE)
                raise ValueError()

        count = 0
        batched = TestBatcher(generator_should_raise(), interval_ms=interval_ms)

        async def parent():
            nonlocal count
            nonlocal batched
            async for _x in batched.stream():
                count += 1

        task = asyncio.create_task(parent())
        await asyncio.sleep(0.2)

        with pytest.raises(ValueError):
            task.result()
        assert count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("interval_ms", [100, None])
    @pytest.mark.parametrize("to_cancel", ["parent", "inner", "stream"])
    async def test_cancellation(self, interval_ms: Optional[float], to_cancel: str):
        """There are 3 ways cancellation can happen:
        1. The parent is cancelled
        2. The generator is cancelled
        3. The stream task is directly cancelled.

        Make sure all associated tasks are cancelled in each instance.
        """

        async def generator_should_raise():
            with pytest.raises(asyncio.CancelledError):
                for _i in range(100):
                    await asyncio.sleep(0.01)
                    yield dict(num_generated_tokens=1, generated_text=TEXT_VALUE)
                    if to_cancel == "inner":
                        raise asyncio.CancelledError()

        batched = TestBatcher(generator_should_raise(), interval_ms=interval_ms)

        async def parent():
            nonlocal batched
            async for _x in batched.stream():
                pass

        task = asyncio.create_task(parent())
        await asyncio.sleep(0.2)

        cancel_task = {
            "parent": task,
            "stream": batched.read_task,
        }.get(to_cancel)

        if cancel_task:
            assert not task.done()
            assert not batched.read_task.done()
            cancel_task.cancel()

        await asyncio.sleep(0.3)
        assert batched.read_task.done(), "Read task should be completed"
        assert task.done(), "All tasks should be done"

        # Inner task is checked automatically with pytest.raises

    @pytest.mark.asyncio
    async def test_stable_streaming(self):
        """Test that the batcher does not add jitter to the stream when interval_ms is 0"""

        async def generator():
            for i in range(100):
                await asyncio.sleep(0.01)
                yield i

        concurrency = 10

        output_intervals = await asyncio.gather(
            *[
                count_interval_ms_from_stream(
                    Batcher(generator(), interval_ms=0).stream()
                )
                for _ in range(concurrency)
            ]
        )
        mean_batcher_interval = np.mean(output_intervals)
        std_batcher_interval = np.std(output_intervals)

        generator_intervals = await asyncio.gather(
            *[count_interval_ms_from_stream(generator()) for _ in range(concurrency)]
        )
        mean_generator_interval = np.mean(generator_intervals)
        std_generator_interval = np.std(generator_intervals)

        assert np.isclose(
            mean_batcher_interval, mean_generator_interval, rtol=0.1
        ), f"{mean_batcher_interval=}, {mean_generator_interval=}"
        assert np.isclose(
            std_batcher_interval, std_generator_interval, atol=0.1
        ), f"{std_batcher_interval=}, {std_generator_interval=}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
