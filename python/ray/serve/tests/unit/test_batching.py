import asyncio
import time
from typing import List

import pytest

from ray import serve
from ray._private.utils import get_or_create_event_loop
from ray.serve.exceptions import RayServeException


@pytest.mark.asyncio
async def test_batching_magic_attributes():
    class BatchingExample:
        def __init__(self):
            self.count = 0
            self.batch_sizes = set()

        @property
        def _ray_serve_max_batch_size(self):
            return self.count + 1

        @property
        def _ray_serve_batch_wait_timeout_s(self):
            return 0.1

        @serve.batch
        async def handle_batch(self, requests):
            self.count += 1
            batch_size = len(requests)
            self.batch_sizes.add(batch_size)
            return [batch_size] * batch_size

    batching_example = BatchingExample()

    for batch_size in range(1, 7):
        tasks = [
            get_or_create_event_loop().create_task(batching_example.handle_batch(1))
            for _ in range(batch_size)
        ]

        done, _ = await asyncio.wait(tasks, return_when="ALL_COMPLETED")
        assert set({task.result() for task in done}) == {batch_size}
        time.sleep(0.05)


@pytest.mark.asyncio
async def test_decorator_validation():
    @serve.batch
    async def function():
        pass

    @serve.batch(max_batch_size=10, batch_wait_timeout_s=1.5)
    async def function2():
        pass

    class Class:
        @serve.batch
        async def method(self):
            pass

    class Class2:
        @serve.batch(max_batch_size=10, batch_wait_timeout_s=1.5)
        async def method(self):
            pass

    with pytest.raises(TypeError, match="async def"):

        @serve.batch
        def non_async_function():
            pass

    with pytest.raises(TypeError, match="async def"):

        class NotAsync:
            @serve.batch
            def method(self, requests):
                pass

    with pytest.raises(ValueError):

        class ZeroBatch:
            @serve.batch(max_batch_size=0)
            async def method(self, requests):
                pass

    with pytest.raises(TypeError):

        class FloatNonIntBatch:
            @serve.batch(max_batch_size=1.1)
            async def method(self, requests):
                pass

    class FloatIntegerBatch:
        @serve.batch(max_batch_size=1.0)
        async def method(self, requests):
            pass

    with pytest.raises(ValueError):

        class NegativeTimeout:
            @serve.batch(batch_wait_timeout_s=-0.1)
            async def method(self, requests):
                pass

    class FloatZeroTimeout:
        @serve.batch(batch_wait_timeout_s=0.0)
        async def method(self, requests):
            pass

    class IntZeroTimeout:
        @serve.batch(batch_wait_timeout_s=0)
        async def method(self, requests):
            pass

    with pytest.raises(TypeError):

        class NonTimeout:
            @serve.batch(batch_wait_timeout_s="a")
            async def method(self, requests):
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize("use_class", [True, False])
async def test_batch_size_one_long_timeout(use_class):
    @serve.batch(max_batch_size=1, batch_wait_timeout_s=1000)
    async def long_timeout(requests):
        if "raise" in requests:
            1 / 0
        return requests

    class LongTimeout:
        @serve.batch(max_batch_size=1, batch_wait_timeout_s=1000)
        async def long_timeout(self, requests):
            if "raise" in requests:
                1 / 0
            return requests

    cls = LongTimeout()

    async def call(arg):
        if use_class:
            return await cls.long_timeout(arg)
        else:
            return await long_timeout(arg)

    assert await call("hi") == "hi"
    with pytest.raises(ZeroDivisionError):
        await call("raise")


@pytest.mark.asyncio
@pytest.mark.parametrize("use_class", [True, False])
async def test_batch_size_multiple_zero_timeout(use_class):
    block_execution_event = asyncio.Event()

    @serve.batch(max_batch_size=2, batch_wait_timeout_s=0)
    async def zero_timeout(requests):
        await block_execution_event.wait()
        if "raise" in requests:
            1 / 0
        return requests

    class ZeroTimeout:
        @serve.batch(max_batch_size=2, batch_wait_timeout_s=0)
        async def zero_timeout(self, requests):
            await block_execution_event.wait()
            if "raise" in requests:
                1 / 0
            return requests

    cls = ZeroTimeout()

    async def call(arg):
        if use_class:
            return await cls.zero_timeout(arg)
        else:
            return await zero_timeout(arg)

    block_execution_event.set()
    assert await call("hi") == "hi"
    with pytest.raises(ZeroDivisionError):
        await call("raise")
    block_execution_event.clear()

    # Check that 2 requests will be executed together if available.
    # The first should cause a size-one batch to be executed, then
    # the next two should be executed together (signaled by both
    # having the exception).
    t1 = get_or_create_event_loop().create_task(call("hi1"))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(t1), timeout=0.001)

    t2 = get_or_create_event_loop().create_task(call("hi2"))
    t3 = get_or_create_event_loop().create_task(call("raise"))

    block_execution_event.set()
    assert await t1 == "hi1"

    with pytest.raises(ZeroDivisionError):
        await t2
    with pytest.raises(ZeroDivisionError):
        await t3


@pytest.mark.asyncio
async def test_batch_timeout_empty_queue():
    """Check that Serve waits when creating batches.

    Serve should wait a full batch_wait_timeout_s after receiving the first
    request in the next batch before processing the batch.
    """

    @serve.batch(max_batch_size=10, batch_wait_timeout_s=0.25)
    async def no_op(requests):
        return ["No-op"] * len(requests)

    num_iterations = 2
    for iteration in range(num_iterations):
        tasks = [get_or_create_event_loop().create_task(no_op(None)) for _ in range(9)]
        done, _ = await asyncio.wait(tasks, timeout=0.05)

        # Due to the long timeout, none of the tasks should finish until a tenth
        # request is submitted
        assert len(done) == 0

        tasks.append(get_or_create_event_loop().create_task(no_op(None)))
        done, _ = await asyncio.wait(tasks, timeout=0.05)

        # All the timeout tasks should be finished
        assert set(tasks) == set(done)
        assert all(t.result() == "No-op" for t in tasks)

        if iteration < num_iterations - 1:
            # Leave queue empty for batch_wait_timeout_s between batches
            time.sleep(0.25)


@pytest.mark.asyncio
async def test_batch_wait_queue_exceeds_batch_size_race_condition():
    """Check that the wait queue can exceed the batch size.

    This test was added to guard against a race condition documented in
    https://github.com/ray-project/ray/pull/42705#discussion_r1466653910.
    """

    @serve.batch(max_batch_size=2, batch_wait_timeout_s=10000)
    async def no_op(requests):
        return ["No-op"] * len(requests)

    tasks = [get_or_create_event_loop().create_task(no_op(None)) for _ in range(10)]

    # All the tasks should finish.
    done, pending = await asyncio.wait(tasks, timeout=0.5)
    assert len(done) == len(tasks)
    assert len(pending) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("use_class", [True, False])
async def test_batch_size_multiple_long_timeout(use_class):
    @serve.batch(max_batch_size=3, batch_wait_timeout_s=1000)
    async def long_timeout(requests):
        if "raise" in requests:
            1 / 0
        return requests

    class LongTimeout:
        @serve.batch(max_batch_size=3, batch_wait_timeout_s=1000)
        async def long_timeout(self, requests):
            if "raise" in requests:
                1 / 0
            return requests

    cls = LongTimeout()

    async def call(arg):
        if use_class:
            return await cls.long_timeout(arg)
        else:
            return await long_timeout(arg)

    t1 = get_or_create_event_loop().create_task(call("hi1"))
    t2 = get_or_create_event_loop().create_task(call("hi2"))
    done, pending = await asyncio.wait([t1, t2], timeout=0.1)
    assert len(done) == 0
    t3 = get_or_create_event_loop().create_task(call("hi3"))
    done, pending = await asyncio.wait([t1, t2, t3], timeout=100)
    assert set(done) == {t1, t2, t3}
    assert [t1.result(), t2.result(), t3.result()] == ["hi1", "hi2", "hi3"]

    t1 = get_or_create_event_loop().create_task(call("hi1"))
    t2 = get_or_create_event_loop().create_task(call("raise"))
    done, pending = await asyncio.wait([t1, t2], timeout=0.1)
    assert len(done) == 0
    t3 = get_or_create_event_loop().create_task(call("hi3"))
    done, pending = await asyncio.wait([t1, t2, t3], timeout=100)
    assert set(done) == {t1, t2, t3}
    assert all(isinstance(t.exception(), ZeroDivisionError) for t in done)
    with pytest.raises(ZeroDivisionError):
        t1.result()
    with pytest.raises(ZeroDivisionError):
        t2.result()
    with pytest.raises(ZeroDivisionError):
        t3.result()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["args", "kwargs", "mixed", "out-of-order"])
@pytest.mark.parametrize("use_class", [True, False])
async def test_batch_args_kwargs(mode, use_class):
    if use_class:

        class MultipleArgs:
            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def method(self, key1, key2):
                return [(key1[i], key2[i]) for i in range(len(key1))]

        instance = MultipleArgs()
        func = instance.method

    else:

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def func(key1, key2):
            return [(key1[i], key2[i]) for i in range(len(key1))]

    if mode == "args":
        coros = [func("hi1", "hi2"), func("hi3", "hi4")]
    elif mode == "kwargs":
        coros = [func(key1="hi1", key2="hi2"), func(key1="hi3", key2="hi4")]
    elif mode == "mixed":
        coros = [func("hi1", key2="hi2"), func("hi3", key2="hi4")]
    elif mode == "out-of-order":
        coros = [func(key2="hi2", key1="hi1"), func(key2="hi4", key1="hi3")]

    result = await asyncio.gather(*coros)
    assert result == [("hi1", "hi2"), ("hi3", "hi4")]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_class", [True, False])
@pytest.mark.parametrize("use_gen", [True, False])
async def test_batch_cancellation(use_class, use_gen):
    block_requests = asyncio.Event()
    request_was_cancelled = True

    async def unary_implementation(key1, key2):
        nonlocal block_requests, request_was_cancelled
        await block_requests.wait()
        request_was_cancelled = False
        return [(key1[i], key2[i]) for i in range(len(key1))]

    async def streaming_implementation(key1, key2):
        nonlocal block_requests, request_was_cancelled
        await block_requests.wait()
        request_was_cancelled = False
        yield [(key1[i], key2[i]) for i in range(len(key1))]

    if use_class:

        class MultipleArgs:
            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def unary_method(self, key1, key2):
                return await unary_implementation(key1, key2)

            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def streaming_method(self, key1, key2):
                async for value in streaming_implementation(key1, key2):
                    yield value

        instance = MultipleArgs()

        if use_gen:
            func = instance.streaming_method
        else:
            func = instance.unary_method

    else:

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def unary_func(key1, key2):
            return await unary_implementation(key1, key2)

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def streaming_func(key1, key2):
            async for value in streaming_implementation(key1, key2):
                yield value

        if use_gen:
            func = streaming_func
        else:
            func = unary_func

    if use_gen:
        gens = [func("hi1", "hi2"), func("hi3", "hi4")]
        coros = [gen.__anext__() for gen in gens]
    else:
        coros = [func("hi1", "hi2"), func("hi3", "hi4")]

    print("Submitted requests.")

    # The requests should be blocked on the long request_timeout
    done, pending = await asyncio.wait(coros, timeout=0.01)
    assert len(done) == 0
    assert len(pending) == 2

    print("Requests are blocked, as expected.")

    # Cancel the first request. The second request should still be blocked on
    # the long request_timeout
    coros[0].close()
    pending, done = await asyncio.wait(coros, timeout=0.01)
    assert len(done) == 1
    assert len(pending) == 1

    print("Cancelled first request.")

    # Cancel the second request. Both requests should be done.
    coros[1].close()
    done, pending = await asyncio.wait(coros, timeout=0.01)
    assert len(done) == 2
    assert len(pending) == 0

    print("Cancelled second request. Sending new requests with no timeout.")

    # Sanity check that the request was actually cancelled.
    assert request_was_cancelled

    # Unblock requests. The requests should succeed.
    block_requests.set()

    if use_gen:
        gens = [func("hi1", "hi2"), func("hi3", "hi4")]
        coros = [gen.__anext__() for gen in gens]
    else:
        coros = [func("hi1", "hi2"), func("hi3", "hi4")]

    result = await asyncio.gather(*coros)
    assert result == [("hi1", "hi2"), ("hi3", "hi4")]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_class", [True, False])
@pytest.mark.parametrize("use_gen", [True, False])
async def test_cancellation_after_error(use_class, use_gen):
    """Cancelling a request after it errors should be supported."""

    raise_error = asyncio.Event()

    async def unary_implementation(key1, key2):
        if not raise_error.is_set():
            raise ValueError()
        return [(key1[i], key2[i]) for i in range(len(key1))]

    async def streaming_implementation(key1, key2):
        if not raise_error.is_set():
            raise ValueError()
        yield [(key1[i], key2[i]) for i in range(len(key1))]

    if use_class:

        class MultipleArgs:
            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def unary_method(self, key1, key2):
                return await unary_implementation(key1, key2)

            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def streaming_method(self, key1, key2):
                async for value in streaming_implementation(key1, key2):
                    yield value

        instance = MultipleArgs()

        if use_gen:
            func = instance.streaming_method
        else:
            func = instance.unary_method

    else:

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def unary_func(key1, key2):
            return await unary_implementation(key1, key2)

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def streaming_func(key1, key2):
            async for value in streaming_implementation(key1, key2):
                yield value

        if use_gen:
            func = streaming_func
        else:
            func = unary_func

    # Submit requests and then cancel them.
    if use_gen:
        gens = [func("hi1", "hi2"), func("hi3", "hi4")]
        coros = [gen.__anext__() for gen in gens]
    else:
        coros = [func("hi1", "hi2"), func("hi3", "hi4")]

    print("Submitted initial batch of requests.")

    for coro in coros:
        coro.close()

    print("Closed initial batch of requests.")

    raise_error.set()

    # Submit requests and check that they still work.
    if use_gen:
        gens = [func("hi1", "hi2"), func("hi3", "hi4")]
        coros = [gen.__anext__() for gen in gens]
    else:
        coros = [func("hi1", "hi2"), func("hi3", "hi4")]

    print("Submitted new batch of requests.")

    result = await asyncio.gather(*coros)
    assert result == [("hi1", "hi2"), ("hi3", "hi4")]


@pytest.mark.asyncio
@pytest.mark.parametrize("use_class", [True, False])
async def test_batch_setters(use_class):
    if use_class:

        class C:
            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def method(self, key1, key2):
                return [(key1[i], key2[i]) for i in range(len(key1))]

        instance = C()
        func = instance.method

    else:

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def func(key1, key2):
            return [(key1[i], key2[i]) for i in range(len(key1))]

    assert func._get_max_batch_size() == 2
    assert func._get_batch_wait_timeout_s() == 1000

    # @serve.batch should create batches of size 2
    tasks = [
        get_or_create_event_loop().create_task(func("hi1", "hi2")),
        get_or_create_event_loop().create_task(func("hi3", "hi4")),
    ]
    done, pending = await asyncio.wait(tasks, timeout=0.1)
    assert len(pending) == 0
    assert {task.result() for task in done} == {("hi1", "hi2"), ("hi3", "hi4")}

    # Set new values
    func.set_max_batch_size(3)
    func.set_batch_wait_timeout_s(15000)

    assert func._get_max_batch_size() == 3
    assert func._get_batch_wait_timeout_s() == 15000

    # @serve.batch should create batches of size 3
    tasks = [
        get_or_create_event_loop().create_task(func("hi1", "hi2")),
        get_or_create_event_loop().create_task(func("hi3", "hi4")),
        get_or_create_event_loop().create_task(func("hi5", "hi6")),
    ]
    done, pending = await asyncio.wait(tasks, timeout=0.1)
    assert len(pending) == 0
    assert {task.result() for task in done} == {
        ("hi1", "hi2"),
        ("hi3", "hi4"),
        ("hi5", "hi6"),
    }


@pytest.mark.asyncio
async def test_batch_use_earliest_setters():
    """@serve.batch should use the right settings when constructing a batch.

    When the @serve.batch setters get called before a batch has started
    accumulating, the next batch should use the setters' values. When they
    get called while a batch is accumulating, the previous values should be
    used.
    """

    @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
    async def func(key1, key2):
        return [(key1[i], key2[i]) for i in range(len(key1))]

    assert func._get_max_batch_size() == 2
    assert func._get_batch_wait_timeout_s() == 1000

    # Set new values
    func.set_max_batch_size(3)
    func.set_batch_wait_timeout_s(15000)

    assert func._get_max_batch_size() == 3
    assert func._get_batch_wait_timeout_s() == 15000

    # Should create batches of size 3, even if setters are called while
    # batch is accumulated
    tasks = [
        get_or_create_event_loop().create_task(func("hi1", "hi2")),
        get_or_create_event_loop().create_task(func("hi3", "hi4")),
    ]

    # Batch should be waiting for last request
    done, pending = await asyncio.wait(tasks, timeout=0.1)
    assert len(done) == 0 and len(pending) == 2

    func.set_max_batch_size(1)
    func.set_batch_wait_timeout_s(0)

    assert func._get_max_batch_size() == 1
    assert func._get_batch_wait_timeout_s() == 0

    # Batch should still be waiting for last request
    done, pending = await asyncio.wait(pending, timeout=0.1)
    assert len(done) == 0 and len(pending) == 2

    # Batch should execute after last request
    pending.add(get_or_create_event_loop().create_task(func("hi5", "hi6")))
    done, pending = await asyncio.wait(pending, timeout=0.1)
    assert len(pending) == 0
    assert {task.result() for task in done} == {
        ("hi1", "hi2"),
        ("hi3", "hi4"),
        ("hi5", "hi6"),
    }

    # Next batch should use updated values
    tasks = [get_or_create_event_loop().create_task(func("hi1", "hi2"))]
    done, pending = await asyncio.wait(tasks, timeout=0.1)
    assert len(done) == 1 and len(pending) == 0
    assert done.pop().result() == ("hi1", "hi2")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["args", "kwargs", "mixed", "out-of-order"])
@pytest.mark.parametrize("use_class", [True, False])
@pytest.mark.parametrize("generator_length", [0, 2, 5])
async def test_batch_generator_basic(mode, use_class, generator_length):
    if use_class:

        class MultipleArgs:
            @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
            async def method(self, key1, key2):
                for gen_idx in range(generator_length):
                    yield [(gen_idx, key1[i], key2[i]) for i in range(len(key1))]

        instance = MultipleArgs()
        func = instance.method

    else:

        @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
        async def func(key1, key2):
            for gen_idx in range(generator_length):
                yield [(gen_idx, key1[i], key2[i]) for i in range(len(key1))]

    if mode == "args":
        generators = [func("hi1", "hi2"), func("hi3", "hi4")]
    elif mode == "kwargs":
        generators = [func(key1="hi1", key2="hi2"), func(key1="hi3", key2="hi4")]
    elif mode == "mixed":
        generators = [func("hi1", key2="hi2"), func("hi3", key2="hi4")]
    elif mode == "out-of-order":
        generators = [func(key2="hi2", key1="hi1"), func(key2="hi4", key1="hi3")]

    results = [
        [result async for result in generators[0]],
        [result async for result in generators[1]],
    ]

    assert results == [
        [(gen_idx, "hi1", "hi2") for gen_idx in range(generator_length)],
        [(gen_idx, "hi3", "hi4") for gen_idx in range(generator_length)],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", ["runtime_error", "mismatched_lengths"])
async def test_batch_generator_exceptions(error_type):
    GENERATOR_LENGTH = 5
    ERROR_IDX = 2
    ERROR_MSG = "Testing error"

    @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
    async def func(key1, key2):
        for gen_idx in range(GENERATOR_LENGTH):
            results = [(gen_idx, key1[i], key2[i]) for i in range(len(key1))]
            if gen_idx == ERROR_IDX:
                if error_type == "runtime_error":
                    raise RuntimeError(ERROR_MSG)
                elif error_type == "mismatched_lengths":
                    yield results * 2
            yield results

    generators = [func("hi1", "hi2"), func("hi3", "hi4")]

    for generator in generators:
        for _ in range(ERROR_IDX):
            await generator.__anext__()

        if error_type == "runtime_error":
            with pytest.raises(RuntimeError, match=ERROR_MSG):
                await generator.__anext__()
        elif error_type == "mismatched_lengths":
            with pytest.raises(RayServeException):
                await generator.__anext__()

        with pytest.raises(StopAsyncIteration):
            await generator.__anext__()


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_token", [StopAsyncIteration, StopIteration])
async def test_batch_generator_early_termination(stop_token):
    NUM_CALLERS = 4

    event = asyncio.Event()

    @serve.batch(max_batch_size=NUM_CALLERS, batch_wait_timeout_s=1000)
    async def sequential_terminator(ids: List[int]):
        """Terminates callers one-after-another in order of call."""

        for num_finished_callers in range(1, NUM_CALLERS + 1):
            event.clear()
            responses = [stop_token for _ in range(num_finished_callers)]
            responses += [ids[idx] for idx in range(num_finished_callers, NUM_CALLERS)]
            yield responses
            await event.wait()

    ids = list(range(NUM_CALLERS))
    generators = [sequential_terminator(id) for id in ids]
    for id, generator in zip(ids, generators):
        async for result in generator:
            assert result == id

        # Each terminated caller frees the sequential_terminator to process
        # another iteration.
        event.set()


@pytest.mark.asyncio
async def test_batch_generator_setters():
    """@serve.batch setters should succeed while the current batch streams."""

    @serve.batch(max_batch_size=2, batch_wait_timeout_s=1000)
    async def yield_three_times(key1, key2):
        for _ in range(3):
            yield [(key1[i], key2[i]) for i in range(len(key1))]

    assert yield_three_times._get_max_batch_size() == 2
    assert yield_three_times._get_batch_wait_timeout_s() == 1000

    args_list = [("hi1", "hi2"), ("hi3", "hi4")]
    coros = [yield_three_times(*args) for args in args_list]

    # Partially consume generators
    for coro, expected_result in zip(coros, args_list):
        for _ in range(2):
            await coro.__anext__() == expected_result

    # Set new values
    yield_three_times.set_max_batch_size(3)
    yield_three_times.set_batch_wait_timeout_s(15000)

    assert yield_three_times._get_max_batch_size() == 3
    assert yield_three_times._get_batch_wait_timeout_s() == 15000

    # Execute three more requests
    args_list_2 = [("hi1", "hi2"), ("hi3", "hi4"), ("hi5", "hi6")]
    coros_2 = [yield_three_times(*args) for args in args_list_2]

    # Finish consuming original requests
    for coro, expected_result in zip(coros, args_list):
        await coro.__anext__() == expected_result
        with pytest.raises(StopAsyncIteration):
            await coro.__anext__()

    # Consume new requests
    for coro, expected_result in zip(coros_2, args_list_2):
        for _ in range(3):
            await coro.__anext__() == expected_result
        with pytest.raises(StopAsyncIteration):
            await coro.__anext__()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
