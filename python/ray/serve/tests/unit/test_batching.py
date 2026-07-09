import asyncio
import logging
import time
from typing import List

import pytest

import ray
from ray import serve
from ray._common.signature import extract_signature, flatten_args
from ray._common.utils import get_or_create_event_loop
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.batching import _BatchQueue, _LazyBatchQueueWrapper, _SingleRequest
from ray.serve.exceptions import RayServeException

# Setup the global replica context for the test.
default_deployment_config = DeploymentConfig()
ray.serve.context._set_internal_replica_context(
    replica_id=ReplicaID(unique_id="test", deployment_id=DeploymentID(name="test")),
    servable_object=None,
    _deployment_config=default_deployment_config,
    rank=0,
    world_size=1,
)


class FakeStream:
    def __init__(self):
        self.messages = []

    def write(self, buf):
        self.messages.append(buf)

    def reset_message(self):
        self.messages = []


def _make_request(func, request, model_id: str = ""):
    future = get_or_create_event_loop().create_future()
    request_context = ray.serve.context._RequestContext(
        multiplexed_model_id=model_id
    )
    single_request = _SingleRequest(
        self_arg=None,
        flattened_args=flatten_args(extract_signature(func), (request,), {}),
        future=future,
        request_context=request_context,
        trace_context=None,
    )
    return single_request, future


# We use a single event loop for the entire test session. Without this
# fixture, the event loop is sometimes prematurely terminated by pytest.
@pytest.fixture(scope="session")
def event_loop():
    loop = get_or_create_event_loop()
    yield loop
    loop.close()


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
            _ = 1 / 0
        return requests

    class LongTimeout:
        @serve.batch(max_batch_size=1, batch_wait_timeout_s=1000)
        async def long_timeout(self, requests):
            if "raise" in requests:
                _ = 1 / 0
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
            _ = 1 / 0
        return requests

    class ZeroTimeout:
        @serve.batch(max_batch_size=2, batch_wait_timeout_s=0)
        async def zero_timeout(self, requests):
            await block_execution_event.wait()
            if "raise" in requests:
                _ = 1 / 0
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
            _ = 1 / 0
        return requests

    class LongTimeout:
        @serve.batch(max_batch_size=3, batch_wait_timeout_s=1000)
        async def long_timeout(self, requests):
            if "raise" in requests:
                _ = 1 / 0
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
async def test_multiplexed_batches_fill_per_model_id():
    batch_records = []

    @serve.batch(max_batch_size=5, batch_wait_timeout_s=1000)
    async def multiplexed_batch(requests):
        model_id = serve.get_multiplexed_model_id()
        batch_records.append((model_id, len(requests), list(requests)))
        return [f"{model_id}:{request}" for request in requests]

    async def call(model_id: str, request: str):
        ray.serve.context._set_request_context(multiplexed_model_id=model_id)
        try:
            return await multiplexed_batch(request)
        finally:
            ray.serve.context._unset_request_context()

    tasks = []
    for i in range(5):
        tasks.append(get_or_create_event_loop().create_task(call("model-a", f"a-{i}")))
        tasks.append(get_or_create_event_loop().create_task(call("model-b", f"b-{i}")))

    assert await asyncio.gather(*tasks) == [
        "model-a:a-0",
        "model-b:b-0",
        "model-a:a-1",
        "model-b:b-1",
        "model-a:a-2",
        "model-b:b-2",
        "model-a:a-3",
        "model-b:b-3",
        "model-a:a-4",
        "model-b:b-4",
    ]

    assert sorted((model_id, size) for model_id, size, _ in batch_records) == [
        ("model-a", 5),
        ("model-b", 5),
    ]


@pytest.mark.asyncio
async def test_multiplexed_batches_do_not_run_before_model_batch_fills():
    batch_records = []

    @serve.batch(max_batch_size=5, batch_wait_timeout_s=1000)
    async def multiplexed_batch(requests):
        model_id = serve.get_multiplexed_model_id()
        batch_records.append((model_id, len(requests), list(requests)))
        return [f"{model_id}:{request}" for request in requests]

    async def call(model_id: str, request: str):
        ray.serve.context._set_request_context(multiplexed_model_id=model_id)
        try:
            return await multiplexed_batch(request)
        finally:
            ray.serve.context._unset_request_context()

    model_a_tasks = [
        get_or_create_event_loop().create_task(call("model-a", f"a-{i}"))
        for i in range(4)
    ]
    first_model_b_task = get_or_create_event_loop().create_task(call("model-b", "b-0"))

    done, _ = await asyncio.wait(model_a_tasks + [first_model_b_task], timeout=0.05)
    assert len(done) == 0
    assert batch_records == []

    final_model_a_task = get_or_create_event_loop().create_task(call("model-a", "a-4"))
    assert await asyncio.wait_for(
        asyncio.gather(*model_a_tasks, final_model_a_task), timeout=1
    ) == [
        "model-a:a-0",
        "model-a:a-1",
        "model-a:a-2",
        "model-a:a-3",
        "model-a:a-4",
    ]

    done, _ = await asyncio.wait([first_model_b_task], timeout=0.05)
    assert len(done) == 0
    assert batch_records == [("model-a", 5, ["a-0", "a-1", "a-2", "a-3", "a-4"])]

    remaining_model_b_tasks = [
        get_or_create_event_loop().create_task(call("model-b", f"b-{i}"))
        for i in range(1, 5)
    ]
    assert await asyncio.wait_for(
        asyncio.gather(first_model_b_task, *remaining_model_b_tasks), timeout=1
    ) == [
        "model-b:b-0",
        "model-b:b-1",
        "model-b:b-2",
        "model-b:b-3",
        "model-b:b-4",
    ]

    assert batch_records == [
        ("model-a", 5, ["a-0", "a-1", "a-2", "a-3", "a-4"]),
        ("model-b", 5, ["b-0", "b-1", "b-2", "b-3", "b-4"]),
    ]


@pytest.mark.asyncio
async def test_multiplexed_batch_queues_share_max_concurrent_batches():
    running_batches = 0
    max_running_batches = 0
    batch_started_event = asyncio.Event()
    finish_batches_event = asyncio.Event()

    async def blocked_batch(requests):
        nonlocal running_batches, max_running_batches

        running_batches += 1
        max_running_batches = max(max_running_batches, running_batches)
        if running_batches == 2:
            batch_started_event.set()

        await finish_batches_event.wait()
        running_batches -= 1
        return requests

    wrapper = _LazyBatchQueueWrapper(
        max_batch_size=1,
        batch_wait_timeout_s=0,
        max_concurrent_batches=2,
        handle_batch_func=blocked_batch,
    )

    futures = []
    for model_id in ["model-a", "model-b", "model-c"]:
        future = get_or_create_event_loop().create_future()
        request_context = ray.serve.context._RequestContext(
            multiplexed_model_id=model_id
        )
        wrapper.get_queue(model_id).put(
            _SingleRequest(
                self_arg=None,
                flattened_args=flatten_args(
                    extract_signature(blocked_batch), (model_id,), {}
                ),
                future=future,
                request_context=request_context,
                trace_context=None,
            )
        )
        futures.append(future)

    await asyncio.wait_for(batch_started_event.wait(), timeout=1)
    await asyncio.sleep(0.05)

    assert running_batches == 2
    assert max_running_batches == 2
    assert not futures[2].done()

    finish_batches_event.set()
    assert await asyncio.wait_for(asyncio.gather(*futures), timeout=1) == [
        "model-a",
        "model-b",
        "model-c",
    ]
    assert max_running_batches == 2


@pytest.mark.asyncio
async def test_idle_multiplexed_batch_queue_cleanup():
    async def no_op(requests):
        return requests

    wrapper = _LazyBatchQueueWrapper(max_batch_size=1, handle_batch_func=no_op)
    default_queue = wrapper.get_queue("")
    model_queue = wrapper.get_queue("model-a")
    model_queue_task = model_queue._handle_batch_task

    wrapper._remove_queue_if_idle("", default_queue)
    assert wrapper._queues[""] is default_queue

    wrapper._remove_queue_if_idle("model-a", model_queue)
    await asyncio.sleep(0)

    assert "model-a" not in wrapper._queues
    assert model_queue_task.cancelled()


@pytest.mark.asyncio
async def test_active_multiplexed_batch_queue_not_cleaned_up():
    async def no_op(requests):
        return requests

    wrapper = _LazyBatchQueueWrapper(max_batch_size=1, handle_batch_func=no_op)
    model_queue = wrapper.get_queue("model-a")
    active_task = get_or_create_event_loop().create_task(asyncio.sleep(1000))
    model_queue.tasks.add(active_task)

    try:
        wrapper._remove_queue_if_idle("model-a", model_queue)

        assert wrapper._queues["model-a"] is model_queue
        assert not model_queue._handle_batch_task.cancelled()
    finally:
        active_task.cancel()
        model_queue._handle_batch_task.cancel()


@pytest.mark.asyncio
async def test_pending_multiplexed_batch_queue_not_cleaned_up():
    async def no_op(requests):
        return requests

    wrapper = _LazyBatchQueueWrapper(max_batch_size=1, handle_batch_func=no_op)
    model_queue = wrapper.get_queue("model-a")
    model_queue._pending_batch_count = 1

    try:
        wrapper._remove_queue_if_idle("model-a", model_queue)

        assert wrapper._queues["model-a"] is model_queue
        assert not model_queue._handle_batch_task.cancelled()
    finally:
        model_queue._pending_batch_count = 0
        model_queue._handle_batch_task.cancel()


@pytest.mark.asyncio
async def test_accumulating_multiplexed_batch_queue_not_cleaned_up():
    batch_started_event = asyncio.Event()
    finish_batch_event = asyncio.Event()

    async def blocked_batch(requests):
        batch_started_event.set()
        await finish_batch_event.wait()
        return requests

    wrapper = _LazyBatchQueueWrapper(
        max_batch_size=2,
        batch_wait_timeout_s=1000,
        max_concurrent_batches=1,
        handle_batch_func=blocked_batch,
    )
    model_queue = wrapper.get_queue("model-a")

    try:
        first_request, first_future = _make_request(
            blocked_batch, "request-1", "model-a"
        )
        second_request, second_future = _make_request(
            blocked_batch, "request-2", "model-a"
        )
        model_queue.put(first_request)
        model_queue.put(second_request)

        await asyncio.wait_for(batch_started_event.wait(), timeout=1)

        third_request, third_future = _make_request(
            blocked_batch, "request-3", "model-a"
        )
        model_queue.put(third_request)

        for _ in range(100):
            if model_queue.queue.empty():
                break
            await asyncio.sleep(0)
        assert model_queue.queue.empty()

        finish_batch_event.set()
        assert await asyncio.wait_for(
            asyncio.gather(first_future, second_future), timeout=1
        ) == ["request-1", "request-2"]

        await asyncio.sleep(0)
        assert wrapper._queues["model-a"] is model_queue
        assert not model_queue._handle_batch_task.cancelled()
        assert not third_future.done()

        fourth_request, fourth_future = _make_request(
            blocked_batch, "request-4", "model-a"
        )
        model_queue.put(fourth_request)

        assert await asyncio.wait_for(
            asyncio.gather(third_future, fourth_future), timeout=1
        ) == ["request-3", "request-4"]

        for _ in range(10):
            if "model-a" not in wrapper._queues:
                break
            await asyncio.sleep(0)
        assert "model-a" not in wrapper._queues
    finally:
        if "model-a" in wrapper._queues:
            model_queue.shutdown()


@pytest.mark.asyncio
async def test_cancelled_pending_batch_sets_dequeued_request_exceptions():
    async def no_op(requests):
        return requests

    semaphore = asyncio.Semaphore(1)
    await semaphore.acquire()

    queue = _BatchQueue(
        max_batch_size=1,
        batch_wait_timeout_s=0,
        max_concurrent_batches=1,
        handle_batch_func=no_op,
        semaphore=semaphore,
    )
    request, future = _make_request(no_op, "request")
    queue.put(request)

    try:
        for _ in range(100):
            if queue._pending_batch_count == 1 and queue.queue.empty():
                break
            await asyncio.sleep(0)
        assert queue._pending_batch_count == 1
        assert queue.queue.empty()

        queue.shutdown()
        with pytest.raises(asyncio.CancelledError):
            await queue._handle_batch_task

        assert future.done()
        with pytest.raises(asyncio.CancelledError):
            future.result()
    finally:
        semaphore.release()
        queue.shutdown()


@pytest.mark.asyncio
async def test_completed_multiplexed_batch_queue_cleans_itself_up():
    async def no_op(requests):
        return requests

    wrapper = _LazyBatchQueueWrapper(max_batch_size=1, handle_batch_func=no_op)
    model_queue = wrapper.get_queue("model-a")
    future = get_or_create_event_loop().create_future()
    request_context = ray.serve.context._RequestContext(multiplexed_model_id="model-a")

    model_queue.put(
        _SingleRequest(
            self_arg=None,
            flattened_args=flatten_args(extract_signature(no_op), ("request",), {}),
            future=future,
            request_context=request_context,
            trace_context=None,
        )
    )

    assert await future == "request"

    for _ in range(10):
        if "model-a" not in wrapper._queues:
            break
        await asyncio.sleep(0)

    assert "model-a" not in wrapper._queues


@pytest.mark.asyncio
async def test_cancelled_multiplexed_batch_queue_cleans_itself_up():
    async def no_op(requests):
        return requests

    wrapper = _LazyBatchQueueWrapper(
        max_batch_size=10,
        batch_wait_timeout_s=0,
        handle_batch_func=no_op,
    )
    model_queue = wrapper.get_queue("model-a")
    future = get_or_create_event_loop().create_future()
    future.cancel()
    request_context = ray.serve.context._RequestContext(multiplexed_model_id="model-a")

    model_queue.put(
        _SingleRequest(
            self_arg=None,
            flattened_args=flatten_args(extract_signature(no_op), ("request",), {}),
            future=future,
            request_context=request_context,
            trace_context=None,
        )
    )

    for _ in range(10):
        if "model-a" not in wrapper._queues:
            break
        await asyncio.sleep(0)

    assert "model-a" not in wrapper._queues


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
        tasks = [asyncio.create_task(gen.__anext__()) for gen in gens]
    else:
        tasks = [
            asyncio.create_task(func("hi1", "hi2")),
            asyncio.create_task(func("hi3", "hi4")),
        ]

    print("Submitted requests.")

    # The requests should be blocked on the long request_timeout
    done, pending = await asyncio.wait(tasks, timeout=0.01)
    assert len(done) == 0
    assert len(pending) == 2

    print("Requests are blocked, as expected.")

    # Cancel the first request. The second request should still be blocked on
    # the long request_timeout
    tasks[0].cancel()
    pending, done = await asyncio.wait(tasks, timeout=0.01)
    assert len(done) == 1
    assert len(pending) == 1

    print("Cancelled first request.")

    # Cancel the second request. Both requests should be done.
    tasks[1].cancel()
    done, pending = await asyncio.wait(tasks, timeout=0.01)
    assert len(done) == 2
    assert len(pending) == 0

    print("Cancelled second request. Sending new requests with no timeout.")

    # Sanity check that the request was actually cancelled.
    assert request_was_cancelled

    # Unblock requests. The requests should succeed.
    block_requests.set()

    if use_gen:
        gens = [func("hi1", "hi2"), func("hi3", "hi4")]
        tasks = [asyncio.create_task(gen.__anext__()) for gen in gens]
    else:
        tasks = [
            asyncio.create_task(func("hi1", "hi2")),
            asyncio.create_task(func("hi3", "hi4")),
        ]

    result = await asyncio.gather(*tasks)
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
        tasks = [asyncio.create_task(gen.__anext__()) for gen in gens]
    else:
        tasks = [
            asyncio.create_task(func("hi1", "hi2")),
            asyncio.create_task(func("hi3", "hi4")),
        ]

    print("Submitted initial batch of requests.")

    for task in tasks:
        task.cancel()

    print("Closed initial batch of requests.")

    raise_error.set()

    # Submit requests and check that they still work.
    if use_gen:
        gens = [func("hi1", "hi2"), func("hi3", "hi4")]
        tasks = [asyncio.create_task(gen.__anext__()) for gen in gens]
    else:
        tasks = [
            asyncio.create_task(func("hi1", "hi2")),
            asyncio.create_task(func("hi3", "hi4")),
        ]

    print("Submitted new batch of requests.")

    result = await asyncio.gather(*tasks)
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


@pytest.mark.asyncio
async def test_batch_size_fn_deferred_item_early_break():
    batches_processed = []

    @serve.batch(
        max_batch_size=10,
        batch_wait_timeout_s=0.05,
        batch_size_fn=lambda items: sum(item["size"] for item in items),
    )
    async def batch_handler(requests):
        batches_processed.append([req["value"] for req in requests])
        return [req["value"] for req in requests]

    # Request 1: size=6 (fits in batch)
    # Request 2: size=6 (would make total 12 > 10, should be deferred)
    # Each should be processed in its own batch
    t1 = get_or_create_event_loop().create_task(
        batch_handler({"size": 6, "value": "first"})
    )
    t2 = get_or_create_event_loop().create_task(
        batch_handler({"size": 6, "value": "second"})
    )

    done, pending = await asyncio.wait([t1, t2], timeout=1.0)

    assert len(done) == 2, "Both tasks should complete"
    assert len(pending) == 0

    results = {t1.result(), t2.result()}
    assert results == {"first", "second"}

    # Verify they were processed in separate batches due to size constraint
    assert (
        len(batches_processed) == 2
    ), f"Expected 2 separate batches, got {batches_processed}"


@pytest.mark.asyncio
async def test_batch_size_fn_fail_to_fit():
    batches_processed = []

    @serve.batch(
        max_batch_size=10,
        batch_wait_timeout_s=0.05,
        batch_size_fn=lambda items: sum(item["size"] for item in items),
    )
    async def batch_handler(requests):
        batches_processed.append([req["value"] for req in requests])
        return [req["value"] for req in requests]

    # Request 1: size=6 (fits in batch)
    # Request 2: size=6 (would make total 12 > 10, should be deferred)
    # Each should be processed in its own batch
    t1 = get_or_create_event_loop().create_task(
        batch_handler({"size": 6, "value": "first"})
    )
    t2 = get_or_create_event_loop().create_task(
        batch_handler({"size": 12, "value": "second"})
    )

    t1_result = await t1
    assert t1_result == "first"
    with pytest.raises(RuntimeError):
        await t2


@pytest.mark.asyncio
async def test_batch_size_fn_multiple_items_fit():
    """Test that multiple items are batched together when they fit within max_batch_size."""
    batches_seen = []

    @serve.batch(
        max_batch_size=20,
        batch_wait_timeout_s=0.1,
        batch_size_fn=lambda items: sum(item["size"] for item in items),
    )
    async def batch_handler(requests):
        batch_values = [req["value"] for req in requests]
        batches_seen.append(batch_values)
        return batch_values

    # All three requests fit: 5 + 6 + 7 = 18 <= 20
    t1 = get_or_create_event_loop().create_task(
        batch_handler({"size": 5, "value": "a"})
    )
    t2 = get_or_create_event_loop().create_task(
        batch_handler({"size": 6, "value": "b"})
    )
    t3 = get_or_create_event_loop().create_task(
        batch_handler({"size": 7, "value": "c"})
    )

    done, pending = await asyncio.wait([t1, t2, t3], timeout=1.0)
    assert len(done) == 3
    assert len(pending) == 0

    # All three should be in the same batch since they fit
    assert len(batches_seen) == 1, f"Expected 1 batch, got {len(batches_seen)}"
    assert set(batches_seen[0]) == {"a", "b", "c"}


def test_warn_if_max_batch_size_exceeds_max_ongoing_requests():
    """Test warn_if_max_batch_size_exceeds_max_ongoing_requests() logged the warning
     message correctly.

    When the queue starts with or updated `max_batch_size` to be larger than
    max_ongoing_requests, log the warning to suggest configuring `max_ongoing_requests`.
    When the queue starts with or updated `max_batch_size` to be smaller or equal than
    max_ongoing_requests, no warning should be logged.
    """
    logger = logging.getLogger(SERVE_LOGGER_NAME)
    stream = FakeStream()
    stream_handler = logging.StreamHandler(stream)
    logger.addHandler(stream_handler)
    bound = default_deployment_config.max_ongoing_requests
    over_bound = bound + 1
    under_bound = bound - 1
    over_bound_warning_message = (
        f"`max_batch_size` ({over_bound}) * `max_concurrent_batches` "
        f"({1}) is larger than `max_ongoing_requests` "
        f"({bound}). This means the replica will never achieve "
        "the configured `max_batch_size` concurrently. Please update "
        "`max_ongoing_requests` to be >= `max_batch_size` * `max_concurrent_batches`.\n"
    )

    # Start queue above the bound will log warning. Start at under or at the bound will
    # not log warning
    for max_batch_size in [over_bound, under_bound, bound]:
        queue = _BatchQueue(
            max_batch_size=max_batch_size,
            batch_wait_timeout_s=1000,
            max_concurrent_batches=1,
        )
        if max_batch_size > bound:
            assert over_bound_warning_message in stream.messages
        else:
            assert over_bound_warning_message not in stream.messages
        stream.reset_message()

    # Update queue above the bound will log warning. Update at under or at the bound
    # will not log warning
    for max_batch_size in [over_bound, under_bound, bound]:
        queue.set_max_batch_size(max_batch_size)
        if max_batch_size > bound:
            assert over_bound_warning_message in stream.messages
        else:
            assert over_bound_warning_message not in stream.messages
        stream.reset_message()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
