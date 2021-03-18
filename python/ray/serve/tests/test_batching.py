import asyncio

import pytest

import ray
from ray import serve
from ray.serve.config import BackendConfig


def test_batching(serve_instance):
    class BatchingExample:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, requests):
            self.count += 1
            batch_size = len(requests)
            return [self.count] * batch_size

    # set the max batch size
    config = BackendConfig(max_batch_size=5, batch_wait_timeout=1)
    serve.create_backend("counter:v11", BatchingExample, config=config)
    serve.create_endpoint(
        "counter1", backend="counter:v11", route="/increment2")

    future_list = []
    handle = serve.get_handle("counter1")
    for _ in range(20):
        f = handle.remote(temp=1)
        future_list.append(f)

    counter_result = ray.get(future_list)
    # since count is only updated per batch of queries
    # If there atleast one __call__ fn call with batch size greater than 1
    # counter result will always be less than 20
    assert max(counter_result) < 20


def test_batching_exception(serve_instance):
    class NoListReturned:
        def __init__(self):
            self.count = 0

        @serve.accept_batch
        def __call__(self, requests):
            return len(requests)

    # Set the max batch size.
    config = BackendConfig(max_batch_size=5)
    serve.create_backend("exception:v1", NoListReturned, config=config)
    serve.create_endpoint("exception-test", backend="exception:v1")

    handle = serve.get_handle("exception-test")
    with pytest.raises(ray.exceptions.RayTaskError):
        assert ray.get(handle.remote(temp=1))


def test_app_level_batching(serve_instance):
    class BatchingExample:
        def __init__(self):
            self.count = 0

        @serve.batch(max_batch_size=5, batch_wait_timeout_s=1)
        async def handle_batch(self, requests):
            self.count += 1
            batch_size = len(requests)
            return [self.count] * batch_size

        async def __call__(self, request):
            return await self.handle_batch(request)

    # set the max batch size
    serve.create_backend("counter:v11", BatchingExample)
    serve.create_endpoint(
        "counter1", backend="counter:v11", route="/increment2")

    future_list = []
    handle = serve.get_handle("counter1")
    for _ in range(20):
        f = handle.remote(temp=1)
        future_list.append(f)

    counter_result = ray.get(future_list)
    # since count is only updated per batch of queries
    # If there atleast one __call__ fn call with batch size greater than 1
    # counter result will always be less than 20
    assert max(counter_result) < 20


def test_app_level_batching_exception(serve_instance):
    class NoListReturned:
        def __init__(self):
            self.count = 0

        @serve.batch(max_batch_size=5)
        async def handle_batch(self, requests):
            return len(requests)

        async def __call__(self, request):
            return await self.handle_batch(request)

    # Set the max batch size.
    serve.create_backend("exception:v1", NoListReturned)
    serve.create_endpoint("exception-test", backend="exception:v1")

    handle = serve.get_handle("exception-test")
    with pytest.raises(ray.exceptions.RayTaskError):
        assert ray.get(handle.remote(temp=1))


@pytest.mark.asyncio
async def test_decorator_validation():
    @serve.batch
    async def function():
        pass

    @serve.batch(max_batch_size=10, batch_wait_timeout_s=1.5)
    async def function2():
        pass

    class Class():
        @serve.batch
        async def method(self):
            pass

    class Class2():
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
    @serve.batch(max_batch_size=2, batch_wait_timeout_s=0)
    async def zero_timeout(requests):
        await asyncio.sleep(1)
        if "raise" in requests:
            1 / 0
        return requests

    class ZeroTimeout:
        @serve.batch(max_batch_size=2, batch_wait_timeout_s=0)
        async def zero_timeout(self, requests):
            await asyncio.sleep(1)
            if "raise" in requests:
                1 / 0
            return requests

    cls = ZeroTimeout()

    async def call(arg):
        if use_class:
            return await cls.zero_timeout(arg)
        else:
            return await zero_timeout(arg)

    assert await call("hi") == "hi"
    with pytest.raises(ZeroDivisionError):
        await call("raise")

    # Check that 2 requests will be executed together if available.
    # The first should cause a size-one batch to be executed, then
    # the next two should be executed together (signaled by both
    # having the exception).
    t1 = asyncio.get_event_loop().create_task(call("hi1"))
    await asyncio.sleep(0.5)
    t2 = asyncio.get_event_loop().create_task(call("hi2"))
    t3 = asyncio.get_event_loop().create_task(call("raise"))

    assert await t1 == "hi1"

    with pytest.raises(ZeroDivisionError):
        await t2
    with pytest.raises(ZeroDivisionError):
        await t3


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

    t1 = asyncio.get_event_loop().create_task(call("hi1"))
    t2 = asyncio.get_event_loop().create_task(call("hi2"))
    done, pending = await asyncio.wait([t1, t2], timeout=0.1)
    assert len(done) == 0
    t3 = asyncio.get_event_loop().create_task(call("hi3"))
    done, pending = await asyncio.wait([t1, t2, t3], timeout=100)
    assert set(done) == {t1, t2, t3}
    assert [t1.result(), t2.result(), t3.result()] == ["hi1", "hi2", "hi3"]

    t1 = asyncio.get_event_loop().create_task(call("hi1"))
    t2 = asyncio.get_event_loop().create_task(call("raise"))
    done, pending = await asyncio.wait([t1, t2], timeout=0.1)
    assert len(done) == 0
    t3 = asyncio.get_event_loop().create_task(call("hi3"))
    done, pending = await asyncio.wait([t1, t2, t3], timeout=100)
    assert set(done) == {t1, t2, t3}
    assert all(isinstance(t.exception(), ZeroDivisionError) for t in done)
    with pytest.raises(ZeroDivisionError):
        t1.result()
    with pytest.raises(ZeroDivisionError):
        t2.result()
    with pytest.raises(ZeroDivisionError):
        t3.result()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
