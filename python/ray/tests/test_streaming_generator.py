import asyncio
import pytest
import numpy as np
import sys
import time
import threading
import gc

from unittest.mock import patch, Mock

import ray
from ray._private.test_utils import wait_for_condition
from ray.experimental.state.api import list_objects
from ray._raylet import ObjectRefGenerator, ObjectRefStreamEndOfStreamError
from ray.cloudpickle import dumps
from ray.exceptions import WorkerCrashedError


class MockedWorker:
    def __init__(self, mocked_core_worker):
        self.core_worker = mocked_core_worker

    def reset_core_worker(self):
        """Emulate the case ray.shutdown is called
        and the core_worker instance is GC'ed.
        """
        self.core_worker = None

    def check_connected(self):
        return True


@pytest.fixture
def mocked_worker():
    mocked_core_worker = Mock()
    mocked_core_worker.try_read_next_object_ref_stream.return_value = None
    mocked_core_worker.delete_object_ref_stream.return_value = None
    mocked_core_worker.create_object_ref_stream.return_value = None
    mocked_core_worker.peek_object_ref_stream.return_value = [], []
    worker = MockedWorker(mocked_core_worker)
    yield worker


def test_streaming_object_ref_generator_basic_unit(mocked_worker):
    """
    Verify the basic case:
    create a generator -> read values -> nothing more to read -> delete.
    """
    with patch("ray.wait") as mocked_ray_wait:
        with patch("ray.get") as mocked_ray_get:
            c = mocked_worker.core_worker
            generator_ref = ray.ObjectRef.from_random()
            generator = ObjectRefGenerator(generator_ref, mocked_worker)

            # Test when there's no new ref, it returns a nil.
            new_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = (new_ref, False)
            mocked_ray_wait.return_value = [], [new_ref]
            ref = generator._next_sync(timeout_s=0)
            assert ref.is_nil()

            # When the new ref is available, next should return it.
            # When peek_object_ref_stream returns is_ready = True,
            # it shouldn't call ray.wait.
            new_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = (new_ref, True)
            c.try_read_next_object_ref_stream.return_value = new_ref
            ref = generator._next_sync(timeout_s=0)
            assert new_ref == ref

            # When the new ref is available, next should return it.
            # When peek_object_ref_stream returns is_ready = False,
            # it should wait until ray.wait returns.
            for _ in range(3):
                new_ref = ray.ObjectRef.from_random()
                c.peek_object_ref_stream.return_value = (new_ref, False)
                mocked_ray_wait.return_value = [new_ref], []
                c.try_read_next_object_ref_stream.return_value = new_ref
                ref = generator._next_sync(timeout_s=0)
                assert new_ref == ref

            # When try_read_next_object_ref_stream raises a
            # ObjectRefStreamEndOfStreamError, it should raise a stop iteration.
            new_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = (new_ref, True)
            c.try_read_next_object_ref_stream.side_effect = (
                ObjectRefStreamEndOfStreamError("")
            )  # noqa
            mocked_ray_get.return_value = None
            with pytest.raises(StopIteration):
                ref = generator._next_sync(timeout_s=0)
            # Make sure we cannot serialize the generator.
            with pytest.raises(TypeError):
                dumps(generator)

            del generator
            c.delete_object_ref_stream.assert_called()


def test_streaming_object_ref_generator_task_failed_unit(mocked_worker):
    """
    Verify when a task is failed by a system error,
    the generator ref is returned.
    """
    with patch("ray.get") as mocked_ray_get:
        with patch("ray.wait") as mocked_ray_wait:
            c = mocked_worker.core_worker
            generator_ref = ray.ObjectRef.from_random()
            generator = ObjectRefGenerator(generator_ref, mocked_worker)

            # Simulate the worker failure happens.
            next_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = (next_ref, False)
            mocked_ray_wait.return_value = [next_ref], []
            mocked_ray_get.side_effect = WorkerCrashedError()

            c.try_read_next_object_ref_stream.side_effect = (
                ObjectRefStreamEndOfStreamError("")
            )  # noqa
            ref = generator._next_sync(timeout_s=0)
            # If the generator task fails by a systsem error,
            # meaning the ref will raise an exception
            # it should be returned.
            assert ref == generator_ref

            # Once exception is raised, it should always
            # raise stopIteration regardless of what
            # the ref contains now.
            with pytest.raises(StopIteration):
                ref = generator._next_sync(timeout_s=0)


def test_generator_basic(shutdown_only):
    ray.init(num_cpus=1)

    """Basic cases"""
    print("Test basic case")

    @ray.remote
    def f():
        for i in range(5):
            yield i

    gen = f.remote()
    i = 0
    for ref in gen:
        print(ray.get(ref))
        assert i == ray.get(ref)
        del ref
        i += 1

    """Exceptions"""
    print("Test exceptions")

    @ray.remote
    def f():
        for i in range(5):
            if i == 2:
                raise ValueError
            yield i

    gen = f.remote()
    print(ray.get(next(gen)))
    print(ray.get(next(gen)))
    with pytest.raises(ray.exceptions.RayTaskError) as e:
        print(ray.get(next(gen)))
    with pytest.raises(StopIteration):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))

    """Generator Task failure"""
    print("Test task failures")

    @ray.remote
    class A:
        def getpid(self):
            import os

            return os.getpid()

        def f(self):
            for i in range(5):
                time.sleep(1)
                yield i

    a = A.remote()
    gen = a.f.remote()
    i = 0
    for ref in gen:
        if i == 2:
            ray.kill(a)
        if i == 3:
            with pytest.raises(ray.exceptions.RayActorError) as e:
                ray.get(ref)
            assert "The actor is dead because it was killed by `ray.kill`" in str(
                e.value
            )
            break
        assert i == ray.get(ref)
        del ref
        i += 1
    for _ in range(10):
        with pytest.raises(StopIteration):
            next(gen)

    """Retry exceptions"""
    print("Test retry exceptions")

    @ray.remote
    class Actor:
        def __init__(self):
            self.should_kill = True

        def should_kill(self):
            return self.should_kill

        async def set(self, wait_s):
            await asyncio.sleep(wait_s)
            self.should_kill = False

    @ray.remote(retry_exceptions=[ValueError], max_retries=10)
    def f(a):
        for i in range(5):
            should_kill = ray.get(a.should_kill.remote())
            if i == 3 and should_kill:
                raise ValueError
            yield i

    a = Actor.remote()
    gen = f.remote(a)
    assert ray.get(next(gen)) == 0
    assert ray.get(next(gen)) == 1
    assert ray.get(next(gen)) == 2
    a.set.remote(3)
    assert ray.get(next(gen)) == 3
    assert ray.get(next(gen)) == 4
    with pytest.raises(StopIteration):
        ray.get(next(gen))

    """Cancel"""
    print("Test cancel")

    @ray.remote
    def f():
        for i in range(5):
            time.sleep(5)
            yield i

    gen = f.remote()
    assert ray.get(next(gen)) == 0
    ray.cancel(gen)
    with pytest.raises(ray.exceptions.RayTaskError) as e:
        assert ray.get(next(gen)) == 1
    assert "was cancelled" in str(e.value)
    with pytest.raises(StopIteration):
        next(gen)


def test_streaming_generator_bad_exception_not_failing(shutdown_only, capsys):
    """This test verifies when a return value cannot be stored
        e.g., because it holds a lock) if it handles failures gracefully.

    Previously, when it happens, there was a check failure. This verifies
    the check failure doesn't happen anymore.
    """
    ray.init()

    class UnserializableException(Exception):
        def __init__(self):
            self.lock = threading.Lock()

    @ray.remote
    def f():
        raise UnserializableException
        yield 1  # noqa

    for ref in f.remote():
        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(ref)
    captured = capsys.readouterr()
    lines = captured.err.strip().split("\n")

    # Verify check failure doesn't happen because we handle the error
    # properly.
    for line in lines:
        assert "Check failed:" not in line


@pytest.mark.parametrize("crash_type", ["exception", "worker_crash"])
def test_generator_streaming_no_leak_upon_failures(
    monkeypatch, shutdown_only, crash_type
):
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_asio_delay_us",
            "CoreWorkerService.grpc_server.ReportGeneratorItemReturns=100000:1000000",
        )
        ray.init(num_cpus=1)

        @ray.remote
        def g():
            try:
                gen = f.remote()
                for ref in gen:
                    print(ref)
                    ray.get(ref)
            except Exception:
                print("exception!")
                del ref

            del gen
            gc.collect()

            # Only the ref g is alive.
            def verify():
                print(list_objects())
                return len(list_objects()) == 1

            wait_for_condition(verify)
            return True

        @ray.remote
        def f():
            for i in range(10):
                time.sleep(0.2)
                if i == 4:
                    if crash_type == "exception":
                        raise ValueError
                    else:
                        sys.exit(9)
                yield 2

        for _ in range(5):
            ray.get(g.remote())


@pytest.mark.parametrize("use_actors", [False, True])
@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_generator_streaming(shutdown_only, use_actors, store_in_plasma):
    """Verify the generator is working in a streaming fashion."""
    ray.init()
    remote_generator_fn = None
    if use_actors:

        @ray.remote
        class Generator:
            def __init__(self):
                pass

            def generator(self, num_returns, store_in_plasma):
                for i in range(num_returns):
                    if store_in_plasma:
                        yield np.ones(1_000_000, dtype=np.int8) * i
                    else:
                        yield [i]

        g = Generator.remote()
        remote_generator_fn = g.generator
    else:

        @ray.remote(max_retries=0)
        def generator(num_returns, store_in_plasma):
            for i in range(num_returns):
                if store_in_plasma:
                    yield np.ones(1_000_000, dtype=np.int8) * i
                else:
                    yield [i]

        remote_generator_fn = generator

    """Verify num_returns="streaming" is streaming"""
    gen = remote_generator_fn.remote(3, store_in_plasma)
    i = 0
    for ref in gen:
        id = ref.hex()
        if store_in_plasma:
            expected = np.ones(1_000_000, dtype=np.int8) * i
            assert np.array_equal(ray.get(ref), expected)
        else:
            expected = [i]
            assert ray.get(ref) == expected

        del ref

        wait_for_condition(
            lambda id=id: len(list_objects(filters=[("object_id", "=", id)])) == 0
        )
        i += 1


def test_generator_dist_chain(ray_start_cluster):
    """E2E test to verify chain of generator works properly."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=1 * 1024 * 1024 * 1024)
    ray.init()
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    @ray.remote
    class ChainActor:
        def __init__(self, child=None):
            self.child = child

        def get_data(self):
            if not self.child:
                for _ in range(10):
                    time.sleep(0.1)
                    yield np.ones(5 * 1024 * 1024)
            else:
                for data in self.child.get_data.remote():
                    yield ray.get(data)

    chain_actor = ChainActor.remote()
    chain_actor_2 = ChainActor.remote(chain_actor)
    chain_actor_3 = ChainActor.remote(chain_actor_2)
    chain_actor_4 = ChainActor.remote(chain_actor_3)

    for ref in chain_actor_4.get_data.remote():
        assert np.array_equal(np.ones(5 * 1024 * 1024), ray.get(ref))
        print("getting the next data")
        del ref


def test_generator_slow_pinning_requests(monkeypatch, shutdown_only):
    """
    Verify when the Object pinning request from the raylet
    is reported slowly, there's no refernece leak.
    """
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_asio_delay_us",
            "CoreWorkerService.grpc_server.PubsubLongPolling=1000000:1000000",
        )

        @ray.remote
        def f():
            yield np.ones(5 * 1024 * 1024)

        for ref in f.remote():
            del ref

        print(list_objects())


@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_actor_streaming_generator(shutdown_only, store_in_plasma):
    """Test actor/async actor with sync/async generator interfaces."""
    ray.init()

    @ray.remote
    class Actor:
        def f(self, ref):
            for i in range(3):
                yield i

        async def async_f(self, ref):
            for i in range(3):
                await asyncio.sleep(0.1)
                yield i

        def g(self):
            return 3

    a = Actor.remote()
    if store_in_plasma:
        arr = np.random.rand(5 * 1024 * 1024)
    else:
        arr = 3

    def verify_sync_task_executor():
        generator = a.f.remote(ray.put(arr))
        # Verify it works with next.
        assert isinstance(generator, ObjectRefGenerator)
        assert ray.get(next(generator)) == 0
        assert ray.get(next(generator)) == 1
        assert ray.get(next(generator)) == 2
        with pytest.raises(StopIteration):
            ray.get(next(generator))

        # Verify it works with for.
        generator = a.f.remote(ray.put(3))
        for index, ref in enumerate(generator):
            assert index == ray.get(ref)

    def verify_async_task_executor():
        # Verify it works with next.
        generator = a.async_f.remote(ray.put(arr))
        assert isinstance(generator, ObjectRefGenerator)
        assert ray.get(next(generator)) == 0
        assert ray.get(next(generator)) == 1
        assert ray.get(next(generator)) == 2

        # Verify it works with for.
        generator = a.f.remote(ray.put(3))
        for index, ref in enumerate(generator):
            assert index == ray.get(ref)

    async def verify_sync_task_async_generator():
        # Verify anext
        async_generator = a.f.remote(ray.put(arr))
        assert isinstance(async_generator, ObjectRefGenerator)
        for expected in range(3):
            ref = await async_generator.__anext__()
            assert await ref == expected
        with pytest.raises(StopAsyncIteration):
            await async_generator.__anext__()

        # Verify async for.
        async_generator = a.f.remote(ray.put(arr))
        expected = 0
        async for ref in async_generator:
            value = await ref
            assert expected == value
            expected += 1

    async def verify_async_task_async_generator():
        async_generator = a.async_f.remote(ray.put(arr))
        assert isinstance(async_generator, ObjectRefGenerator)
        for expected in range(3):
            ref = await async_generator.__anext__()
            assert await ref == expected
        with pytest.raises(StopAsyncIteration):
            await async_generator.__anext__()

        # Verify async for.
        async_generator = a.async_f.remote(ray.put(arr))
        expected = 0
        async for ref in async_generator:
            value = await ref
            assert expected == value
            expected += 1

    verify_sync_task_executor()
    verify_async_task_executor()
    asyncio.run(verify_sync_task_async_generator())
    asyncio.run(verify_async_task_async_generator())


def test_streaming_generator_exception(shutdown_only):
    # Verify the exceptions are correctly raised.
    # Also verify the followup next will raise StopIteration.
    ray.init()

    @ray.remote
    class Actor:
        def f(self):
            raise ValueError
            yield 1  # noqa

        async def async_f(self):
            raise ValueError
            yield 1  # noqa

    a = Actor.remote()
    g = a.f.remote()
    with pytest.raises(ValueError):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))

    g = a.async_f.remote()
    with pytest.raises(ValueError):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
