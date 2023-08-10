import asyncio
import pytest
import numpy as np
import sys
import time
import threading
import gc
import random

from collections import Counter

from unittest.mock import patch, Mock

import ray
from ray._private.test_utils import wait_for_condition
from ray.experimental.state.api import list_objects
from ray._raylet import StreamingObjectRefGenerator, ObjectRefStreamEndOfStreamError
from ray.cloudpickle import dumps
from ray.exceptions import WorkerCrashedError

RECONSTRUCTION_CONFIG = {
    "health_check_failure_threshold": 10,
    "health_check_period_ms": 100,
    "health_check_timeout_ms": 100,
    "health_check_initial_delay_ms": 0,
    "max_direct_call_object_size": 100,
    "task_retry_delay_ms": 100,
    "object_timeout_milliseconds": 200,
    "fetch_warn_timeout_milliseconds": 1000,
}


def assert_no_leak():
    gc.collect()
    core_worker = ray._private.worker.global_worker.core_worker
    ref_counts = core_worker.get_all_reference_counts()
    print(ref_counts)
    for rc in ref_counts.values():
        assert rc["local"] == 0
        assert rc["submitted"] == 0
    assert core_worker.get_memory_store_size() == 0


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
            generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)

            # Test when there's no new ref, it returns a nil.
            new_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = new_ref
            mocked_ray_wait.return_value = [], [new_ref]
            ref = generator._next_sync(timeout_s=0)
            assert ref.is_nil()

            # When the new ref is available, next should return it.
            for _ in range(3):
                new_ref = ray.ObjectRef.from_random()
                c.peek_object_ref_stream.return_value = new_ref
                mocked_ray_wait.return_value = [new_ref], []
                c.try_read_next_object_ref_stream.return_value = new_ref
                ref = generator._next_sync(timeout_s=0)
                assert new_ref == ref

            # When try_read_next_object_ref_stream raises a
            # ObjectRefStreamEndOfStreamError, it should raise a stop iteration.
            new_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = new_ref
            mocked_ray_wait.return_value = [new_ref], []
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
            generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)

            # Simulate the worker failure happens.
            next_ref = ray.ObjectRef.from_random()
            c.peek_object_ref_stream.return_value = next_ref
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

    gen = f.options(num_returns="streaming").remote()
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

    gen = f.options(num_returns="streaming").remote()
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
    gen = a.f.options(num_returns="streaming").remote()
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
    gen = f.options(num_returns="streaming").remote(a)
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

    gen = f.options(num_returns="streaming").remote()
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

    for ref in f.options(num_returns="streaming").remote():
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
                gen = f.options(num_returns="streaming").remote()
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
    gen = remote_generator_fn.options(num_returns="streaming").remote(
        3, store_in_plasma
    )
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
                for data in self.child.get_data.options(
                    num_returns="streaming"
                ).remote():
                    yield ray.get(data)

    chain_actor = ChainActor.remote()
    chain_actor_2 = ChainActor.remote(chain_actor)
    chain_actor_3 = ChainActor.remote(chain_actor_2)
    chain_actor_4 = ChainActor.remote(chain_actor_3)

    for ref in chain_actor_4.get_data.options(num_returns="streaming").remote():
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

        for ref in f.options(num_returns="streaming").remote():
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
        generator = a.f.options(num_returns="streaming").remote(ray.put(arr))
        # Verify it works with next.
        assert isinstance(generator, StreamingObjectRefGenerator)
        assert ray.get(next(generator)) == 0
        assert ray.get(next(generator)) == 1
        assert ray.get(next(generator)) == 2
        with pytest.raises(StopIteration):
            ray.get(next(generator))

        # Verify it works with for.
        generator = a.f.options(num_returns="streaming").remote(ray.put(3))
        for index, ref in enumerate(generator):
            assert index == ray.get(ref)

    def verify_async_task_executor():
        # Verify it works with next.
        generator = a.async_f.options(num_returns="streaming").remote(ray.put(arr))
        assert isinstance(generator, StreamingObjectRefGenerator)
        assert ray.get(next(generator)) == 0
        assert ray.get(next(generator)) == 1
        assert ray.get(next(generator)) == 2

        # Verify it works with for.
        generator = a.f.options(num_returns="streaming").remote(ray.put(3))
        for index, ref in enumerate(generator):
            assert index == ray.get(ref)

    async def verify_sync_task_async_generator():
        # Verify anext
        async_generator = a.f.options(num_returns="streaming").remote(ray.put(arr))
        assert isinstance(async_generator, StreamingObjectRefGenerator)
        for expected in range(3):
            ref = await async_generator.__anext__()
            assert await ref == expected
        with pytest.raises(StopAsyncIteration):
            await async_generator.__anext__()

        # Verify async for.
        async_generator = a.f.options(num_returns="streaming").remote(ray.put(arr))
        expected = 0
        async for ref in async_generator:
            value = await ref
            assert expected == value
            expected += 1

    async def verify_async_task_async_generator():
        async_generator = a.async_f.options(num_returns="streaming").remote(
            ray.put(arr)
        )
        assert isinstance(async_generator, StreamingObjectRefGenerator)
        for expected in range(3):
            ref = await async_generator.__anext__()
            assert await ref == expected
        with pytest.raises(StopAsyncIteration):
            await async_generator.__anext__()

        # Verify async for.
        async_generator = a.async_f.options(num_returns="streaming").remote(
            ray.put(arr)
        )
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
    g = a.f.options(num_returns="streaming").remote()
    with pytest.raises(ValueError):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))

    g = a.async_f.options(num_returns="streaming").remote()
    with pytest.raises(ValueError):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))

    with pytest.raises(StopIteration):
        ray.get(next(g))


def test_threaded_actor_generator(shutdown_only):
    ray.init()

    @ray.remote(max_concurrency=10)
    class Actor:
        def f(self):
            for i in range(30):
                time.sleep(0.1)
                yield np.ones(1024 * 1024) * i

    @ray.remote(max_concurrency=20)
    class AsyncActor:
        async def f(self):
            for i in range(30):
                await asyncio.sleep(0.1)
                yield np.ones(1024 * 1024) * i

    async def main():
        a = Actor.remote()
        asy = AsyncActor.remote()

        async def run():
            i = 0
            async for ref in a.f.options(num_returns="streaming").remote():
                print("before get")
                val = ray.get(ref, timeout=0.1)
                print("after get")
                print(val)
                print(ref)
                assert np.array_equal(val, np.ones(1024 * 1024) * i)
                i += 1
                del ref

        async def run2():
            i = 0
            async for ref in asy.f.options(num_returns="streaming").remote():
                val = await ref
                print(ref)
                print(val)
                assert np.array_equal(val, np.ones(1024 * 1024) * i), ref
                i += 1
                del ref

        coroutines = [run() for _ in range(10)]
        coroutines = [run2() for _ in range(20)]

        await asyncio.gather(*coroutines)

    asyncio.run(main())


def test_generator_dist_gather(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, object_store_memory=1 * 1024 * 1024 * 1024)
    ray.init()
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)
    cluster.add_node(num_cpus=1)

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self, child=None):
            self.child = child

        def get_data(self):
            for _ in range(10):
                time.sleep(0.1)
                yield np.ones(5 * 1024 * 1024)

    async def all_gather():
        actor = Actor.remote()
        async for ref in actor.get_data.options(num_returns="streaming").remote():
            val = await ref
            assert np.array_equal(np.ones(5 * 1024 * 1024), val)
            del ref

    async def main():
        await asyncio.gather(all_gather(), all_gather(), all_gather(), all_gather())

    asyncio.run(main())
    summary = ray._private.internal_api.memory_summary(stats_only=True)
    print(summary)


def test_generator_wait(shutdown_only):
    """
    Make sure the generator works with ray.wait.
    """
    ray.init(num_cpus=8)

    @ray.remote
    def f(sleep_time):
        for i in range(2):
            time.sleep(sleep_time)
            yield i

    @ray.remote
    def g(sleep_time):
        time.sleep(sleep_time)
        return 10

    gen = f.options(num_returns="streaming").remote(1)

    """
    Test basic cases.
    """
    for expected_rval in [0, 1]:
        s = time.time()
        r, ur = ray.wait([gen], num_returns=1)
        print(time.time() - s)
        assert len(r) == 1
        assert ray.get(next(r[0])) == expected_rval
        assert len(ur) == 0

    # Should raise a stop iteration.
    for _ in range(3):
        s = time.time()
        r, ur = ray.wait([gen], num_returns=1)
        print(time.time() - s)
        assert len(r) == 1
        with pytest.raises(StopIteration):
            assert next(r[0]) == 0
        assert len(ur) == 0

    gen = f.options(num_returns="streaming").remote(0)
    # Wait until the generator task finishes
    ray.get(gen._generator_ref)
    for i in range(2):
        r, ur = ray.wait([gen], timeout=0)
        assert len(r) == 1
        assert len(ur) == 0
        assert ray.get(next(r[0])) == i

    """
    Test the case ref is mixed with regular object ref.
    """
    gen = f.options(num_returns="streaming").remote(0)
    ref = g.remote(3)
    ready, unready = [], [gen, ref]
    result_set = set()
    while unready:
        ready, unready = ray.wait(unready)
        print(ready, unready)
        assert len(ready) == 1
        for r in ready:
            if isinstance(r, StreamingObjectRefGenerator):
                try:
                    ref = next(r)
                    print(ref)
                    print(ray.get(ref))
                    result_set.add(ray.get(ref))
                except StopIteration:
                    pass
                else:
                    unready.append(r)
            else:
                result_set.add(ray.get(r))

    assert result_set == {0, 1, 10}

    """
    Test timeout.
    """
    gen = f.options(num_returns="streaming").remote(3)
    ref = g.remote(1)
    ready, unready = ray.wait([gen, ref], timeout=2)
    assert len(ready) == 1
    assert len(unready) == 1

    """
    Test num_returns
    """
    gen = f.options(num_returns="streaming").remote(1)
    ref = g.remote(1)
    ready, unready = ray.wait([ref, gen], num_returns=2)
    assert len(ready) == 2
    assert len(unready) == 0


def test_generator_wait_e2e(shutdown_only):
    ray.init(num_cpus=8)

    @ray.remote
    def f(sleep_time):
        for i in range(2):
            time.sleep(sleep_time)
            yield i

    @ray.remote
    def g(sleep_time):
        time.sleep(sleep_time)
        return 10

    gen = [f.options(num_returns="streaming").remote(1) for _ in range(4)]
    ref = [g.remote(2) for _ in range(4)]
    ready, unready = [], [*gen, *ref]
    result = []
    start = time.time()
    while unready:
        ready, unready = ray.wait(unready, num_returns=len(unready), timeout=0.1)
        for r in ready:
            if isinstance(r, StreamingObjectRefGenerator):
                try:
                    ref = next(r)
                    result.append(ray.get(ref))
                except StopIteration:
                    pass
                else:
                    unready.append(r)
            else:
                result.append(ray.get(r))
    elapsed = time.time() - start
    assert elapsed < 3
    assert 2 < elapsed

    assert len(result) == 12
    result = Counter(result)
    assert result[0] == 4
    assert result[1] == 4
    assert result[10] == 4


@pytest.mark.parametrize("delay", [True])
def test_reconstruction(monkeypatch, ray_start_cluster, delay):
    with monkeypatch.context() as m:
        if delay:
            m.setenv(
                "RAY_testing_asio_delay_us",
                "CoreWorkerService.grpc_server."
                "ReportGeneratorItemReturns=10000:1000000",
            )
        cluster = ray_start_cluster
        # Head node with no resources.
        cluster.add_node(
            num_cpus=0,
            _system_config=RECONSTRUCTION_CONFIG,
            enable_object_reconstruction=True,
        )
        ray.init(address=cluster.address)
        # Node to place the initial object.
        node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
        cluster.wait_for_nodes()

    @ray.remote(num_returns="streaming", max_retries=2)
    def dynamic_generator(num_returns):
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def fetch(x):
        return x[0]

    # Test recovery of all dynamic objects through re-execution.
    gen = ray.get(dynamic_generator.remote(10))
    refs = []

    for i in range(5):
        refs.append(next(gen))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    for i, ref in enumerate(refs):
        print("first trial.")
        print("fetching ", i)
        assert ray.get(fetch.remote(ref)) == i

    # Try second retry.
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    for i in range(4):
        refs.append(next(gen))

    for i, ref in enumerate(refs):
        print("second trial")
        print("fetching ", i)
        assert ray.get(fetch.remote(ref)) == i

    # third retry should fail.
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    for i in range(1):
        refs.append(next(gen))

    for i, ref in enumerate(refs):
        print("third trial")
        print("fetching ", i)
        with pytest.raises(ray.exceptions.RayTaskError) as e:
            ray.get(fetch.remote(ref))
        assert "the maximum number of task retries has been exceeded" in str(e.value)


@pytest.mark.parametrize("failure_type", ["exception", "crash"])
def test_reconstruction_retry_failed(ray_start_cluster, failure_type):
    """Test the streaming generator retry fails in the second retry."""
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=RECONSTRUCTION_CONFIG,
        enable_object_reconstruction=True,
    )
    ray.init(address=cluster.address)

    @ray.remote(num_cpus=0)
    class SignalActor:
        def __init__(self):
            self.crash = False

        def set(self):
            self.crash = True

        def get(self):
            return self.crash

    signal = SignalActor.remote()
    ray.get(signal.get.remote())

    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(num_returns="streaming")
    def dynamic_generator(num_returns, signal_actor):
        for i in range(num_returns):
            if i == 3:
                should_crash = ray.get(signal_actor.get.remote())
                if should_crash:
                    if failure_type == "exception":
                        raise Exception
                    else:
                        sys.exit(5)
            time.sleep(1)
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def fetch(x):
        return x[0]

    gen = ray.get(dynamic_generator.remote(10, signal))
    refs = []

    for i in range(5):
        refs.append(next(gen))

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    for i, ref in enumerate(refs):
        print("first trial.")
        print("fetching ", i)
        assert ray.get(fetch.remote(ref)) == i

    # Try second retry.
    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)

    signal.set.remote()

    for ref in gen:
        refs.append(ref)

    for i, ref in enumerate(refs):
        print("second trial")
        print("fetching ", i)
        print(ref)
        if i < 3:
            assert ray.get(fetch.remote(ref)) == i
        else:
            with pytest.raises(ray.exceptions.RayTaskError) as e:
                assert ray.get(fetch.remote(ref)) == i
                assert "The worker died" in str(e.value)


def test_ray_datasetlike_mini_stress_test(monkeypatch, ray_start_cluster):
    """
    Test a workload that's like ray dataset + lineage reconstruction.
    """
    with monkeypatch.context() as m:
        m.setenv(
            "RAY_testing_asio_delay_us",
            "CoreWorkerService.grpc_server." "ReportGeneratorItemReturns=10000:1000000",
        )
        cluster = ray_start_cluster
        # Head node with no resources.
        cluster.add_node(
            num_cpus=1,
            resources={"head": 1},
            _system_config=RECONSTRUCTION_CONFIG,
            enable_object_reconstruction=True,
        )
        ray.init(address=cluster.address)

        @ray.remote(num_returns="streaming", max_retries=-1)
        def dynamic_generator(num_returns):
            for i in range(num_returns):
                time.sleep(0.1)
                yield np.ones(1_000_000, dtype=np.int8) * i

        @ray.remote(num_cpus=0, resources={"head": 1})
        def driver():
            unready = [dynamic_generator.remote(10) for _ in range(5)]
            ready = []
            while unready:
                ready, unready = ray.wait(
                    unready, num_returns=len(unready), timeout=0.1
                )
                for r in ready:
                    try:
                        ref = next(r)
                        print(ref)
                        ray.get(ref)
                    except StopIteration:
                        pass
                    else:
                        unready.append(r)
            return None

        ref = driver.remote()

        nodes = []
        for _ in range(4):
            nodes.append(cluster.add_node(num_cpus=1, object_store_memory=10**8))
        cluster.wait_for_nodes()

        for _ in range(10):
            time.sleep(0.1)
            node_to_kill = random.choices(nodes)[0]
            nodes.remove(node_to_kill)
            cluster.remove_node(node_to_kill, allow_graceful=False)
            nodes.append(cluster.add_node(num_cpus=1, object_store_memory=10**8))

        ray.get(ref)
        del ref

        assert_no_leak()


def test_generator_max_returns(monkeypatch, shutdown_only):
    """
    Test when generator returns more than system limit values
    (100 million by default), it fails a task.
    """
    with monkeypatch.context() as m:
        # defer for 10s for the second node.
        m.setenv(
            "RAY_max_num_generator_returns",
            "2",
        )

        @ray.remote(num_returns="streaming")
        def generator_task():
            for _ in range(3):
                yield 1

        @ray.remote
        def driver():
            gen = generator_task.remote()
            for ref in gen:
                assert ray.get(ref) == 1

        with pytest.raises(ray.exceptions.RayTaskError):
            ray.get(driver.remote())


def test_return_yield_mix(shutdown_only):
    """
    Test the case where yield and return is mixed within a
    generator task.
    """

    @ray.remote
    def g():
        for i in range(3):
            yield i
            return

    generator = g.options(num_returns="streaming").remote()
    result = []
    for ref in generator:
        result.append(ray.get(ref))

    assert len(result) == 1
    assert result[0] == 0


def test_task_name_not_changed_for_iteration(shutdown_only):
    """Handles https://github.com/ray-project/ray/issues/37147.
    Verify the task_name is not changed for each iteration in
    async actor generator task.
    """

    @ray.remote
    class A:
        async def gen(self):
            task_name = asyncio.current_task().get_name()
            for i in range(5):
                assert (
                    task_name == asyncio.current_task().get_name()
                ), f"{task_name} != {asyncio.current_task().get_name()}"
                yield i

            assert task_name == asyncio.current_task().get_name()

    a = A.remote()
    for obj_ref in a.gen.options(num_returns="streaming").remote():
        print(ray.get(obj_ref))


def test_async_actor_concurrent(shutdown_only):
    """Verify the async actor generator tasks are concurrent."""

    @ray.remote
    class A:
        async def gen(self):
            for i in range(5):
                await asyncio.sleep(1)
                yield i

    a = A.remote()

    async def co():
        async for ref in a.gen.options(num_returns="streaming").remote():
            print(await ref)

    async def main():
        await asyncio.gather(co(), co(), co())

    s = time.time()
    asyncio.run(main())
    assert 4.5 < time.time() - s < 6.5


def test_no_memory_store_obj_leak(shutdown_only):
    """Fixes https://github.com/ray-project/ray/issues/38089

    Verify there's no leak from in-memory object store when
    using a streaming generator.
    """
    ray.init()

    @ray.remote
    def f():
        for _ in range(10):
            yield 1

    for _ in range(10):
        for ref in f.options(num_returns="streaming").remote():
            del ref

        time.sleep(0.2)

    core_worker = ray._private.worker.global_worker.core_worker
    assert core_worker.get_memory_store_size() == 0
    assert_no_leak()

    for _ in range(10):
        for ref in f.options(num_returns="streaming").remote():
            break

        time.sleep(0.2)

    del ref
    core_worker = ray._private.worker.global_worker.core_worker
    assert core_worker.get_memory_store_size() == 0
    assert_no_leak()


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
