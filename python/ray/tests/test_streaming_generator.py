import pytest
import numpy as np
import sys
import time
import gc

from unittest.mock import patch, Mock

import ray
from ray._private.test_utils import wait_for_condition
from ray.experimental.state.api import list_objects
from ray._raylet import StreamingObjectRefGenerator, ObjectRefStreamEoFError
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


@pytest.fixture
def mocked_worker():
    mocked_core_worker = Mock()
    mocked_core_worker.try_read_next_object_ref_stream.return_value = None
    mocked_core_worker.delete_object_ref_stream.return_value = None
    mocked_core_worker.create_object_ref_stream.return_value = None
    worker = MockedWorker(mocked_core_worker)
    yield worker


def test_streaming_object_ref_generator_basic_unit(mocked_worker):
    """
    Verify the basic case:
    create a generator -> read values -> nothing more to read -> delete.
    """
    with patch("ray.wait") as mocked_ray_wait:
        c = mocked_worker.core_worker
        generator_ref = ray.ObjectRef.from_random()
        generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)
        c.try_read_next_object_ref_stream.return_value = ray.ObjectRef.nil()
        c.create_object_ref_stream.assert_called()

        # Test when there's no new ref, it returns a nil.
        mocked_ray_wait.return_value = [], [generator_ref]
        ref = generator._next(timeout_s=0)
        assert ref.is_nil()

        # When the new ref is available, next should return it.
        for _ in range(3):
            new_ref = ray.ObjectRef.from_random()
            c.try_read_next_object_ref_stream.return_value = new_ref
            ref = generator._next(timeout_s=0)
            assert new_ref == ref

        # When try_read_next_object_ref_stream raises a
        # ObjectRefStreamEoFError, it should raise a stop iteration.
        c.try_read_next_object_ref_stream.side_effect = ObjectRefStreamEoFError(
            ""
        )  # noqa
        with pytest.raises(StopIteration):
            ref = generator._next(timeout_s=0)

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
            mocked_ray_wait.return_value = [generator_ref], []
            mocked_ray_get.side_effect = WorkerCrashedError()

            c.try_read_next_object_ref_stream.return_value = ray.ObjectRef.nil()
            ref = generator._next(timeout_s=0)
            # If the generator task fails by a systsem error,
            # meaning the ref will raise an exception
            # it should be returned.
            print(ref)
            print(generator_ref)
            assert ref == generator_ref

            # Once exception is raised, it should always
            # raise stopIteration regardless of what
            # the ref contains now.
            with pytest.raises(StopIteration):
                ref = generator._next(timeout_s=0)


def test_streaming_object_ref_generator_network_failed_unit(mocked_worker):
    """
    Verify when a task is finished, but if the next ref is not available
    on time, it raises an assertion error.

    TODO(sang): Once we move the task subimssion path to use pubsub
    to guarantee the ordering, we don't need this test anymore.
    """
    with patch("ray.get") as mocked_ray_get:
        with patch("ray.wait") as mocked_ray_wait:
            c = mocked_worker.core_worker
            generator_ref = ray.ObjectRef.from_random()
            generator = StreamingObjectRefGenerator(generator_ref, mocked_worker)

            # Simulate the task has finished.
            mocked_ray_wait.return_value = [generator_ref], []
            mocked_ray_get.return_value = None

            # If StopIteration is not raised within
            # unexpected_network_failure_timeout_s second,
            # it should fail.
            c.try_read_next_object_ref_stream.return_value = ray.ObjectRef.nil()
            ref = generator._next(timeout_s=0, unexpected_network_failure_timeout_s=1)
            assert ref == ray.ObjectRef.nil()
            time.sleep(1)
            with pytest.raises(AssertionError):
                generator._next(timeout_s=0, unexpected_network_failure_timeout_s=1)
            # After that StopIteration should be raised.
            with pytest.raises(StopIteration):
                generator._next(timeout_s=0, unexpected_network_failure_timeout_s=1)


def test_generator_basic(shutdown_only):
    ray.init(num_cpus=1)

    """Basic cases"""

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

    @ray.remote
    def f():
        for i in range(5):
            if i == 2:
                raise ValueError
            yield i

    gen = f.options(num_returns="streaming").remote()
    ray.get(next(gen))
    ray.get(next(gen))
    with pytest.raises(ray.exceptions.RayTaskError) as e:
        ray.get(next(gen))
    print(str(e.value))
    with pytest.raises(StopIteration):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))

    """Generator Task failure"""

    @ray.remote
    class A:
        def getpid(self):
            import os

            return os.getpid()

        def f(self):
            for i in range(5):
                time.sleep(0.1)
                yield i

    a = A.remote()
    i = 0
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
    # TODO(sang): Enable it once retry is supported.
    # @ray.remote
    # class Actor:
    #     def __init__(self):
    #         self.should_kill = True

    #     def should_kill(self):
    #         return self.should_kill

    #     async def set(self, wait_s):
    #         await asyncio.sleep(wait_s)
    #         self.should_kill = False

    # @ray.remote(retry_exceptions=[ValueError], max_retries=10)
    # def f(a):
    #     for i in range(5):
    #         should_kill = ray.get(a.should_kill.remote())
    #         if i == 3 and should_kill:
    #             raise ValueError
    #         yield i

    # a = Actor.remote()
    # gen = f.options(num_returns="streaming").remote(a)
    # assert ray.get(next(gen)) == 0
    # assert ray.get(next(gen)) == 1
    # assert ray.get(next(gen)) == 2
    # a.set.remote(3)
    # assert ray.get(next(gen)) == 3
    # assert ray.get(next(gen)) == 4
    # with pytest.raises(StopIteration):
    #     ray.get(next(gen))

    """Cancel"""

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


@pytest.mark.parametrize("crash_type", ["exception", "worker_crash"])
def test_generator_streaming_no_leak_upon_failures(
    monkeypatch, shutdown_only, crash_type
):
    with monkeypatch.context() as m:
        # defer for 10s for the second node.
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
            lambda: len(list_objects(filters=[("object_id", "=", id)])) == 0
        )
        i += 1


def test_generator_dist_chain(ray_start_cluster):
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
        del ref


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
