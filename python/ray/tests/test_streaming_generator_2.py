import asyncio
import pytest
import numpy as np
import sys
import time
import gc
import random

import ray
from ray.experimental.state.api import list_actors
from ray._raylet import StreamingObjectRefGenerator

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


@pytest.mark.parametrize("delay", [True, False])
@pytest.mark.parametrize("actor_task", [True])
def test_reconstruction(monkeypatch, ray_start_cluster, delay, actor_task):
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

    @ray.remote(max_restarts=-1, max_task_retries=2)
    class Actor:
        def dynamic_generator(self, num_returns):
            for i in range(num_returns):
                yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote(num_returns="streaming", max_retries=2)
    def dynamic_generator(num_returns):
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def fetch(x):
        return x[0]

    # Test recovery of all dynamic objects through re-execution.
    if actor_task:
        actor = Actor.remote()
        gen = actor.dynamic_generator.options(num_returns="streaming").remote(10)
    else:
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
@pytest.mark.parametrize("actor_task", [True])
def test_reconstruction_retry_failed(ray_start_cluster, failure_type, actor_task):
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

    @ray.remote(max_restarts=-1, max_task_retries=-1)
    class Actor:
        def dynamic_generator(self, num_returns, signal_actor):
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

    if actor_task:
        actor = Actor.remote()
        gen = actor.dynamic_generator.options(num_returns="streaming").remote(10, signal)
    else:
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


def test_reconstruction_batch_inference_like_test(monkeypatch, ray_start_cluster):
    with monkeypatch.context() as m:
        # m.setenv(
        #     "RAY_testing_asio_delay_us",
        #     "CoreWorkerService.grpc_server." "ReportGeneratorItemReturns=10000:1000000",
        # )
        cluster = ray_start_cluster
        # Head node with no resources.
        cluster.add_node(
            num_cpus=0,
            resources={"head": 1},
            _system_config=RECONSTRUCTION_CONFIG,
            enable_object_reconstruction=True,
        )
        ray.init(address=cluster.address)

        @ray.remote(num_cpus=1, scheduling_strategy="SPREAD", max_retries=-1)
        def map_task():
            for i in range(20):
                time.sleep(0.1)
                yield np.ones(1_000_000, dtype=np.int8) * i

        @ray.remote(num_cpus=1, scheduling_strategy="SPREAD", max_restarts=-1, max_task_retries=-1)
        class MapActor:
            def run(self, item):
                time.sleep(1)
                return item

        @ray.remote(num_cpus=1)
        def t(item):
            time.sleep(3)
            return item

        @ray.remote(num_cpus=0)
        def driver(num_nodes):
            actors = [MapActor.remote() for _ in range(num_nodes)]
            ray.get([a.__ray_ready__.remote() for a in actors])

            active_tasks = [
                map_task.options(num_returns="streaming").remote()
                    for _ in range(num_nodes*10)]

            def handle_ready_streaming_ref(streaming_ref, actor_handle):
                refs = []
                while True:
                    try:
                        ref = streaming_ref._next_sync(0)
                        if ref.is_nil():
                            return refs
                    except StopIteration:
                        return refs
                    # Submit the object reference to the actor.
                    # refs.append(actor_handle.run.remote(ref))
                    refs.append(t.remote(ref))

            while active_tasks:
                print(len(active_tasks))
                ready, _ = ray.wait(
                    active_tasks,
                    num_returns=len(active_tasks),
                    fetch_local=False, timeout=0.1)

                # Handle ready tasks.
                for streaming_ref in ready:
                    # Pick a random actor to process the outputs for this streaming_ref.
                    random_actor = actors[random.randint(0, len(actors) - 1)]
                    if isinstance(streaming_ref, StreamingObjectRefGenerator):
                        active_tasks.extend(handle_ready_streaming_ref(streaming_ref, random_actor))
                    # Remove the task from active_tasks if stop iteration has been reached.
                    active_tasks.remove(streaming_ref)

        num_nodes = 5
        ref = driver.remote(num_nodes)

        nodes = []
        for _ in range(num_nodes):
            nodes.append(cluster.add_node(num_cpus=16, object_store_memory=10**9))
        cluster.wait_for_nodes()

        for _ in range(100):
            r, _ = ray.wait([ref], timeout=0.1)

            if len(r) > 0:
                break

            time.sleep(5)
            node_to_kill = random.choices(nodes)[0]
            nodes.remove(node_to_kill)
            cluster.remove_node(node_to_kill, allow_graceful=False)
            nodes.append(cluster.add_node(num_cpus=16, object_store_memory=10**9))

        ray.get(ref)
        del ref
        del r

        assert_no_leak()


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


def test_python_object_leak(shutdown_only):
    """Make sure the objects are not leaked
    (due to circular references) when tasks run
    for all the execution model in Ray actors.
    """
    ray.init()

    @ray.remote
    class AsyncActor:
        def __init__(self):
            self.gc_garbage_len = 0

        def get_gc_garbage_len(self):
            return self.gc_garbage_len

        async def gen(self, fail=False):
            gc.set_debug(gc.DEBUG_SAVEALL)
            gc.collect()
            self.gc_garbage_len = len(gc.garbage)
            print("Objects: ", self.gc_garbage_len)
            if fail:
                print("exception")
                raise Exception
            yield 1

        async def f(self, fail=False):
            gc.set_debug(gc.DEBUG_SAVEALL)
            gc.collect()
            self.gc_garbage_len = len(gc.garbage)
            print("Objects: ", self.gc_garbage_len)
            if fail:
                print("exception")
                raise Exception
            return 1

    @ray.remote
    class A:
        def __init__(self):
            self.gc_garbage_len = 0

        def get_gc_garbage_len(self):
            return self.gc_garbage_len

        def f(self, fail=False):
            gc.set_debug(gc.DEBUG_SAVEALL)
            gc.collect()
            self.gc_garbage_len = len(gc.garbage)
            print("Objects: ", self.gc_garbage_len)
            if fail:
                print("exception")
                raise Exception
            return 1

        def gen(self, fail=False):
            gc.set_debug(gc.DEBUG_SAVEALL)
            gc.collect()
            self.gc_garbage_len = len(gc.garbage)
            print("Objects: ", self.gc_garbage_len)
            if fail:
                print("exception")
                raise Exception
            yield 1

    def verify_regular(actor, fail):
        for _ in range(100):
            try:
                ray.get(actor.f.remote(fail=fail))
            except Exception:
                pass
        assert ray.get(actor.get_gc_garbage_len.remote()) == 0

    def verify_generator(actor, fail):
        for _ in range(100):
            for ref in actor.gen.options(num_returns="streaming").remote(fail=fail):
                try:
                    ray.get(ref)
                except Exception:
                    pass
            assert ray.get(actor.get_gc_garbage_len.remote()) == 0

    print("Test regular actors")
    verify_regular(A.remote(), True)
    verify_regular(A.remote(), False)
    print("Test regular actors + generator")
    verify_generator(A.remote(), True)
    verify_generator(A.remote(), False)

    # Test threaded actor
    print("Test threaded actors")
    verify_regular(A.options(max_concurrency=10).remote(), True)
    verify_regular(A.options(max_concurrency=10).remote(), False)
    print("Test threaded actors + generator")
    verify_generator(A.options(max_concurrency=10).remote(), True)
    verify_generator(A.options(max_concurrency=10).remote(), False)

    # Test async actor
    print("Test async actors")
    verify_regular(AsyncActor.remote(), True)
    verify_regular(AsyncActor.remote(), False)
    print("Test async actors + generator")
    verify_generator(AsyncActor.remote(), True)
    verify_generator(AsyncActor.remote(), False)
    assert len(list_actors()) == 12


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
