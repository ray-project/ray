import asyncio
import gc
import sys
import time

import numpy as np
import pytest

import ray
from ray._common.test_utils import (
    SignalActor,
    wait_for_condition,
)
from ray.experimental.state.api import list_actors

RECONSTRUCTION_CONFIG = {
    "health_check_failure_threshold": 10,
    "health_check_period_ms": 100,
    "health_check_timeout_ms": 100,
    "health_check_initial_delay_ms": 0,
    "max_direct_call_object_size": 100,
    "task_retry_delay_ms": 100,
    "object_timeout_milliseconds": 200,
    "fetch_warn_timeout_milliseconds": 1000,
    # Required for reducing the retry time of RequestWorkerLease
    "raylet_rpc_server_reconnect_timeout_s": 0,
}


def assert_no_leak(filter_refs=None):
    if filter_refs is None:
        filter_refs = []
    filter_refs = [ref.hex().encode("utf-8") for ref in filter_refs]

    def check():
        gc.collect()
        core_worker = ray._private.worker.global_worker.core_worker
        ref_counts = core_worker.get_all_reference_counts()

        num_in_memory_objects = core_worker.get_memory_store_size()
        for k, rc in ref_counts.items():
            if k in filter_refs:
                num_in_memory_objects -= 1
                continue
            if rc["local"] != 0:
                return False
            if rc["submitted"] != 0:
                return False
        return num_in_memory_objects <= 0

    wait_for_condition(check)


@pytest.mark.skip(
    reason="This test is flaky on darwin as of https://github.com/ray-project/ray/pull/53999."
    "See https://github.com/ray-project/ray/pull/54320 for context on when to stop skipping."
)
def test_reconstruction(ray_start_cluster):
    cluster = ray_start_cluster
    # Head node with no resources.
    cluster.add_node(
        num_cpus=0,
        _system_config=RECONSTRUCTION_CONFIG,
    )
    ray.init(address=cluster.address)
    # Node to place the initial object.
    node_to_kill = cluster.add_node(num_cpus=1, object_store_memory=10**8)
    cluster.wait_for_nodes()

    @ray.remote(max_retries=2)
    def generator(num_returns):
        for i in range(num_returns):
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def fetch(x):
        return x[0]

    # Test recovery of all dynamic objects through re-execution.
    gen = generator.remote(10)
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

    @ray.remote
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

    gen = dynamic_generator.remote(10, signal)
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

        @ray.remote
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

    generator = g.remote()
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
    for obj_ref in a.gen.remote():
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
        async for ref in a.gen.remote():
            print(await ref)

    async def main():
        await asyncio.gather(co(), co(), co())

    s = time.time()
    asyncio.run(main())
    assert 4.5 < time.time() - s < 6.5


def test_no_memory_store_obj_leak(ray_start_regular):
    """Fixes https://github.com/ray-project/ray/issues/38089

    Verify there's no leak from in-memory object store when
    using a streaming generator.
    """

    @ray.remote
    def f(signal=None):
        for _ in range(10):
            yield 1
        if signal is not None:
            signal.send.remote()

    for _ in range(2):
        gen = f.remote()
        for _ in range(10):
            for ref in gen:
                del ref

        del gen
        assert_no_leak()

    signal = SignalActor.remote()
    gen = f.remote(signal)
    ray.get(signal.wait.remote())
    del gen
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
            # Clear any existing circular references
            # before testing leaks in actor tasks.
            gc.collect()
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
            # Clear any existing circular references
            # before testing leaks in actor tasks.
            gc.collect()
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
            for ref in actor.gen.remote(fail=fail):
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


@pytest.mark.parametrize("delay", [True, False])
@pytest.mark.parametrize("actor_task", [True, False])
def test_reconstruction_generator_out_of_scope(
    monkeypatch, ray_start_cluster, delay, actor_task
):
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
        node_to_kill = cluster.add_node(
            num_cpus=1, num_gpus=1, object_store_memory=10**8
        )
        cluster.wait_for_nodes()

    @ray.remote(num_cpus=0, num_gpus=1, max_restarts=-1, max_task_retries=2)
    class Actor:
        def dynamic_generator(self, num_returns):
            for i in range(num_returns):
                print("yield", i)
                yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote(num_returns="streaming", max_retries=2)
    def dynamic_generator(num_returns):
        for i in range(num_returns):
            print("yield", i)
            yield np.ones(1_000_000, dtype=np.int8) * i

    @ray.remote
    def dependent_task(x):
        return x

    @ray.remote
    def fetch(x):
        return x[0]

    # Test recovery of all dynamic objects through re-execution.
    if actor_task:
        actor = Actor.remote()
        gen = actor.dynamic_generator.options(num_returns="streaming").remote(2)
    else:
        gen = ray.get(dynamic_generator.remote(2))
    refs = []

    for ref in gen:
        ref = dependent_task.remote(ref)
        refs.append(ref)
    del gen

    for i, ref in enumerate(refs):
        assert ray.get(fetch.remote(ref)) == i

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, num_gpus=1, object_store_memory=10**8)

    for i, ref in enumerate(refs):
        assert ray.get(fetch.remote(ref)) == i
    refs = []
    del ref

    assert_no_leak()

    # Test that when the generator task stays in the in-scope lineage, we still
    # clean up the unconsumed objects' values. The lineage (task and stream
    # metadata) gets cleaned up later, once all of the references are out of
    # scope.
    if actor_task:
        actor = Actor.remote()
        gen = actor.dynamic_generator.options(num_returns="streaming").remote(2)
    else:
        gen = ray.get(dynamic_generator.remote(2))

    ref = dependent_task.remote(next(gen))
    del gen

    assert ray.get(fetch.remote(ref)) == 0

    cluster.remove_node(node_to_kill, allow_graceful=False)
    node_to_kill = cluster.add_node(num_cpus=1, num_gpus=1, object_store_memory=10**8)

    assert ray.get(fetch.remote(ref)) == 0
    assert_no_leak(filter_refs=[ref])

    del ref
    assert_no_leak()


if __name__ == "__main__":

    sys.exit(pytest.main(["-sv", __file__]))
