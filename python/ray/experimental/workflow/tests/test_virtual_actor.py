import time
from filelock import FileLock

import pytest
import ray

from ray.tests.conftest import *  # noqa
from ray.experimental import workflow
from ray.experimental.workflow import virtual_actor_class


@workflow.virtual_actor
class Counter:
    def __init__(self, x: int):
        self.x = x

    @workflow.virtual_actor.readonly
    def readonly_get(self):
        return self.x

    @workflow.virtual_actor.readonly
    def readonly_incr(self):
        self.x += 1
        return self.x

    @workflow.virtual_actor.readonly
    def readonly_workload(self):
        # simulate a workload
        time.sleep(1)

    def __getstate__(self):
        return self.x

    def __setstate__(self, state):
        self.x = state


@workflow.step
def init_virtual_actor(x):
    return x


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4  # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_readonly_actor(workflow_start_regular):
    actor = Counter.get_or_create("Counter", 42)
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 42
    assert actor.readonly_incr.run() == 43
    assert actor.readonly_get.run() == 42

    # test get actor
    readonly_actor = workflow.get_actor("Counter")
    # test concurrency
    assert ray.get([
        readonly_actor.readonly_get.run_async() for _ in range(10)
    ]) == [42] * 10
    assert ray.get([
        readonly_actor.readonly_incr.run_async() for _ in range(10)
    ]) == [43] * 10
    assert ray.get([
        readonly_actor.readonly_get.run_async() for _ in range(10)
    ]) == [42] * 10
    start = time.time()
    ray.get([readonly_actor.readonly_workload.run_async() for _ in range(10)])
    end = time.time()
    assert end - start < 5


@workflow.virtual_actor
class SlowInit:
    def __init__(self, x: int):
        time.sleep(5)
        self.x = x

    @workflow.virtual_actor.readonly
    def readonly_get(self):
        return self.x

    def __getstate__(self):
        return self.x

    def __setstate__(self, state):
        self.x = state


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4  # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_actor_ready(workflow_start_regular):
    actor = SlowInit.get_or_create("SlowInit", 42)
    with pytest.raises(virtual_actor_class.VirtualActorNotInitializedError):
        actor.readonly_get.run()
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 42


@pytest.mark.parametrize(
    "workflow_start_regular", [{
        "num_cpus": 4
    }],
    indirect=True)
def test_manager(workflow_start_regular, tmp_path):
    succ_flag = tmp_path / "succ_flag"
    lock_file = tmp_path / "lock"
    @workflow.virtual_actor
    class LockCounter:
        def __init__(self, l, succ):
            self.counter = 0
            self.succ = succ
            self.l = l

        @workflow.virtual_actor.readonly
        def val(self):
            with FileLock(lock_file):
                print("!!!")
                if not succ.exists():
                    raise ValueError()
                return self.counter

        def __getstate__(self):
            return (self.l, self.counter)

        def __setstate__(self, state):
            self.l, self.counter = state

    actor = LockCounter.get_or_create("counter", str(lock_file), succ_flag)
    ray.get(actor.ready())

    lock = FileLock(lock_file)
    lock.acquire()

    assert { 'counter': workflow.FINISHED } == workflow.list_all()

    r = actor.val.run_async()
    # Readonly function won't make the workflow running
    assert { 'counter': workflow.FINISHED } == workflow.list_all()
