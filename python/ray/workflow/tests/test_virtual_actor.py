import time

from filelock import FileLock
import pytest
import ray
from pathlib import Path

from ray.tests.conftest import *  # noqa
from ray import workflow
from ray.workflow import virtual_actor_class


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

    def add(self, y):
        self.x += y
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
    [
        {
            # We need more CPUs, otherwise 'create()' blocks 'get()', and task
            # execution suffers from worker capping.
            "num_cpus": 16
        }
    ],
    indirect=True,
)
def test_readonly_actor(workflow_start_regular):
    actor = Counter.get_or_create("Counter", 42)
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 42
    assert actor.readonly_incr.run() == 43
    assert actor.readonly_get.run() == 42

    # test get actor
    readonly_actor = workflow.get_actor("Counter")
    # test concurrency
    assert (
        ray.get([readonly_actor.readonly_get.run_async() for _ in range(10)])
        == [42] * 10
    )
    assert (
        ray.get([readonly_actor.readonly_incr.run_async() for _ in range(10)])
        == [43] * 10
    )
    assert (
        ray.get([readonly_actor.readonly_get.run_async() for _ in range(10)])
        == [42] * 10
    )
    start = time.time()
    ray.get([readonly_actor.readonly_workload.run_async() for _ in range(10)])
    end = time.time()
    assert end - start < 5, f"Took too long, {end-start}"


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
    [{"num_cpus": 4}],  # We need more CPUs, otherwise 'create()' blocks 'get()'
    indirect=True,
)
def test_actor_ready(workflow_start_regular):
    actor = SlowInit.get_or_create("SlowInit", 42)
    with pytest.raises(virtual_actor_class.VirtualActorNotInitializedError):
        actor.readonly_get.run()
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 42


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4
            # We need more CPUs, otherwise 'create()' blocks 'get()'
        }
    ],
    indirect=True,
)
def test_actor_writer_1(workflow_start_regular):
    actor = Counter.get_or_create("Counter", 0)
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 0
    array = []
    s = 0
    for i in range(1, 10):
        s += i
        array.append(s)
    assert [actor.add.run(i) for i in range(1, 10)] == array
    assert actor.readonly_get.run() == 45

    array = []
    for i in range(10, 20):
        s += i
        array.append(s)
    assert ray.get([actor.add.run_async(i) for i in range(10, 20)]) == array


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{"num_cpus": 4}],  # We need more CPUs, otherwise 'create()' blocks 'get()'
    indirect=True,
)
def test_actor_writer_2(workflow_start_regular, tmp_path):
    g_lock = str(Path(tmp_path / "g.lock"))
    incr_lock = str(Path(tmp_path / "incr.lock"))
    val_lock = str(Path(tmp_path / "val.lock"))

    val_err = str(Path(tmp_path / "val.err"))
    incr_err = str(Path(tmp_path / "incr.err"))

    @workflow.virtual_actor
    class SyncCounter:
        def __init__(
            self,
            val_lock: str,
            incr_lock: str,
            g_lock: str,
            val_err: str,
            incr_err: str,
        ):
            self.val_lock = val_lock
            self.incr_lock = incr_lock
            self.g_lock = g_lock

            self.val_err = val_err
            self.incr_err = incr_err
            self.v = 0
            if Path(self.val_err).exists():
                raise ValueError()

        @workflow.virtual_actor.readonly
        def val(self):
            with FileLock(self.val_lock), FileLock(self.g_lock):
                if Path(self.val_err).exists():
                    raise ValueError()
                return self.v

        def incr(self, create_incr_err=False):
            with FileLock(self.incr_lock), FileLock(self.g_lock):
                if Path(self.incr_err).exists():
                    raise ValueError()
                if create_incr_err:
                    Path(incr_err).touch()
                self.v += 1
                return self.v

        def __getstate__(self):
            return (
                self.v,
                self.val_lock,
                self.incr_lock,
                self.g_lock,
                self.val_err,
                self.incr_err,
            )

        def __setstate__(self, state):
            (
                self.v,
                self.val_lock,
                self.incr_lock,
                self.g_lock,
                self.val_err,
                self.incr_err,
            ) = state

    # trigger error in init
    Path(val_err).touch()
    actor = SyncCounter.get_or_create(
        "sync_counter", val_lock, incr_lock, g_lock, val_err, incr_err
    )
    with pytest.raises(Exception):
        actor.incr.run()
    Path(val_err).unlink()

    assert ray.get([actor.incr.run_async() for _ in range(9)]) == list(range(2, 11))
    incr_lock = FileLock(incr_lock)
    incr_lock.acquire()

    objs = [actor.incr.run_async() for _ in range(10)]
    assert 10 == actor.val.run()
    Path(val_err).touch()
    with pytest.raises(Exception):
        actor.val.run()
    Path(val_err).unlink()
    incr_lock.release()
    assert ray.get(objs) == list(range(11, 21))

    # test error cases
    actor.incr.run_async()  # 21
    actor.incr.run_async()  # 22
    actor.incr.run_async(create_incr_err=True)  # 23
    actor.incr.run_async()  # 24
    s5 = actor.incr.run_async()  # 25
    with pytest.raises(Exception):
        ray.get(s5)

    assert 23 == actor.val.run()
    Path(incr_err).unlink()
    obj = workflow.resume("sync_counter")
    assert 25 == ray.get(obj)[0]
    assert 25 == actor.val.run()


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 8,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_writer_actor_pressure_test(workflow_start_regular):
    actor = Counter.get_or_create("Counter", 0)
    array = []
    length = 50
    s = 0
    for i in range(1, length):
        s += i
        array.append(s)
    assert ray.get([actor.add.run_async(i) for i in range(1, length)]) == array


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 8,  # increase CPUs to add pressure
        }
    ],
    indirect=True,
)
def test_default_getset(workflow_start_regular):
    @workflow.virtual_actor
    class ActorWithoutSetGetState:
        def __init__(self):
            self.x = 10

        def set(self, x):
            self.x = x

    a1 = ActorWithoutSetGetState.get_or_create("a1")
    a1.set.run(10)

    with pytest.raises(ValueError):

        @workflow.virtual_actor
        class ActorWithSetStateOnly:
            def __init__(self):
                self.x = 10

            def set(self, x):
                self.x = x

            def __setstate__(self, x):
                self.x = x

    with pytest.raises(ValueError):

        @workflow.virtual_actor
        class ActorWithGetStateOnly:
            def __init__(self):
                self.x = 10

            def set(self, x):
                self.x = x

            def __getstate__(self):
                return self.x

    @workflow.virtual_actor
    class ActorHavingComplicatedStructure:
        def __init__(self):
            import numpy

            self.x = {"A": numpy.ones(10)}

        def set(self, x):
            self.x = x

    # TODO (yic) We need better error message here
    # https://github.com/ray-project/ray/issues/18147
    with pytest.raises(ray.exceptions.RaySystemError):
        a2 = ActorHavingComplicatedStructure.get_or_create("a2")
        a2.set.run(10)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
