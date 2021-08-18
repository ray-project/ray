import pytest
import ray
from filelock import FileLock
from ray.test_utils import run_string_as_driver
from ray.experimental import workflow
from ray.tests.conftest import *  # noqa


def test_init_twice(call_ray_start, reset_workflow, tmp_path):
    workflow.init()
    with pytest.raises(RuntimeError):
        workflow.init(str(tmp_path))


driver_script = """
from ray.experimental import workflow

if __name__ == "__main__":
    workflow.init()
"""


def test_init_twice_2(call_ray_start, reset_workflow, tmp_path):
    run_string_as_driver(driver_script)
    with pytest.raises(RuntimeError):
        workflow.init(str(tmp_path))


@pytest.mark.parametrize(
    "workflow_start_regular", [{
        "num_cpus": 2,
    }], indirect=True)
def test_step_resources(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    # We use signal actor here because we can't guarantee the order of tasks
    # sent from worker to raylet.
    signal_actor = ray.test_utils.SignalActor.remote()

    @workflow.step
    def step_run():
        ray.wait([signal_actor.send.remote()])
        with FileLock(lock_path):
            return None

    @ray.remote(num_cpus=1)
    def remote_run():
        return None

    lock = FileLock(lock_path)
    lock.acquire()
    ret = step_run.options(num_cpus=2).step().run_async()
    ray.wait([signal_actor.wait.remote()])
    obj = remote_run.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj, timeout=2)
    lock.release()
    assert ray.get(ret) is None
    assert ray.get(obj) is None


def test_get_output_1(workflow_start_regular, tmp_path):
    @workflow.step
    def simple(v):
        return v

    assert 0 == simple.step(0).run("simple")
    assert 0 == ray.get(workflow.get_output("simple"))


def test_get_output_2(workflow_start_regular, tmp_path):
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)

    @workflow.step
    def simple(v):
        with FileLock(lock_path):
            return v

    lock.acquire()
    obj = simple.step(0).run_async("simple")
    obj2 = workflow.get_output("simple")
    lock.release()
    assert ray.get([obj, obj2]) == [0, 0]


def test_get_named_step_output(workflow_start_regular, tmp_path):
    @workflow.step
    def double(v, lock=None):
        if lock is not None:
            with FileLock(lock_path):
                return 2 * v
        else:
            return 2 * v

    # Get the result from named step after workflow finished
    assert 4 == double.options(step_name="outer").step(
        double.options(step_name="inner").step(1)).run("double")
    assert ray.get(workflow.get_output("double", "inner")) == 2
    assert ray.get(workflow.get_output("double", "outer")) == 4

    # Get the result from named step after workflow before it's finished
    lock_path = str(tmp_path / "lock")
    lock = FileLock(lock_path)
    lock.acquire()
    output = double.options(step_name="outer").step(
        double.options(step_name="inner").step(1, lock_path),
        lock_path).run_async("double-2")

    inner = workflow.get_output("double-2", "inner")
    outer = workflow.get_output("double-2", "outer")

    @ray.remote
    def wait(obj_ref):
        return ray.get(obj_ref[0])

    ready, waiting = ray.wait(
        [wait.remote([output]),
         wait.remote([inner]),
         wait.remote([outer])],
        timeout=1)
    assert 0 == len(ready)
    assert 3 == len(waiting)

    lock.release()
    assert 4 == ray.get(output)
    assert 2 == ray.get(inner)
    assert 4 == ray.get(outer)

    inner = workflow.get_output("double-2", "inner")
    outer = workflow.get_output("double-2", "outer")
    assert 2 == ray.get(inner)
    assert 4 == ray.get(outer)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
