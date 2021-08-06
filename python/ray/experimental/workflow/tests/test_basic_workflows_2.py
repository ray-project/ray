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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
