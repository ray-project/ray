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

    @workflow.step
    def step_run():
        with FileLock(lock_path):
            return None

    @ray.remote(num_cpus=1)
    def remote_run():
        return None

    lock = FileLock(lock_path)
    lock.acquire()
    ret = step_run.options(num_cpus=2).step().run_async()
    obj = remote_run.remote()
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get(obj, timeout=2)
    lock.release()
    assert ray.get(ret) is None
    assert ray.get(obj) is None


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
