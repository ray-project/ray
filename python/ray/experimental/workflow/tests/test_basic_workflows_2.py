import pytest
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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
