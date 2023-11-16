import sys

import pytest

from ray import serve
from ray._private.test_utils import wait_for_condition
from ray.serve.handle import DeploymentHandle


def check_application(app_handle: DeploymentHandle, expected: str):
    ref = app_handle.remote()
    assert ref.result() == expected
    return True


@pytest.mark.skipif(sys.platform != "linux", reason="Only works on Linux.")
def test_basic(ray_start_stop):
    @serve.deployment(
        ray_actor_options={
            "runtime_env": {
                "container": {
                    "image": "zcin/runtime-env-prototype:nested",
                    "worker_path": "/home/ray/anaconda3/lib/python3.9/site-packages/ray/_private/workers/default_worker.py",  # noqa
                }
            }
        }
    )
    class Model:
        def __call__(self):
            with open("file.txt") as f:
                return f.read()

    h = serve.run(Model.bind())
    wait_for_condition(
        check_application,
        app_handle=h,
        expected="Hi I'm Cindy, this is version 1\n",
        timeout=300,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
