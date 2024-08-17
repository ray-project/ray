import os

import ray

cuda_env = ray._private.ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR
if os.environ.get("RAY_TEST_RESOURCES_SPECIFIED") == "1":
    assert cuda_env not in os.environ
    assert "CUDA_VISIBLE_DEVICES" in os.environ
else:
    assert os.environ[cuda_env] == "1"


@ray.remote
def f():
    assert cuda_env not in os.environ


# Will raise if task fails.
ray.get(f.remote())
