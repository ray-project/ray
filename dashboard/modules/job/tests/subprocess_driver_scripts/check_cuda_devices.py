import os
import ray

cuda_env = ray.ray_constants.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR
assert os.environ[cuda_env] == "1"


@ray.remote
def f():
    assert cuda_env not in os.environ


# Will raise if task fails.
ray.get(f.remote())
