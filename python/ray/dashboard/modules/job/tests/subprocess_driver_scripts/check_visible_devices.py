import os

import ray

noset_env = ray._private.accelerators.nvidia_gpu.NOSET_CUDA_VISIBLE_DEVICES_ENV_VAR
visible_devices_env = ray._private.accelerators.get_accelerator_manager_for_resource(
    "GPU"
).get_visible_accelerator_ids_env_var()
if os.environ.get("RAY_TEST_RESOURCES_SPECIFIED") == "1":
    assert noset_env not in os.environ
    if os.environ.get("RAY_TEST_GPUS_SPECIFIED") == "1":
        assert visible_devices_env in os.environ
    elif visible_devices_env is not None:
        assert visible_devices_env not in os.environ
else:
    assert os.environ[noset_env] == "1"


@ray.remote
def f():
    assert noset_env not in os.environ


# Will raise if task fails.
ray.get(f.remote())
