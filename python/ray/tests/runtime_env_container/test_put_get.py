import ray
import numpy as np


@ray.remote(
    runtime_env={
        "container": {
            "image": "rayproject/ray:runtime_env_container_nested",
            "worker_path": "/home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py",  # noqa
        }
    }
)
def create_ref():
    with open("file.txt") as f:
        assert f.read().strip() == "helloworldalice"

    ref = ray.put(np.zeros(100_000_000))
    return ref


wrapped_ref = create_ref.remote()
ray.get(ray.get(wrapped_ref)) == np.zeros(100_000_000)
