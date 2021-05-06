import ray
import tensorflow as tf

from ray.job_config import JobConfig

@ray.remote
def get_conda_env():
    return tf.__version__

runtime_env = {"conda": "ray-py36-tf220"}
ray.init(job_config=JobConfig(runtime_env=runtime_env))

print(ray.get(get_conda_env.remote()))
print("should be 2.2.0")