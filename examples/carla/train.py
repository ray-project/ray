import ray
from ray.tune.registry import get_registry, register_env
from ray.rllib import ppo

from env import CarlaEnv

env_name = "carla_env"
register_env(env_name, lambda: CarlaEnv())

ray.init()
alg = ppo.PPOAgent(env=env_name, registry=get_registry(), config={"num_workers": 1, "timesteps_per_batch": 100, "min_steps_per_task": 100, "sgd_batchsize": 16, "devices": ["/gpu:0"]})

for i in range(10):
    alg.train()
