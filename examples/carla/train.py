import ray
from ray.tune import register_env, run_experiments

from env import CarlaEnv

env_name = "carla_env"
register_env(env_name, lambda: CarlaEnv())

run_experiments({
    "carla": {
        "run": "PPO",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "num_workers": 1,
            "timesteps_per_batch": 100,
            "min_steps_per_task": 100,
            "sgd_batchsize": 16,
            "devices": ["/gpu:0"],
        },
    },
})
