from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 80,
    "y_res": 80,
    "use_depth_camera": False,
    "discrete_actions": False,
    "max_steps": 200,
    "weather": [1],
})
register_env(env_name, lambda: CarlaEnv(env_config))

run_experiments({
    "carla": {
        "run": "A3C",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "gamma": 0.95,
            "num_batches_per_iteration": 20,
            "num_workers": 1,
            "model": {
              "grayscale": False,
              "zero_mean": True,
              "dim": 80,
            },
        },
    },
})
