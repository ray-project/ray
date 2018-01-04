from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_model
from scenarios import LANE_KEEP

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 80,
    "y_res": 80,
    "use_depth_camera": False,
    "discrete_actions": False,
    "server_map": "/Game/Maps/Town02",
    "reward_function": "lane_keep",
    "enable_planner": False,
    "scenarios": [LANE_KEEP],
})

register_env(env_name, lambda env_config: CarlaEnv(env_config))
register_carla_model()

run_experiments({
    "carla-a3c": {
        "run": "A3C",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "env_config": env_config,
            "model": {
                "custom_model": "carla",
                "custom_options": {
                    "image_shape": [80, 80, 6],
                },
                "conv_filters": [
                    [16, [8, 8], 4],
                    [32, [4, 4], 2],
                    [512, [10, 10], 1],
                ],
            },
            "gamma": 0.8,
            "num_workers": 1,
        },
    },
})
