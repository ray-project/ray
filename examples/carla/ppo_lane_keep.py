from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
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

ray.init()
run_experiments({
    "carla-ppo": {
        "run": "PPO",
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
            "num_workers": 1,
            "timesteps_per_batch": 2000,
            "min_steps_per_task": 100,
            "lambda": 0.95,
            "clip_param": 0.2,
            "num_sgd_iter": 20,
            "sgd_stepsize": 0.0001,
            "sgd_batchsize": 32,
            "devices": ["/gpu:0"],
            "tf_session_args": {
              "gpu_options": {"allow_growth": True}
            }
        },
    },
})
