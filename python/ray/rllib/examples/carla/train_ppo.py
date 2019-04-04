from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.tune import run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_model
from scenarios import TOWN2_STRAIGHT

env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 80,
    "y_res": 80,
    "use_depth_camera": False,
    "discrete_actions": False,
    "server_map": "/Game/Maps/Town02",
    "scenarios": TOWN2_STRAIGHT,
})
register_carla_model()

ray.init()
run_experiments({
    "carla": {
        "run": "PPO",
        "env": CarlaEnv,
        "config": {
            "env_config": env_config,
            "model": {
                "custom_model": "carla",
                "custom_options": {
                    "image_shape": [
                        env_config["x_res"], env_config["y_res"], 6
                    ],
                },
                "conv_filters": [
                    [16, [8, 8], 4],
                    [32, [4, 4], 2],
                    [512, [10, 10], 1],
                ],
            },
            "num_workers": 1,
            "train_batch_size": 2000,
            "sample_batch_size": 100,
            "lambda": 0.95,
            "clip_param": 0.2,
            "num_sgd_iter": 20,
            "lr": 0.0001,
            "sgd_minibatch_size": 32,
            "num_gpus": 1,
        },
    },
})
