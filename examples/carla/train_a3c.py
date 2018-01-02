from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_model
from scenarios import TOWN2_STRAIGHT

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 240,
    "y_res": 240,
    "use_depth_camera": False,
    "discrete_actions": False,
    "server_map": "/Game/Maps/Town02",
    "scenarios": TOWN2_STRAIGHT,
})
register_env(env_name, lambda: CarlaEnv(env_config))
register_carla_model()

run_experiments({
    "carla": {
        "run": "A3C",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "model": {
                "custom_model": "carla",
                "custom_options": {
                    "image_shape": [
                        env_config["x_res"], env_config["y_res"], 3],
                },
            },
            "gamma": 0.95,
            "num_workers": 1,
        },
    },
})
