from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

impor oray
from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_model
from scenarios import TOWN1_ONE_CURVE

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 80,
    "y_res": 80,
    "discrete_actions": True,
    "server_map": "/Game/Maps/Town01",
    "scenarios": TOWN1_ONE_CURVE,
})

register_env(env_name, lambda env_config: CarlaEnv(env_config))
register_carla_model()
redis_address = ray.services.get_node_ip_address() + ":6379"

run_experiments({
    "carla-a3c": {
        "run": "A3C",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 4, "driver_gpu_limit": 0},
        "config": {
            "env_config": env_config,
            "use_gpu_for_workers": True,
            "model": {
                "custom_model": "carla",
                "custom_options": {
                    "command_mode": "switched",
                    "image_shape": [80, 80, 6],
                },
                "conv_filters": [
                    [16, [8, 8], 4],
                    [32, [4, 4], 2],
                    [512, [10, 10], 1],
                ],
            },
            "gamma": 0.95,
            "num_workers": 4,
        },
    },
}, redis_address=redis_address, redirect_output=True)
