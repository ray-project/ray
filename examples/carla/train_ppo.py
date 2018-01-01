from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG
from models import register_carla_preprocessor, register_carla_model
from scenarios import TOWN2_STRAIGHT

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "verbose": False,
    "x_res": 240,
    "y_res": 240,
    "use_depth_camera": True,
    "discrete_actions": False,
    "server_map": "/Game/Maps/Town02",
    "scenarios": TOWN2_STRAIGHT,
})
register_env(env_name, lambda: CarlaEnv(env_config))
register_carla_preprocessor()
register_carla_model()

run_experiments({
    "carla": {
        "run": "PPO",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "model": {
                "custom_preprocessor": "carla",
                "custom_model": "carla",
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
}, redirect_output=True)
