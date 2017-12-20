from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "x_res": 160,
    "y_res": 210,
    "use_depth_camera": False,
    "discrete_actions": True,
    "max_steps": 200,
    "weather": [1, 3, 7, 8, 14],
})
register_env(env_name, lambda: CarlaEnv(env_config))

run_experiments({
    "carla": {
        "run": "DQN",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "timesteps_per_iteration": 100,
            "learning_starts": 1000,
            "schedule_max_timesteps": 100000,
            "gamma": 0.95,
            "tf_session_args": {
              "gpu_options": {"allow_growth": True},
            },
        },
    },
})
