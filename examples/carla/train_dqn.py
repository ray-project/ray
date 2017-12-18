#!/usr/bin/env python

from ray.tune import register_env, run_experiments

from env import CarlaEnv, ENV_CONFIG

env_name = "carla_env"
env_config = ENV_CONFIG.copy()
env_config.update({
    "x_res": 210,
    "y_res": 160,
    "use_depth_camera": False,
    "discrete_actions": True,
    "max_steps": 150,
})
register_env(env_name, lambda: CarlaEnv(env_config))

run_experiments({
    "carla": {
        "run": "DQN",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
            "timesteps_per_iteration": 100,
            "learning_starts": 100,
            "schedule_max_timesteps": 20000,
            "gamma": 0.95,
        },
    },
})
