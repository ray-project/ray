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
    "use_depth_camera": True,
    "discrete_actions": False,
    "max_steps": 150,
})
register_env(env_name, lambda: CarlaEnv(env_config))

run_experiments({
    "carla": {
        "run": "PPO",
        "env": "carla_env",
        "resources": {"cpu": 4, "gpu": 1},
        "config": {
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
