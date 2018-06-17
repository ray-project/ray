""" Run script for multiagent pendulum env in which a single pendulum
is run as a multiagent system with a single agent.
"""

import gym
from gym.envs.registration import register

import ray
import ray.rllib.ppo as ppo
from ray.tune.registry import register_env
from ray.tune import run_experiments

env_name = "AlteredPendulumEnv"

env_version_num = 0
env_name = env_name + '-v' + str(env_version_num)


def pass_params_to_gym(env_name):
    global env_version_num

    register(
        id=env_name,
        entry_point='ray.rllib.examples:' + "AlteredPendulumEnv",
        max_episode_steps=config['horizon'],
        kwargs={}
    )


def create_env(env_config):
    pass_params_to_gym(env_name)
    env = gym.envs.make(env_name)
    return env


if __name__ == '__main__':
    register_env(env_name, lambda env_config: create_env(env_config))
    config = ppo.DEFAULT_CONFIG.copy()
    num_cpus = 8
    ray.init(redirect_output=False)
    config["timesteps_per_batch"] = 64
    config["num_sgd_iter"] = 10
    config["gamma"] = 0.95
    config["horizon"] = 200
    config["use_gae"] = False
    config["lambda"] = 0.1
    config["sgd_stepsize"] = .0003
    config["sgd_batchsize"] = 64
    config["min_steps_per_task"] = 100
    config["model"].update({"fcnet_hiddens": [256, 256]})
    options = {"multiagent_obs_shapes": [3],
               "multiagent_act_shapes": [1],
               "multiagent_shared_model": False,
               "multiagent_fcnet_hiddens": [[64, 64]]}
    config["model"].update({"custom_options": options})
    register_env("AlteredPendulumEnv-v0", create_env)

    trials = run_experiments({
            "pendulum_tests": {
                "run": "PPO",
                "env": "AlteredPendulumEnv-v0",
                "config": {
                   **config
                },
                "checkpoint_freq": 20,
                "max_failures": 999,
                "stop": {"training_iteration": 2},
                "trial_resources": {"cpu": 1, "gpu": 0, "extra_cpu": 0}
            },
        })
