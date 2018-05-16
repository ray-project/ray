""" Run script for multiagent cartpole env. Each agent outputs a
action whose mean is the actual action. This is a multiagent
example of an extremely simple task meant to be used for testing
"""

import gym
from gym.envs.registration import register

import ray
import ray.rllib.ppo as ppo
from ray.tune.registry import register_env
from ray.tune import run_experiments

env_name = "MultiAgentCartPoleEnv"

env_version_num = 0
env_name = env_name + '-v' + str(env_version_num)


def pass_params_to_gym(env_name):
    global env_version_num

    register(
        id=env_name,
        entry_point='ray.rllib.examples:' + "MultiAgentCartPoleEnv",
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
    config["timesteps_per_batch"] = 100
    num_cpus = 2
    ray.init(redirect_output=False)
    options = {"multiagent_obs_shapes": [4, 4],
               "multiagent_act_shapes": [1, 1],
               "multiagent_shared_model": False,
               "multiagent_fcnet_hiddens": [[32, 32]] * 2}
    config["model"].update({"custom_options": options})
    config["horizon"] = 200
    register_env("MultiAgentCartPoleEnv-v0", create_env)
    trials = run_experiments({
            "pendulum_tests": {
                "run": "PPO",
                "env": "MultiAgentCartPoleEnv-v0",
                "config": {
                   **config
                },
                "checkpoint_freq": 20,
                "max_failures": 999,
                "stop": {"training_iteration": 1},
                "trial_resources": {"cpu": 1, "extra_cpu": 0}
            },
        })
