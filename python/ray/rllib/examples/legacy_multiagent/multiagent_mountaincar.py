""" Multiagent mountain car. Each agent outputs an action which
is summed to form the total action. This is a discrete
multiagent example
"""

import gym
from gym.envs.registration import register

import ray
import ray.rllib.agents.ppo as ppo
from ray.tune.registry import register_env

env_name = "MultiAgentMountainCarEnv"

env_version_num = 0
env_name = env_name + '-v' + str(env_version_num)


def pass_params_to_gym(env_name):
    global env_version_num

    register(
        id=env_name,
        entry_point=(
            "ray.rllib.examples.legacy_multiagent.multiagent_mountaincar_env:"
            "MultiAgentMountainCarEnv"),
        max_episode_steps=200,
        kwargs={})


def create_env(env_config):
    pass_params_to_gym(env_name)
    env = gym.envs.make(env_name)
    return env


if __name__ == '__main__':
    register_env(env_name, lambda env_config: create_env(env_config))
    config = ppo.DEFAULT_CONFIG.copy()
    horizon = 10
    num_cpus = 4
    ray.init(num_cpus=num_cpus, redirect_output=True)
    config["num_workers"] = num_cpus
    config["timesteps_per_batch"] = 10
    config["num_sgd_iter"] = 10
    config["gamma"] = 0.999
    config["horizon"] = horizon
    config["use_gae"] = False
    config["model"].update({"fcnet_hiddens": [256, 256]})
    options = {
        "multiagent_obs_shapes": [2, 2],
        "multiagent_act_shapes": [1, 1],
        "multiagent_shared_model": False,
        "multiagent_fcnet_hiddens": [[32, 32]] * 2
    }
    config["model"].update({"custom_options": options})
    alg = ppo.PPOAgent(env=env_name, config=config)
    for i in range(1):
        alg.train()
