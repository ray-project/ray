#!/usr/bin/env python
# This script runs all the integration tests for RLlib.
# TODO(ekl) add large-scale tests on different envs here.

import glob
import yaml

import ray
from ray.tune import run_experiments
from gym.envs.registration import register
from ray.tune.registry import register_env
import gym


def pass_params_to_gym(env_config):
    env_name = env_config["env_name"]
    env_version = env_name+'-v0'
    register(
        id=env_version,
        entry_point='ray.rllib.examples:' + env_name,
        max_episode_steps=env_config['horizon'],
        kwargs={}
    )


def create_env(env_config):
    env_name = env_config["env_name"]
    pass_params_to_gym(env_config)
    env = gym.envs.make(env_name+'-v0')
    return env


if __name__ == '__main__':
    experiments = {}

    for test in glob.glob("regression_tests/*.yaml"):
        config = yaml.load(open(test).read())
        experiments.update(config)

    # now add the multiagent tests
    for test in glob.glob("multiagent_regression_tests/*.yaml"):
        config = yaml.load(open(test).read())
        for key in config.keys():
            register_env(config[key]['env'], create_env)
        experiments.update(config)

    print("== Test config ==")
    print(yaml.dump(experiments))
    ray.init()
    trials = run_experiments(experiments)

    num_failures = 0
    for t in trials:
        if (t.last_result.episode_reward_mean <
                t.stopping_criterion["episode_reward_mean"]):
            num_failures += 1

    if num_failures:
        raise Exception(
            "{} trials did not converge".format(num_failures))
