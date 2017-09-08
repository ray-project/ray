#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys

import ray
import ray.rllib.ppo as ppo
import ray.rllib.es as es
import ray.rllib.dqn as dqn
import ray.rllib.a3c as a3c


class Experiment(object):
    def __init__(self, alg, env, config):
        self.alg = alg
        self.env = env
        self.config = config
        self.max_iters = 0
        self.agent = None

    def start(self):
        (agent_class, agent_config) = AGENTS[exp.alg]
        config = agent_config.copy()
        for k in self.config.keys():
            if k not in config:
                raise Exception(
                    "Unknown agent config `{}`, all agent configs: {}".format(
                        k, config.keys()))
        config.update(self.config)
        self.agent = ray.remote(agent_class).remote(self.env, config)

    def train_remote(self):
        return self.agent.train.remote()

    def should_stop(self, result):
        return result.training_iteration > self.max_iters

    def __str__(self):
        return '{}_{}'.format(self.alg, self.env)


AGENTS = {
    'PPO': (ppo.PPOAgent, ppo.DEFAULT_CONFIG),
    'ES': (es.ESAgent, es.DEFAULT_CONFIG),
    'DQN': (dqn.DQNAgent, dqn.DEFAULT_CONFIG),
    'A3C': (a3c.A3CAgent, a3c.DEFAULT_CONFIG)
}


def parse_experiments(yaml_file):
    # TODO(ekl)
    return [
        Experiment('DQN', 'CartPole-v0', {'buffer_size': 10000}),
        Experiment('DQN', 'CartPole-v0', {'buffer_size': 1000}),
        Experiment('DQN', 'CartPole-v0', {'buffer_size': 100}),
        Experiment('DQN', 'CartPole-v0', {'buffer_size': 10})
    ]


if __name__ == '__main__':
    experiments = parse_experiments(sys.argv[1])

    for i, experiment in enumerate(experiments):
        print("Experiment {}: {}".format(i, experiment))

    ray.init()

    agents = []
    for exp in experiments:
        exp.start()

    print("Launching experiments...")
    running = {exp.train_remote(): exp for exp in experiments}

    while running:
        [next_agent], waiting_agents = ray.wait(list(running.keys()))
        exp = running.pop(next_agent)
        result = ray.get(next_agent)
        if exp.should_stop(result):
            print("{} *** FINISHED ***: {}".format(exp, result))
        else:
            print("{} progress: {}".format(exp, result))
            running[exp.train_remote()] = exp

    print("All experiments finished!")
