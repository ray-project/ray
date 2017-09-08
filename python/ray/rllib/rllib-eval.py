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
        self.max_iters = 5
        self.agent = None

    def start(self):
        (agent_class, agent_config) = AGENTS[exp.alg]
        self.agent = ray.remote(agent_class).remote(agent_config)

    def train(self):
        self.agent.train.remote()


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

    agent_dict = {exp.agent.train.remote(): exp for exp in experiments}
    while True:
        [next_agent], waiting_agents = ray.wait(list(agent_dict.keys()))
        agent = agent_dict.pop(next_agent)
        result = ray.get(next_agent)
        print(result)
