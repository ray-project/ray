#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ray
from ray.rllib.trpo import DEFAULT_CONFIG, TRPOAgent

ray.init(num_workers=0)

config = DEFAULT_CONFIG.copy()
config['num_workers'] = 2

agent = TRPOAgent(config, 'CartPole-v0')

for _ in range(2):
    agent.train()
