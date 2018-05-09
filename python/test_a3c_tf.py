#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ray
from ray.rllib.a3c import DEFAULT_CONFIG, A3CAgent

ray.init(num_workers=0)

config = DEFAULT_CONFIG.copy()
config['num_workers'] = 2
config['use_pytorch'] = False  # tf agent

agent = A3CAgent(config, 'CartPole-v0')

SEP = 72 * '-'

for i in range(2000):
    print(i, SEP, sep='\n')

    res = agent.train()
    print(res)
