#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ray
from ray.rllib.trpo import DEFAULT_CONFIG, TRPOAgent

ray.init(num_workers=0)

config = DEFAULT_CONFIG.copy()
config['num_workers'] = 2  # TODO try >1 worker
config['batch_size'] = 64
config['use_pytorch'] = True  # tf agent

agent = TRPOAgent(config, 'CartPole-v0')

SEP = 72 * '-'

for i in range(2000):
    print(i, SEP, sep='\n')

    res = agent.train()

    print(res)
