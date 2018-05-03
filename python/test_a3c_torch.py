#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ray
from ray.rllib.a3c import DEFAULT_CONFIG, A3CAgent

ray.init(num_workers=0)

config = DEFAULT_CONFIG.copy()
config['num_workers'] = 1 # TODO try >1 worker
config['batch_size'] = 15
config['use_pytorch'] = True  # tf agent

agent = A3CAgent(config, 'CartPole-v0')

SEP = 72 * '-'

for i in range(2):
    print(i, SEP, sep='\n')

    agent.train()
