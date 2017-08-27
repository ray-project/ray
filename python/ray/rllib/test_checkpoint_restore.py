#!/usr/bin/env python

import ray

from ray.rllib.dqn import DQN, DEFAULT_CONFIG
cls = DQN

ray.init()
config = DEFAULT_CONFIG.copy()
config["num_sgd_iter"] = 5
config["episodes_per_batch"] = 100
config["timesteps_per_batch"] = 1000
alg1 = cls('CartPole-v0', config)

print("Test action", alg1.compute_action([0, 0, 0, 0]))

res = alg1.train()
while res.episode_reward_mean < 180:
    for _ in range(5):
        res = alg1.train()
        print("current status: " + str(res))
    checkpoint = alg1.save()
    print('-------------------------------------------------')
    alg1 = cls('CartPole-v0', config)
    alg1.restore(checkpoint)

print()
print("Finished: " + str(res))
print()
