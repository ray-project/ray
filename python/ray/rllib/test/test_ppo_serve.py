#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import gym
import ray
from ray.rllib.ppo import PPOAgent, DEFAULT_CONFIG


config = DEFAULT_CONFIG.copy()
config["num_workers"] = 3
config["num_sgd_iter"] = 6
config["sgd_batchsize"] = 128
config["timesteps_per_batch"] = 4000

ray.init()

# first train one agent
agent = PPOAgent("CartPole-v0", config)

for i in range(10):
    result = agent.train()
    print(result)

# checkpoint and restore in a copied agent
checkpoint_path = agent.save()
trained_config = config.copy()
test_agent = PPOAgent("CartPole-v0", trained_config)
test_agent.restore(checkpoint_path)

# evaluate on copied agent
results = []
env = gym.make("CartPole-v0")
for _ in range(20):
    state = env.reset()
    done = False
    cumulative_reward = 0

    while not done:
        action = test_agent.compute_action(state)
        state, reward, done, _ = env.step(action)
        cumulative_reward += reward

    results.append(cumulative_reward)

print("All results", results)
print("Mean result", np.mean(results))

assert(np.mean(results)) > 0.9 * result.episode_reward_mean
