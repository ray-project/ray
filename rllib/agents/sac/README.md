# Soft Actor Critic (SAC)

## Overview 

[SAC](https://arxiv.org/abs/1801.01290) is a SOTA model-free off-policy RL algorithm that performs remarkably well on continuous-control domains. SAC employs an actor-critic framework and combats high sample complexity and training stability via learning based on a maximum-entropy framework. Unlike the standard RL objective which aims to maximize sum of reward into the future, SAC seeks to optimize sum of rewards as well as expected entropy over the current policy. In addition to optimizing over an actor and critic with entropy-based objectives, SAC also optimizes for the entropy coeffcient. 

## Documentation & Implementation:

[Soft Actor-Critic Algorithm (SAC)](https://arxiv.org/abs/1801.01290) with also [discrete-action support](https://arxiv.org/abs/1910.07207). 

**[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#sac)**

**[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/sac/sac.py)**



