# Advantage Actor-Critic (A2C)

## Overview 

[Advantage Actor-Critic](https://arxiv.org/pdf/1602.01783.pdf) proposes two distributed model-free on-policy RL algorithms, A3C and A2C.
These algorithms are distributed versions of the vanilla Policy Gradient (PG) algorithm with different distributed execution patterns.
The paper suggests accelerating training via scaling data collection, i.e. introducing worker nodes,
which carry copies of the central node's policy network and collect data from the environment in parallel.
This data is used on each worker to compute gradients. The central node applies each of these gradients and then sends updated weights back to the workers.

In A2C, the worker nodes synchronously collect data. The collected data forms a giant batch of data,
from which the central node (the central policy) computes gradient updates.


## Documentation & Implementation of A2C:

**[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#a2c)**

**[Implementation](https://github.com/ray-project/ray/blob/master/rllib/algorithms/a2c/a2c.py)**
