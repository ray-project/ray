# Decentralized Distributed Proximal Policy Optimization (DDPPO)

## Overview 

[PPO](https://arxiv.org/abs/1707.06347) is a model-free on-policy RL algorithm that works
well for both discrete and continuous action space environments. PPO utilizes an
actor-critic framework, where there are two networks, an actor (policy network) and
critic network (value function). 

## Distributed PPO Algorithms

### Distributed baseline PPO
[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py)

### Asychronous PPO (APPO)
[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/appo/appo.py)


### Decentralized Distributed PPO (DDPPO) ..

.. removes the assumption that gradient-updates must
be done on a central node.  Instead, gradients are computed remotely on each data
collection node and all-reduced at each mini-batch using torch distributed. This allows
each worker’s GPU to be used both for sampling and for training.

[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ddppo/ddppo.py)


## Documentation & Implementation:

### [Decentralized Distributed Proximal Policy Optimization (DDPPO)](https://arxiv.org/abs/1911.00357)

**[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#decentralized-distributed-proximal-policy-optimization-dd-ppo)**

**[Implementation](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ddppo/ddppo.py)**
