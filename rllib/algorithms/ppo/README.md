# Proximal Policy Optimization (PPO)

## Overview 

[PPO](https://arxiv.org/abs/1707.06347) is a model-free on-policy RL algorithm that works
well for both discrete and continuous action space environments. PPO utilizes an
actor-critic framework, where there are two networks, an actor (policy network) and
critic network (value function). 

There are two formulations of PPO, which are both implemented in RLlib. The first
formulation of PPO imitates the prior paper [TRPO](https://arxiv.org/abs/1502.05477)
without the complexity of second-order optimization. In this formulation, for every
iteration, an old version of an actor-network is saved and the agent seeks to optimize
the RL objective while staying close to the old policy. This makes sure that the agent
does not destabilize during training. In the second formulation, To mitigate destructive
large policy updates, an issue discovered for vanilla policy gradient methods, PPO
introduces the surrogate objective, which clips large action probability ratios between
the current and old policy. Clipping has been shown in the paper to significantly
improve training stability and speed. 

## Distributed PPO Algorithms

PPO is a core algorithm in RLlib due to its ability to scale well with the number of nodes.

In RLlib, we provide various implementations of distributed PPO, with different underlying
execution plans, as shown below:

### Distributed baseline PPO ..
.. is a synchronous distributed RL algorithm (this algo here).
Data collection nodes, which represent the old policy, gather data synchronously to
create a large pool of on-policy data from which the agent performs minibatch
gradient descent on.

### Asychronous PPO (APPO)

[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/appo/appo.py)

### Decentralized Distributed PPO (DDPPO)

[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ddppo/ddppo.py)


## Documentation & Implementation:

### Proximal Policy Optimization (PPO). 

**[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#ppo)**

**[Implementation](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py)**
