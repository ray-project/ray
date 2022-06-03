# Proximal Policy Optimization (PPO)

## Overview 

[PPO](https://arxiv.org/abs/1707.06347) is a model-free on-policy RL algorithm that works well for both discrete and continuous action space environments. PPO utilizes an actor-critic framework, where there are two networks, an actor (policy network) and critic network (value function). 

There are two formulations of PPO, which are both implemented in RLlib. The first formulation of PPO imitates the prior paper [TRPO](https://arxiv.org/abs/1502.05477) without the complexity of second-order optimization. In this formulation, for every iteration, an old version of an actor-network is saved and the agent seeks to optimize the RL objective while staying close to the old policy. This makes sure that the agent does not destabilize during training. In the second formulation, To mitigate destructive large policy updates, an issue discovered for vanilla policy gradient methods, PPO introduces the surrogate objective, which clips large action probability ratios between the current and old policy. Clipping has been shown in the paper to significantly improve training stability and speed. 

## Distributed PPO Algorithms

PPO is a core algorithm in RLlib due to its ability to scale well with the number of nodes. In RLlib, we provide various implementation of distributed PPO, with different underlying execution plans, as shown below. 

Distributed baseline PPO is a synchronous distributed RL algorithm. Data collection nodes, which represent the old policy, gather data synchronously to create a large pool of on-policy data from which the agent performs minibatch gradient descent on.

On the other hand, Asychronous PPO (APPO) opts to imitate IMPALA as its distributed execution plan. Data collection nodes gather data asynchronously, which are collected in a circular replay buffer. A target network and doubly-importance sampled surrogate objective is introduced to enforce training stability in the asynchronous data-collection setting.

Lastly, Decentralized Distributed PPO (DDPPO) removes the assumption that gradient-updates must be done on a central node.  Instead, gradients are computed remotely on each data collection node and all-reduced at each mini-batch using torch distributed. This allows each worker’s GPU to be used both for sampling and for training.

## Documentation & Implementation:

1) Proximal Policy Optimization (PPO). 

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#ppo)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo.py)**

2) [Asynchronous Proximal Policy Optimization (APPO)](https://arxiv.org/abs/1912.00167).

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#appo)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/appo.py)**

3) [Decentralized Distributed Proximal Policy Optimization (DDPPO)](https://arxiv.org/abs/1911.00357)

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#decentralized-distributed-proximal-policy-optimization-dd-ppo)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ddppo.py)**
