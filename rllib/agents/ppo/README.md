#Proximal Policy Optimization (PPO)

## Overview 

[PPO](https://arxiv.org/abs/1707.06347) is an model-free on-policy RL algorithm that works well for both discrete and continuous action space environments. PPO utilizes an actor-critic framework, where there are two networks, an actor (policy network) and critic network (value function). 

There are two formulations of PPO, which are both implemented in RLlib. The first formulation of PPO imitates the prior paper [TRPO](https://arxiv.org/abs/1502.05477). In this formulation, for every iteration, an old version of an actor-network is saved and the agent seeks to optimize the RL objective while staying close to the old policy. This makes sure that the agent does not destabilize during training. In the second formulation, To mitigate destructive large policy updates, an issue discovered for vanilla policy gradient methods, PPO introduces the surrogate objective, which clips large action probability ratios between the current and old policy. Clipping has been shown in the paper to significantly improve training stability and speed. 

## Documentation & Implementation:

1) Proximal Policy Optimization (PPO). 

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#ppo)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ppo.py)**

2) Asynchronous Proximal Policy Optimization (APPO).

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#appo)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/appo.py)**

3) Decentralized Distributed Proximal Policy Optimization (DDPPO)

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#decentralized-distributed-proximal-policy-optimization-dd-ppo)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/ddppo.py)**

