# Asynchronous Proximal Policy Optimization (APPO)

## Overview 

[PPO](https://arxiv.org/abs/1707.06347) is a model-free on-policy RL algorithm that works
well for both discrete and continuous action space environments. PPO utilizes an
actor-critic framework, where there are two networks, an actor (policy network) and
critic network (value function). 

## Distributed PPO Algorithms

### Distributed baseline PPO
[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py)

### Asychronous PPO (APPO) ..

.. opts to imitate IMPALA as its distributed execution plan.
Data collection nodes gather data asynchronously, which are collected in a circular replay
buffer. A target network and doubly-importance sampled surrogate objective is introduced
to enforce training stability in the asynchronous data-collection setting.
[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/appo/appo.py)

### Decentralized Distributed PPO (DDPPO)

[See implementation here](https://github.com/ray-project/ray/blob/master/rllib/algorithms/ddppo/ddppo.py)


## Documentation & Implementation:

### [Asynchronous Proximal Policy Optimization (APPO)](https://arxiv.org/abs/1912.00167).

**[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#appo)**

**[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/ppo/appo.py)**
