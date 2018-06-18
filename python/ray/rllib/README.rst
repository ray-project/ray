Ray RLlib: Scalable Reinforcement Learning
==========================================

Ray RLlib is an RL execution toolkit built on the Ray distributed execution framework. See the `user documentation <http://ray.readthedocs.io/en/latest/rllib.html>`__ and `paper <https://arxiv.org/abs/1712.09381>`__.

RLlib includes the following reference algorithms:

- Proximal Policy Optimization (`PPO <https://github.com/ray-project/ray/tree/master/python/ray/rllib/ppo>`__) which is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

- Policy Gradients (`PG <https://github.com/ray-project/ray/tree/master/python/ray/rllib/pg>`__).

- Asynchronous Advantage Actor-Critic (`A3C <https://github.com/ray-project/ray/tree/master/python/ray/rllib/a3c>`__).

- Deep Q Networks (`DQN <https://github.com/ray-project/ray/tree/master/python/ray/rllib/dqn>`__).

- Deep Deterministic Policy Gradients (`DDPG <https://github.com/ray-project/ray/tree/master/python/ray/rllib/ddpg>`__).

- Ape-X Distributed Prioritized Experience Replay, including both `DQN <https://github.com/ray-project/ray/blob/master/python/ray/rllib/dqn/apex.py>`__ and `DDPG <https://github.com/ray-project/ray/blob/master/python/ray/rllib/ddpg/apex.py>`__ variants.

- Evolution Strategies (`ES <https://github.com/ray-project/ray/tree/master/python/ray/rllib/es>`__), as described in `this paper <https://arxiv.org/abs/1703.03864>`__.

These algorithms can be run on any OpenAI Gym MDP, including custom ones written and registered by the user.
