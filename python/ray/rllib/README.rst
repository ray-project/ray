Ray RLlib: Scalable Reinforcement Learning
==========================================

Ray RLlib is an RL execution toolkit built on the Ray distributed execution framework. See the `user documentation <http://ray.readthedocs.io/en/latest/rllib.html>`__ and `paper <https://arxiv.org/abs/1712.09381>`__.

RLlib includes the following reference algorithms:

-  `Proximal Policy Optimization (PPO) <https://arxiv.org/abs/1707.06347>`__ which
   is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  `The Asynchronous Advantage Actor-Critic (A3C) <https://arxiv.org/abs/1602.01783>`__.

- `Deep Q Networks (DQN) <https://arxiv.org/abs/1312.5602>`__.

- `Ape-X Distributed Prioritized Experience Replay <https://arxiv.org/abs/1803.00933>`__.

-  Evolution Strategies, as described in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   is adapted from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

These algorithms can be run on any OpenAI Gym MDP, including custom ones written and registered by the user.
