RLLib: Ray's modular and scalable reinforcement learning library
================================================================

This document describes Ray's reinforcement learning library.
It currently supports the following algorithms:

-  `Proximal Policy Optimization <https://arxiv.org/abs/1707.06347>`__ which
   is a proximal variant of `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  Evolution Strategies which is decribed in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   borrows code from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

-  `Deep Q Networks <https://www.cs.toronto.edu/~vmnih/docs/dqn.pdf>`__
   based on `OpenAI baselines <https://github.com/openai/baselines>`__.

-  `The Asynchronous Advantage Actor-Critic <https://arxiv.org/abs/1602.01783>`__
   based on `the OpenAI starter agent <https://github.com/openai/universe-starter-agent>`__.

Getting Started
---------------

You can run training with

::

    python train.py --env CartPole-v0 --alg PPO
