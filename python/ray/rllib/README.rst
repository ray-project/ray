RLLib: Ray's modular and scalable reinforcement learning library
================================================================

Getting Started
---------------

You can run training with

::

    python train.py --env CartPole-v0 --run PPO

The available algorithms are:

-  ``PPO`` is a proximal variant of
   `TRPO <https://arxiv.org/abs/1502.05477>`__.

-  ``ES`` is decribed in `this
   paper <https://arxiv.org/abs/1703.03864>`__. Our implementation
   borrows code from
   `here <https://github.com/openai/evolution-strategies-starter>`__.

-  ``DQN`` is an implementation of `Deep Q
   Networks <https://www.cs.toronto.edu/~vmnih/docs/dqn.pdf>`__ based on
   `OpenAI baselines <https://github.com/openai/baselines>`__.

-  ``A3C`` is an implementation of
   `A3C <https://arxiv.org/abs/1602.01783>`__ based on `the OpenAI
   starter agent <https://github.com/openai/universe-starter-agent>`__.

Documentation can be `found here <http://ray.readthedocs.io/en/latest/rllib.html>`__.
