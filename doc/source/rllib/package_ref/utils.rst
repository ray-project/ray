.. _utils-reference-docs:


RLlib Utilities
===============

Exploration API
+++++++++++++++

Exploration in RL is crucial for a learning agent in order to more easily
reach areas of the environment that have not been discovered so far and therefore
find new states yielding possibly high rewards.

RLlib comes with several built-in exploration components, used by
the different algorithms. Also users can customize an algo's exploration
behavior by sub-classing the Exploration base class and implementing
their own logic:

Base Exploration class (ray.rllib.utils.explorations.exploration.Exploration)
-----------------------------------------------------------------------------

.. autoclass:: ray.rllib.utils.explorations.exploration.Exploration
    :members:

All built-in Exploration classes
--------------------------------

.. automodule:: ray.rllib.utils.explorations.random.Random
    :special-members: __init__
    :members:

.. automodule:: ray.rllib.utils.explorations.stochastic_sampling.StochasticSampling
    :special-members: __init__
    :members:

.. automodule:: ray.rllib.utils.explorations.epsilon_greedy.EpsilonGreedy
    :special-members: __init__
    :members:

.. automodule:: ray.rllib.utils.explorations.gaussian_noise.GaussianNoise
    :special-members: __init__
    :members:

.. automodule:: ray.rllib.utils.explorations.ornstein_uhlenbeck_noise.OrnsteinUhlenbeckNoise
    :special-members: __init__
    :members:

.. automodule:: ray.rllib.utils.explorations.parameter_noise.ParameterNoise
    :special-members: __init__
    :members:

.. automodule:: ray.rllib.utils.explorations.epsilon_greedy.EpsilonGreedy
    :special-members: __init__
    :members:


Schedules
+++++++++



Utility Functions
+++++++++++++++++

.. automodule:: ray.rllib.utils.
    :members:
