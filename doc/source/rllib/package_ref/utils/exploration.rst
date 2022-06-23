.. _exploration-reference-docs:

Exploration API
===============

Exploration in RL is crucial for a learning agent in order to more easily
reach areas of the environment that have not been discovered so far and therefore
find new states yielding possibly high rewards.

RLlib comes with several built-in exploration components, used by
the different algorithms. Also users can customize an algo's exploration
behavior by sub-classing the Exploration base class and implementing
their own logic:

Base Exploration class (ray.rllib.utils.exploration.exploration.Exploration)
-----------------------------------------------------------------------------

.. autoclass:: ray.rllib.utils.exploration.exploration.Exploration
    :members:

All built-in Exploration classes
--------------------------------

.. autoclass:: ray.rllib.utils.exploration.random.Random
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.stochastic_sampling.StochasticSampling
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.epsilon_greedy.EpsilonGreedy
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.gaussian_noise.GaussianNoise
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.ornstein_uhlenbeck_noise.OrnsteinUhlenbeckNoise
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.random_encoder.RE3
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.curiosity.Curiosity
    :special-members: __init__
    :members:

.. autoclass:: ray.rllib.utils.exploration.parameter_noise.ParameterNoise
    :special-members: __init__
    :members:
