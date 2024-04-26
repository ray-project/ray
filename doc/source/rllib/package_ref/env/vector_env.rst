.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _vector-env-reference-docs:

VectorEnv API
=============

rllib.env.vector_env.VectorEnv
------------------------------

.. autoclass:: ray.rllib.env.vector_env.VectorEnv
    :special-members: __init__
    :members:


Gym.Env to VectorEnv
--------------------

Internally, RLlib uses the following wrapper class to convert your provided ``gym.Env`` class into
a ``VectorEnv`` first. After that, RLlib will convert the resulting objects into a ``BaseEnv``.

.. autoclass:: ray.rllib.env.vector_env._VectorizedGymEnv
    :special-members: __init__
    :members:
