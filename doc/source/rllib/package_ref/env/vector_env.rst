VectorEnv (rllib.env.vector_env.VectorEnv)
==========================================

.. autoclass:: ray.rllib.env.vector_env.VectorEnv
    :special-members: __init__
    :members:


Gym.Env to VectorEnv
--------------------

Internally, RLlib uses this wrapper to convert your provided ``gym.Env`` class into
a ``VectorEnv`` first (then RLlib will convert the resulting objects into a ``BaseEnv``).

.. autoclass:: ray.rllib.env.vector_env._VectorizedGymEnv
    :special-members: __init__
    :members:
