.. _env-docs:

Environment APIs
================

BaseEnv API (rllib.env.base_env.BaseEnv)
++++++++++++++++++++++++++++++++++++++++

RLlib internally works with the `BaseEnv` API, which is quite similar to openAI's
`gym.Env` API.

.. autoclass:: ray.rllib.env.base_env.BaseEnv
    :members:

MultiAgentEnv API (rllib.env.multi_agent_env.MultiAgentEnv)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.multi_agent_env.MultiAgentEnv
    :members:

VectorEnv (rllib.env.vector_env.VectorEnv)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.vector_env.VectorEnv
    :members:

ExternalEnv API (rllib.env.external_env.ExternalEnv)
++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.external_env.ExternalEnv
    :members:

