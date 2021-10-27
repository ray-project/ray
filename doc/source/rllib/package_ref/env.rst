.. _env-docs:

Environment APIs
================

.. _baseenv-docs:

BaseEnv API (rllib.env.base_env.BaseEnv)
++++++++++++++++++++++++++++++++++++++++

All environments in RLlib are converted internally into the RLlib ``BaseEnv`` API,
whose main methods are ``poll()`` and ``send_actions()``.

This API allows ``BaseEnv`` to support:

1) Vectorization of sub-envs in order to batch action-computing model forward passes.
2) Async execution via its ``poll()``/``send_actions`` methods, such that external simulators (e.g. Envs that run on separate machines and independently request actions from a policy server) can be handled through the API as well.
3) Parallelization of the vectorized sub-envs via ray.remote.
4) Multi-agent support via dicts mapping agentIDs to observations/rewards/etc..

The user, however, may conveniently provide any of the supported environment types.
The path from a user provided env type (or env generating callable) to
an RLlib BaseEnv is usually one of the following:

- User provides a gym.Env -> VectorEnv -> BaseEnv
- User provides a custom MultiAgentEnv (is-a gym.Env) -> VectorEnv -> BaseEnv
- User uses a policy client (via an external env) -> ExternalEnv -> BaseEnv
- User provides a custom VectorEnv -> BaseEnv
- User provides a custom BaseEnv -> do nothing

.. autoclass:: ray.rllib.env.base_env.BaseEnv
    :members:

.. _multiagentenv-docs:

MultiAgentEnv API (rllib.env.multi_agent_env.MultiAgentEnv)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.multi_agent_env.MultiAgentEnv
    :special-members: __init__
    :members: reset, step, render, with_agent_groups

A convenience method to convert a simple (single-agent) gym env
into a multi-agent env is provided as follows:

.. automodule:: ray.rllib.env.multi_agent_env
    :members: make_multi_agent


VectorEnv (rllib.env.vector_env.VectorEnv)
++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.vector_env.VectorEnv
    :special-members: __init__
    :members:

ExternalEnv API (rllib.env.external_env.ExternalEnv)
++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.external_env.ExternalEnv
    :special-members: __init__
    :members:

