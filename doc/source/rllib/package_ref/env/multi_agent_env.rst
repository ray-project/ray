.. _multiagentenv-docs:

MultiAgentEnv API (rllib.env.multi_agent_env.MultiAgentEnv)
===========================================================

.. autoclass:: ray.rllib.env.multi_agent_env.MultiAgentEnv
    :members:
    :inherited-members: __init__, reset, step, render, with_agent_groups
    :undoc-members:


Convert single-agent envs into multi-agent ones via cloning
-----------------------------------------------------------

A convenience method to convert a simple (single-agent) ``gym.Env`` class
into a ``MultiAgentEnv`` class is provided here:

.. automodule:: ray.rllib.env.multi_agent_env
    :members: make_multi_agent
