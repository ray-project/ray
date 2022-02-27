.. _multi-agent-env-reference-docs:

MultiAgentEnv API
=================

rllib.env.multi_agent_env.MultiAgentEnv
---------------------------------------

.. autoclass:: ray.rllib.env.multi_agent_env.MultiAgentEnv

    .. automethod:: __init__
    .. automethod:: reset
    .. automethod:: step
    .. automethod:: render
    .. automethod:: with_agent_groups


Convert gym.Env into MultiAgentEnv
----------------------------------

.. automodule:: ray.rllib.env.multi_agent_env
    :members: make_multi_agent
