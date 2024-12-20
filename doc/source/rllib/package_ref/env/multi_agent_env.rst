
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _multi-agent-env-reference-docs:

MultiAgentEnv API
=================

rllib.env.multi_agent_env.MultiAgentEnv
---------------------------------------

.. autoclass:: ray.rllib.env.multi_agent_env.MultiAgentEnv

    .. automethod:: __init__
    .. automethod:: reset
    .. automethod:: step
    .. automethod:: get_observation_space
    .. automethod:: get_action_space
    .. automethod:: with_agent_groups
    .. automethod:: render


Convert gymnasium.Env into MultiAgentEnv
----------------------------------------

.. automodule:: ray.rllib.env.multi_agent_env
    :members: make_multi_agent
