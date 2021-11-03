ExternalEnv API (rllib.env.external_env.ExternalEnv)
====================================================

Base Class (Single-Agent Case)
------------------------------

.. autoclass:: ray.rllib.env.external_env.ExternalEnv
    :special-members: __init__
    :members:


Multi-Agent Case
----------------

If your external environment needs to support multi-agent RL, you should instead
sub-class ``ExternalMultiAgentEnv``:

.. autoclass:: ray.rllib.env.external_multi_agent_env.ExternalMultiAgentEnv
    :special-members: __init__
    :members:
