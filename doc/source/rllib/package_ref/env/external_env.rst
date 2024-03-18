
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _external-env-reference-docs:

ExternalEnv API
===============

ExternalEnv (Single-Agent Case)
-------------------------------

rllib.env.external_env.ExternalEnv
++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.env.external_env.ExternalEnv
    :special-members: __init__
    :members:


ExternalMultiAgentEnv (Multi-Agent Case)
----------------------------------------

rllib.env.external_multi_agent_env.ExternalMultiAgentEnv
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

If your external environment needs to support multi-agent RL, you should instead
sub-class ``ExternalMultiAgentEnv``:

.. autoclass:: ray.rllib.env.external_multi_agent_env.ExternalMultiAgentEnv
    :special-members: __init__
    :members:
