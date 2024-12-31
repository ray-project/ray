
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _env-runner-reference-docs:

EnvRunner API
=============

rllib.env.env_runner.EnvRunner
------------------------------

.. currentmodule:: ray.rllib.env.env_runner

Construction and setup
~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunner
    EnvRunner.make_env
    EnvRunner.make_module
    EnvRunner.get_spaces
    EnvRunner.assert_healthy

Sampling
~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunner.sample
    EnvRunner.get_metrics

Cleanup
~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunner.stop



Single-agent and multi-agent EnvRunners
---------------------------------------

By default, RLlib uses two built-in subclasses of EnvRunner, one for single-agent, one
for multi-agent setups.


rllib.env.single_agent_env_runner.SingleAgentEnvRunner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.env.single_agent_env_runner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    SingleAgentEnvRunner
    SingleAgentEnvRunner.get_state
    SingleAgentEnvRunner.set_state


rllib.env.multi_agent_env_runner.MultiAgentEnvRunner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.env.multi_agent_env_runner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiAgentEnvRunner
    MultiAgentEnvRunner.get_state
    MultiAgentEnvRunner.set_state
