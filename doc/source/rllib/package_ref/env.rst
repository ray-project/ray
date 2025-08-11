.. include:: /_includes/rllib/we_are_hiring.rst

.. _env-reference-docs:

Environments
============

.. include:: /_includes/rllib/new_api_stack.rst

RLlib mainly supports the `Farama gymnasium API <https://gymnasium.farama.org/>`__ for
single-agent environments, and RLlib's own :py:class:`~ray.rllib.env.multi_agent_env.MultiAgentEnv`
API for multi-agent setups.

Env Vectorization
-----------------

For single-agent setups, RLlib automatically vectorizes your provided
`gymnasium.Env <https://gymnasium.farama.org/_modules/gymnasium/core/#Env>`__ using
gymnasium's own `vectorization feature <https://gymnasium.farama.org/api/vector/>`__.

Use the `config.env_runners(num_envs_per_env_runner=..)` setting to vectorize your env
beyond 1 env copy.


External Envs
-------------

.. note::

    External Env support is under development on the new API stack. The recommended
    way to implement your own external env connection logic, for example through TCP or
    shared memory, is to write your own :py:class:`~ray.rllib.env.env_runner.EnvRunner`
    subclass.

See this an end-to-end example of an `external CartPole (client) env <https://github.com/ray-project/ray/blob/master/rllib/examples/envs/env_connecting_to_rllib_w_tcp_client.py>`__
connecting to RLlib through a custom, TCP-capable
:py:class:`~ray.rllib.env.env_runner.EnvRunner` server.


Environment API Reference
-------------------------

.. toctree::
   :maxdepth: 1

   env/env_runner.rst
   env/single_agent_env_runner.rst
   env/single_agent_episode.rst
   env/multi_agent_env.rst
   env/multi_agent_env_runner.rst
   env/multi_agent_episode.rst
   env/external.rst
   env/utils.rst
