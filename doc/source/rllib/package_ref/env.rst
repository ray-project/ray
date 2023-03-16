.. _env-reference-docs:

Environments
============

BaseEnv class API
-----------

.. currentmodule:: ray.rllib.env

Constructor
~~~~~~~~~~~
.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

   ~base_env.BaseEnv

Interacting with the environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~base_env.BaseEnv.poll
   ~base_env.BaseEnv.send_actions
   ~base_env.BaseEnv.last
   ~base_env.BaseEnv.stop
   ~base_env.BaseEnv.try_reset
   ~base_env.BaseEnv.try_restart
   ~base_env.BaseEnv.try_render

Attributes
~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~base_env.BaseEnv.action_space
   ~base_env.BaseEnv.observation_space


Utilities
~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~base_env.BaseEnv.get_agent_ids
   ~base_env.BaseEnv.get_sub_environments
   ~base_env.BaseEnv.action_space_contains
   ~base_env.BaseEnv.observation_space_contains
   ~base_env.BaseEnv.action_space_sample
   ~base_env.BaseEnv.observation_space_sample

VectorEnv class API
-------------

Constructor
~~~~~~~~~~~
.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

   ~vector_env.VectorEnv

Environment conversion
~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~vector_env.VectorEnv.vectorize_gym_envs
   ~vector_env.VectorEnv.to_base_env


Interacting with the environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~vector_env.VectorEnv.vector_reset
   ~vector_env.VectorEnv.vector_step
   ~vector_env.VectorEnv.reset_at
   ~vector_env.VectorEnv.restart_at
   ~vector_env.VectorEnv.try_render_at



MultiAgentEnv class API
-----------------------

Extends the `gym.Env` API to support multi-agent.

.. currentmodule:: ray.rllib.env.multi_agent_env

Public methods
~~~~~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   make_multi_agent

Constructor
~~~~~~~~~~~
.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

   MultiAgentEnv


Environment conversion
~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~MultiAgentEnv.to_base_env
   
Environment interactions
~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
   :toctree: doc/

   ~MultiAgentEnv.reset
   ~MultiAgentEnv.step
   ~MultiAgentEnv.close
   ~MultiAgentEnv.render

Utilities
~~~~~~~~~

.. autosummary::
   :toctree: doc/

   ~MultiAgentEnv.get_agent_ids
   ~MultiAgentEnv.action_space_contains
   ~MultiAgentEnv.observation_space_contains
   ~MultiAgentEnv.action_space_sample
   ~MultiAgentEnv.observation_space_sample


External Application API
------------------------

In some cases, for instance when interacting with an externally hosted simulator or
production environment, it makes more sense to interact with RLlib as if it were an
independently running service, rather than RLlib hosting the simulations itself.




ExternalEnv class API
~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.env.external_env

Constructor
++++++++++
.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

   ExternalEnv


Environment conversion
++++++++++++++++++++++
.. autosummary::
   :toctree: doc/

   ExternalEnv.to_base_env
   


Interacting with the environment
++++++++++++++++++++++++++++++++
.. autosummary::
   :toctree: doc/


   ExternalEnv.start
   ExternalEnv.get_action
   ExternalEnv.start_episode
   ExternalEnv.end_episode
   ExternalEnv.run
   ExternalEnv.join
   ExternalEnv.is_alive


Logging
+++++++

.. autosummary::
   :toctree: doc/

   ExternalEnv.log_action
   ExternalEnv.log_returns


PolicyClient API
~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.env.policy_client

.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

   PolicyClient
   PolicyClient.start_episode
   PolicyClient.end_episode
   PolicyClient.get_action
   PolicyClient.log_action
   PolicyClient.log_returns
   PolicyClient.update_policy_weights


PolicyServerInput API
~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.env.policy_server_input

.. autosummary::
   :toctree: doc/
   :template: autosummary/class_with_autosummary.rst

   PolicyServerInput
   PolicyServerInput.next


