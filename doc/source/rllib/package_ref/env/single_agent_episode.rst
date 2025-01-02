.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _single-agent-episode-reference-docs:

SingleAgentEpisode API
======================

rllib.env.single_agent_episode.SingleAgentEpisode
-------------------------------------------------

.. currentmodule:: ray.rllib.env.single_agent_episode

Constructor
~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode
    ~SingleAgentEpisode.validate

Getting basic information
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.__len__
    ~SingleAgentEpisode.get_return
    ~SingleAgentEpisode.get_duration_s
    ~SingleAgentEpisode.is_done
    ~SingleAgentEpisode.is_finalized
    ~SingleAgentEpisode.env_steps

Getting environment data
~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.get_observations
    ~SingleAgentEpisode.get_infos
    ~SingleAgentEpisode.get_actions
    ~SingleAgentEpisode.get_rewards
    ~SingleAgentEpisode.get_extra_model_outputs
    ~SingleAgentEpisode.get_temporary_timestep_data

Adding data
~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.add_env_reset
    ~SingleAgentEpisode.add_env_step
    ~SingleAgentEpisode.add_temporary_timestep_data

Creating and handling episode chunks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: env/

    ~SingleAgentEpisode.cut
    ~SingleAgentEpisode.slice
    ~SingleAgentEpisode.concat_episode
    ~SingleAgentEpisode.finalize
