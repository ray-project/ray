

ray.rllib.env.multi\_agent\_episode.MultiAgentEpisode
=====================================================

.. currentmodule:: ray.rllib.env.multi_agent_episode

.. autoclass:: MultiAgentEpisode
   :show-inheritance:

   
   
   .. rubric:: Methods

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~MultiAgentEpisode.__init__
      ~MultiAgentEpisode.add_env_reset
      ~MultiAgentEpisode.add_env_step
      ~MultiAgentEpisode.add_temporary_timestep_data
      ~MultiAgentEpisode.agent_steps
      ~MultiAgentEpisode.concat_episode
      ~MultiAgentEpisode.cut
      ~MultiAgentEpisode.env_steps
      
      ~MultiAgentEpisode.from_state
      ~MultiAgentEpisode.get_actions
      ~MultiAgentEpisode.get_agents_that_stepped
      ~MultiAgentEpisode.get_agents_to_act
      ~MultiAgentEpisode.get_duration_s
      ~MultiAgentEpisode.get_extra_model_outputs
      ~MultiAgentEpisode.get_infos
      ~MultiAgentEpisode.get_observations
      ~MultiAgentEpisode.get_return
      ~MultiAgentEpisode.get_rewards
      ~MultiAgentEpisode.get_sample_batch
      ~MultiAgentEpisode.get_state
      ~MultiAgentEpisode.get_temporary_timestep_data
      ~MultiAgentEpisode.get_terminateds
      
      
      ~MultiAgentEpisode.module_for
      ~MultiAgentEpisode.print
      ~MultiAgentEpisode.slice
      ~MultiAgentEpisode.to_numpy
      ~MultiAgentEpisode.validate

   
   


   
   
   .. rubric:: Attributes

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~MultiAgentEpisode.id_
      ~MultiAgentEpisode.agent_to_module_mapping_fn
      ~MultiAgentEpisode.observation_space
      ~MultiAgentEpisode.action_space
      ~MultiAgentEpisode.env_t_started
      ~MultiAgentEpisode.env_t
      ~MultiAgentEpisode.agent_t_started
      ~MultiAgentEpisode.env_t_to_agent_t
      ~MultiAgentEpisode.is_terminated
      ~MultiAgentEpisode.is_truncated
      ~MultiAgentEpisode.agent_episodes
      ~MultiAgentEpisode.SKIP_ENV_TS_TAG
      ~MultiAgentEpisode.agent_episode_ids
      ~MultiAgentEpisode.agent_ids
      ~MultiAgentEpisode.is_done
      ~MultiAgentEpisode.is_numpy
      ~MultiAgentEpisode.is_reset

   
   