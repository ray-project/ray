

ray.rllib.policy.sample\_batch.SampleBatch
==========================================

.. currentmodule:: ray.rllib.policy.sample_batch

.. autoclass:: SampleBatch
   :show-inheritance:

   
   
   .. rubric:: Methods

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~SampleBatch.__init__
      ~SampleBatch.agent_steps
      ~SampleBatch.as_multi_agent
      ~SampleBatch.clear
      ~SampleBatch.columns
      ~SampleBatch.compress
      ~SampleBatch.concat
      
      ~SampleBatch.copy
      ~SampleBatch.decompress_if_needed
      
      
      ~SampleBatch.env_steps
      ~SampleBatch.fromkeys
      ~SampleBatch.get
      ~SampleBatch.get_single_step_input_dict
      ~SampleBatch.is_single_trajectory
      ~SampleBatch.is_terminated_or_truncated
      ~SampleBatch.items
      ~SampleBatch.keys
      ~SampleBatch.pop
      ~SampleBatch.popitem
      ~SampleBatch.right_zero_pad
      ~SampleBatch.rows
      ~SampleBatch.set_get_interceptor
      ~SampleBatch.set_training
      ~SampleBatch.setdefault
      ~SampleBatch.shuffle
      ~SampleBatch.size_bytes
      ~SampleBatch.slice
      ~SampleBatch.split_by_episode
      ~SampleBatch.timeslices
      ~SampleBatch.to_device
      ~SampleBatch.update
      ~SampleBatch.values
      

   
   


   
   
   .. rubric:: Attributes

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~SampleBatch.ACTIONS
      ~SampleBatch.ACTION_DIST
      ~SampleBatch.ACTION_DIST_INPUTS
      ~SampleBatch.ACTION_LOGP
      ~SampleBatch.ACTION_PROB
      ~SampleBatch.AGENT_INDEX
      ~SampleBatch.ATTENTION_MASKS
      ~SampleBatch.CUR_OBS
      ~SampleBatch.DONES
      ~SampleBatch.ENV_ID
      ~SampleBatch.EPS_ID
      ~SampleBatch.INFOS
      ~SampleBatch.NEXT_OBS
      ~SampleBatch.OBS
      ~SampleBatch.OBS_EMBEDS
      ~SampleBatch.PREV_ACTIONS
      ~SampleBatch.PREV_REWARDS
      ~SampleBatch.RETURNS_TO_GO
      ~SampleBatch.REWARDS
      ~SampleBatch.SEQ_LENS
      ~SampleBatch.T
      ~SampleBatch.TERMINATEDS
      ~SampleBatch.TRUNCATEDS
      ~SampleBatch.UNROLL_ID
      ~SampleBatch.VALUES_BOOTSTRAPPED
      ~SampleBatch.VF_PREDS
      ~SampleBatch.is_training

   
   