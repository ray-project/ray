

ray.rllib.algorithms.algorithm\_config.AlgorithmConfig
======================================================

.. currentmodule:: ray.rllib.algorithms.algorithm_config

.. autoclass:: AlgorithmConfig
   :show-inheritance:

   
   
   .. rubric:: Methods

   .. autosummary::
      :nosignatures:
      :toctree:

   
      
      
      ~AlgorithmConfig.__init__
      ~AlgorithmConfig.api_stack
      ~AlgorithmConfig.build
      
      ~AlgorithmConfig.build_learner
      
      ~AlgorithmConfig.build_learner_group
      
      ~AlgorithmConfig.callbacks
      ~AlgorithmConfig.checkpointing
      ~AlgorithmConfig.copy
      ~AlgorithmConfig.debugging
      ~AlgorithmConfig.env_runners
      ~AlgorithmConfig.environment
      ~AlgorithmConfig.evaluation
      ~AlgorithmConfig.experimental
      
      ~AlgorithmConfig.fault_tolerance
      ~AlgorithmConfig.framework
      ~AlgorithmConfig.freeze
      ~AlgorithmConfig.from_dict
      ~AlgorithmConfig.get
      ~AlgorithmConfig.get_config_for_module
      ~AlgorithmConfig.get_default_learner_class
      ~AlgorithmConfig.get_default_rl_module_spec
      ~AlgorithmConfig.get_evaluation_config_object
      ~AlgorithmConfig.get_marl_module_spec
      ~AlgorithmConfig.get_multi_agent_setup
      ~AlgorithmConfig.get_rollout_fragment_length
      ~AlgorithmConfig.get_torch_compile_worker_config
      ~AlgorithmConfig.is_multi_agent
      ~AlgorithmConfig.items
      ~AlgorithmConfig.keys
      ~AlgorithmConfig.learners
      ~AlgorithmConfig.multi_agent
      ~AlgorithmConfig.offline_data
      ~AlgorithmConfig.overrides
      ~AlgorithmConfig.pop
      ~AlgorithmConfig.python_environment
      ~AlgorithmConfig.reporting
      ~AlgorithmConfig.resources
      ~AlgorithmConfig.rl_module
      
      ~AlgorithmConfig.serialize
      ~AlgorithmConfig.to_dict
      ~AlgorithmConfig.training
      ~AlgorithmConfig.update_from_dict
      ~AlgorithmConfig.validate
      ~AlgorithmConfig.validate_train_batch_size_vs_rollout_fragment_length
      ~AlgorithmConfig.values

   
   


   
   
   .. rubric:: Attributes

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~AlgorithmConfig.custom_resources_per_worker
      ~AlgorithmConfig.delay_between_worker_restarts_s
      ~AlgorithmConfig.evaluation_num_workers
      ~AlgorithmConfig.ignore_worker_failures
      ~AlgorithmConfig.is_atari
      ~AlgorithmConfig.learner_class
      ~AlgorithmConfig.max_num_worker_restarts
      ~AlgorithmConfig.model_config
      ~AlgorithmConfig.num_consecutive_worker_failures_tolerance
      ~AlgorithmConfig.num_cpus_for_local_worker
      ~AlgorithmConfig.num_cpus_per_learner_worker
      ~AlgorithmConfig.num_cpus_per_worker
      ~AlgorithmConfig.num_envs_per_worker
      ~AlgorithmConfig.num_gpus_per_learner_worker
      ~AlgorithmConfig.num_gpus_per_worker
      ~AlgorithmConfig.num_learner_workers
      ~AlgorithmConfig.num_rollout_workers
      ~AlgorithmConfig.recreate_failed_workers
      ~AlgorithmConfig.rl_module_spec
      ~AlgorithmConfig.total_train_batch_size
      ~AlgorithmConfig.uses_new_env_runners
      ~AlgorithmConfig.validate_workers_after_construction
      ~AlgorithmConfig.worker_health_probe_timeout_s
      ~AlgorithmConfig.worker_restore_timeout_s

   
   