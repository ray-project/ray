

ray.rllib.env.env\_runner\_group.EnvRunnerGroup
===============================================

.. currentmodule:: ray.rllib.env.env_runner_group

.. autoclass:: EnvRunnerGroup
   :show-inheritance:

   
   
   .. rubric:: Methods

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~EnvRunnerGroup.__init__
      ~EnvRunnerGroup.add_policy
      ~EnvRunnerGroup.add_workers
      ~EnvRunnerGroup.fetch_ready_async_reqs
      ~EnvRunnerGroup.foreach_env
      ~EnvRunnerGroup.foreach_env_with_context
      ~EnvRunnerGroup.foreach_policy
      ~EnvRunnerGroup.foreach_policy_to_train
      
      ~EnvRunnerGroup.foreach_worker
      ~EnvRunnerGroup.foreach_worker_async
      ~EnvRunnerGroup.foreach_worker_with_id
      ~EnvRunnerGroup.healthy_worker_ids
      ~EnvRunnerGroup.is_policy_to_train
      ~EnvRunnerGroup.local_worker
      ~EnvRunnerGroup.num_healthy_remote_workers
      ~EnvRunnerGroup.num_healthy_workers
      ~EnvRunnerGroup.num_in_flight_async_reqs
      ~EnvRunnerGroup.num_remote_env_runners
      ~EnvRunnerGroup.num_remote_worker_restarts
      ~EnvRunnerGroup.num_remote_workers
      ~EnvRunnerGroup.probe_unhealthy_workers
      
      ~EnvRunnerGroup.reset
      ~EnvRunnerGroup.stop
      ~EnvRunnerGroup.sync_env_runner_states
      ~EnvRunnerGroup.sync_weights

   
   


   
   
   .. rubric:: Attributes

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~EnvRunnerGroup.local_env_runner

   
   