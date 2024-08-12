
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst


.. _evaluation-reference-docs:

Sampling the Environment or offline data
========================================

Data ingest via either environment rollouts or other data-generating methods
(e.g. reading from offline files) is done in RLlib by :py:class:`~ray.rllib.env.env_runner.EnvRunner` instances,
which sit inside a :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup`
(together with other parallel ``EnvRunners``) in the RLlib :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
(under the ``self.env_runner_group`` property):


.. https://docs.google.com/drawings/d/1OewMLAu6KZNon7zpDfZnTh9qiT6m-3M9wnkqWkQQMRc/edit
.. figure:: ../images/rollout_worker_class_overview.svg
    :width: 600
    :align: left

    **A typical RLlib EnvRunnerGroup setup inside an RLlib Algorithm:** Each :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` contains
    exactly one local :py:class:`~ray.rllib.env.env_runner.EnvRunner` object and N ray remote
    :py:class:`~ray.rllib.env.env_runner.EnvRunner` (Ray actors).
    The workers contain a policy map (with one or more policies), and - in case a simulator
    (env) is available - a vectorized :py:class:`~ray.rllib.env.base_env.BaseEnv`
    (containing M sub-environments) and a :py:class:`~ray.rllib.evaluation.sampler.SamplerInput` (either synchronous or asynchronous) which controls
    the environment data collection loop.
    In the online case (i.e. environment is available) as well as the offline case (i.e. no environment),
    :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` uses the :py:meth:`~ray.rllib.env.env_runner.EnvRunner.sample` method to
    get :py:class:`~ray.rllib.policy.sample_batch.SampleBatch` objects for training.


.. _rolloutworker-reference-docs:

RolloutWorker API
-----------------

.. currentmodule:: ray.rllib.evaluation.rollout_worker

Constructor
~~~~~~~~~~~


.. autosummary::
   :nosignatures:
   :toctree: doc/

   RolloutWorker

Multi agent
~~~~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~RolloutWorker.add_policy
    ~RolloutWorker.remove_policy
    ~RolloutWorker.get_policy
    ~RolloutWorker.set_is_policy_to_train
    ~RolloutWorker.set_policy_mapping_fn
    ~RolloutWorker.for_policy
    ~RolloutWorker.foreach_policy
    ~RolloutWorker.foreach_policy_to_train

Setter and getter methods
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~RolloutWorker.get_filters
    ~RolloutWorker.get_global_vars
    ~RolloutWorker.set_global_vars
    ~RolloutWorker.get_host
    ~RolloutWorker.get_metrics
    ~RolloutWorker.get_node_ip
    ~RolloutWorker.get_weights
    ~RolloutWorker.set_weights
    ~RolloutWorker.get_state
    ~RolloutWorker.set_state

Threading
~~~~~~~~~
.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~RolloutWorker.lock
    ~RolloutWorker.unlock

Sampling API
~~~~~~~~~~~~

.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~RolloutWorker.sample
    ~RolloutWorker.sample_with_count
    ~RolloutWorker.sample_and_learn

Training API
~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RolloutWorker.learn_on_batch
    ~RolloutWorker.setup_torch_data_parallel
    ~RolloutWorker.compute_gradients
    ~RolloutWorker.apply_gradients

Environment API
~~~~~~~~~~~~~~~


.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RolloutWorker.foreach_env
    ~RolloutWorker.foreach_env_with_context


Miscellaneous
~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~RolloutWorker.stop
    ~RolloutWorker.apply
    ~RolloutWorker.sync_filters
    ~RolloutWorker.find_free_port
    ~RolloutWorker.creation_args
    ~RolloutWorker.assert_healthy


.. _workerset-reference-docs:

EnvRunner API
-------------

.. currentmodule:: ray.rllib.env.env_runner

.. autosummary::
   :nosignatures:
   :toctree: doc/

   EnvRunner

EnvRunnerGroup API
------------------

.. currentmodule:: ray.rllib.env.env_runner_group

Constructor
~~~~~~~~~~~


.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunnerGroup
    EnvRunnerGroup.stop
    EnvRunnerGroup.reset


Worker Orchestration
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~EnvRunnerGroup.add_workers
    ~EnvRunnerGroup.foreach_worker
    ~EnvRunnerGroup.foreach_worker_with_id
    ~EnvRunnerGroup.foreach_worker_async
    ~EnvRunnerGroup.fetch_ready_async_reqs
    ~EnvRunnerGroup.num_in_flight_async_reqs
    ~EnvRunnerGroup.local_worker
    ~EnvRunnerGroup.remote_workers
    ~EnvRunnerGroup.num_healthy_remote_workers
    ~EnvRunnerGroup.num_healthy_workers
    ~EnvRunnerGroup.num_remote_worker_restarts
    ~EnvRunnerGroup.probe_unhealthy_workers

Pass-through methods
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~EnvRunnerGroup.add_policy
    ~EnvRunnerGroup.foreach_env
    ~EnvRunnerGroup.foreach_env_with_context
    ~EnvRunnerGroup.foreach_policy
    ~EnvRunnerGroup.foreach_policy_to_train
    ~EnvRunnerGroup.sync_weights



Sampler API
-----------
:py:class:`~ray.rllib.offline.input_reader.InputReader` instances are used to collect and return experiences from the envs.
For more details on `InputReader` used for offline RL (e.g. reading files of
pre-recorded data), see the :ref:`offline RL API reference here <offline-reference-docs>`.




Input Reader API
~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.offline.input_reader

.. autosummary::
    :nosignatures:
    :toctree: doc/

    InputReader
    InputReader.next


Input Sampler API
~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.evaluation.sampler

.. autosummary::
    :nosignatures:
    :toctree: doc/

    SamplerInput
    SamplerInput.get_data
    SamplerInput.get_extra_batches
    SamplerInput.get_metrics

Synchronous Sampler API
~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.evaluation.sampler

.. autosummary::
    :nosignatures:
    :toctree: doc/

    SyncSampler



.. _offline-reference-docs:

Offline Sampler API
~~~~~~~~~~~~~~~~~~~~~~~

The InputReader API is used by an individual :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`
to produce batches of experiences either from an simulator or from an
offline source (e.g. a file).

Here are some example extentions of the InputReader API:

JSON reader API
++++++++++++++++

.. currentmodule:: ray.rllib.offline.json_reader

.. autosummary::
    :nosignatures:
    :toctree: doc/

    JsonReader
    JsonReader.read_all_files

.. currentmodule:: ray.rllib.offline.mixed_input

Mixed input reader
++++++++++++++++++
.. autosummary::
    :nosignatures:
    :toctree: doc/

    MixedInput

.. currentmodule:: ray.rllib.offline.d4rl_reader

D4RL reader
+++++++++++
.. autosummary::
    :nosignatures:
    :toctree: doc/

    D4RLReader

.. currentmodule:: ray.rllib.offline.io_context

IOContext
~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    IOContext
    IOContext.default_sampler_input



Policy Map API
--------------

.. currentmodule:: ray.rllib.policy.policy_map

.. autosummary::
    :nosignatures:
    :toctree: doc/

    PolicyMap
    PolicyMap.items
    PolicyMap.keys
    PolicyMap.values

Sample batch API
----------------

.. currentmodule:: ray.rllib.policy.sample_batch

.. autosummary::
    :nosignatures:
    :toctree: doc/

    SampleBatch
    SampleBatch.set_get_interceptor
    SampleBatch.is_training
    SampleBatch.set_training
    SampleBatch.as_multi_agent
    SampleBatch.get
    SampleBatch.to_device
    SampleBatch.right_zero_pad
    SampleBatch.slice
    SampleBatch.split_by_episode
    SampleBatch.shuffle
    SampleBatch.columns
    SampleBatch.rows
    SampleBatch.copy
    SampleBatch.is_single_trajectory
    SampleBatch.is_terminated_or_truncated
    SampleBatch.env_steps
    SampleBatch.agent_steps


MultiAgent batch API
--------------------

.. currentmodule:: ray.rllib.policy.sample_batch

.. autosummary::
    :nosignatures:
    :toctree: doc/

    MultiAgentBatch
    MultiAgentBatch.env_steps
    MultiAgentBatch.agent_steps


