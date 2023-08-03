.. _evaluation-reference-docs:

Sampling the Environment or offline data
========================================

Data ingest via either environment rollouts or other data-generating methods
(e.g. reading from offline files) is done in RLlib by :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` instances,
which sit inside a :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet`
(together with other parallel ``RolloutWorkers``) in the RLlib :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
(under the ``self.workers`` property):


.. https://docs.google.com/drawings/d/1OewMLAu6KZNon7zpDfZnTh9qiT6m-3M9wnkqWkQQMRc/edit
.. figure:: ../images/rollout_worker_class_overview.svg
    :width: 600
    :align: left

    **A typical RLlib WorkerSet setup inside an RLlib Algorithm:** Each :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` contains
    exactly one local :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` object and N ray remote
    :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` (ray actors).
    The workers contain a policy map (with one or more policies), and - in case a simulator
    (env) is available - a vectorized :py:class:`~ray.rllib.env.base_env.BaseEnv`
    (containing M sub-environments) and a :py:class:`~ray.rllib.evaluation.sampler.SamplerInput` (either synchronous or asynchronous) which controls
    the environment data collection loop.
    In the online case (i.e. environment is available) as well as the offline case (i.e. no environment),
    :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` uses the :py:meth:`~ray.rllib.evaluation.rollout_worker.RolloutWorker.sample` method to
    get :py:class:`~ray.rllib.policy.sample_batch.SampleBatch` objects for training.


.. _rolloutworker-reference-docs:

RolloutWorker API
-----------------

.. currentmodule:: ray.rllib.evaluation.rollout_worker

Constructor
~~~~~~~~~~~


.. autosummary::
   :toctree: doc/

   RolloutWorker

Multi agent
~~~~~~~~~~~

.. autosummary::
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
   :toctree: doc/

    ~RolloutWorker.lock
    ~RolloutWorker.unlock

Sampling API
~~~~~~~~~~~~

.. autosummary::
   :toctree: doc/

    ~RolloutWorker.sample
    ~RolloutWorker.sample_with_count
    ~RolloutWorker.sample_and_learn

Training API
~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~RolloutWorker.learn_on_batch
    ~RolloutWorker.setup_torch_data_parallel
    ~RolloutWorker.compute_gradients
    ~RolloutWorker.apply_gradients

Environment API
~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~RolloutWorker.foreach_env
    ~RolloutWorker.foreach_env_with_context


Miscellaneous
~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~RolloutWorker.stop
    ~RolloutWorker.apply
    ~RolloutWorker.sync_filters
    ~RolloutWorker.find_free_port
    ~RolloutWorker.creation_args
    ~RolloutWorker.assert_healthy


.. _workerset-reference-docs:

WorkerSet API
-------------

.. currentmodule:: ray.rllib.evaluation.worker_set

Constructor
~~~~~~~~~~~


.. autosummary::
    :toctree: doc/

    WorkerSet
    WorkerSet.stop
    WorkerSet.reset


Worker Orchestration
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~WorkerSet.add_workers
    ~WorkerSet.foreach_worker
    ~WorkerSet.foreach_worker_with_id
    ~WorkerSet.foreach_worker_async
    ~WorkerSet.fetch_ready_async_reqs
    ~WorkerSet.num_in_flight_async_reqs
    ~WorkerSet.local_worker
    ~WorkerSet.remote_workers
    ~WorkerSet.num_healthy_remote_workers
    ~WorkerSet.num_healthy_workers
    ~WorkerSet.num_remote_worker_restarts
    ~WorkerSet.probe_unhealthy_workers

Pass-through methods
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :toctree: doc/

    ~WorkerSet.add_policy
    ~WorkerSet.foreach_env
    ~WorkerSet.foreach_env_with_context
    ~WorkerSet.foreach_policy
    ~WorkerSet.foreach_policy_to_train
    ~WorkerSet.sync_weights



Sampler API
-----------
:py:class:`~ray.rllib.offline.input_reader.InputReader` instances are used to collect and return experiences from the envs.
For more details on `InputReader` used for offline RL (e.g. reading files of
pre-recorded data), see the :ref:`offline RL API reference here <offline-reference-docs>`.




Input Reader API
~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.offline.input_reader

.. autosummary::
    :toctree: doc/

    InputReader
    InputReader.next


Input Sampler API
~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.evaluation.sampler

.. autosummary::
    :toctree: doc/

    SamplerInput
    SamplerInput.get_data
    SamplerInput.get_extra_batches
    SamplerInput.get_metrics

Synchronous Sampler API
~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.evaluation.sampler

.. autosummary::
    :toctree: doc/

    SyncSampler


Asynchronous Sampler API
~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: ray.rllib.evaluation.sampler

.. autosummary::
    :toctree: doc/

    AsyncSampler


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
    :toctree: doc/

    JsonReader
    JsonReader.read_all_files

.. currentmodule:: ray.rllib.offline.mixed_input

Mixed input reader
++++++++++++++++++
.. autosummary::
    :toctree: doc/

    MixedInput

.. currentmodule:: ray.rllib.offline.d4rl_reader

D4RL reader
+++++++++++
.. autosummary::
    :toctree: doc/

    D4RLReader

.. currentmodule:: ray.rllib.offline.io_context

IOContext
~~~~~~~~~
.. autosummary::
    :toctree: doc/

    IOContext
    IOContext.default_sampler_input



Policy Map API
--------------

.. currentmodule:: ray.rllib.policy.policy_map

.. autosummary::
    :toctree: doc/

    PolicyMap
    PolicyMap.items
    PolicyMap.keys
    PolicyMap.values

Sample batch API
----------------

.. currentmodule:: ray.rllib.policy.sample_batch

.. autosummary::
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
    :toctree: doc/

    MultiAgentBatch
    MultiAgentBatch.env_steps
    MultiAgentBatch.agent_steps


