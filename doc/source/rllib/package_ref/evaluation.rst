.. _evaluation-reference-docs:

Evaluation and Environment Rollout
==================================

Data ingest via either environment rollouts or other data-generating methods
(e.g. reading from offline files) is done in RLlib by :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`s,
which sit inside a :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet`
(together with other parallel ``RolloutWorkers``) in the RLlib :py:class:`~ray.rllib.agents.trainer.Trainer`
(under the ``self.workers`` property):


.. https://docs.google.com/drawings/d/1OewMLAu6KZNon7zpDfZnTh9qiT6m-3M9wnkqWkQQMRc/edit
.. figure:: ../images/rollout_worker_class_overview.svg
    :width: 600
    :align: left

    **A typical RLlib WorkerSet setup inside an RLlib Trainer:** Each :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` contains
    exactly one local :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` object and n ray remote
    :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` (ray actors).
    The workers contain a policy map (with one or more policies), and - in case a simulator
    (env) is available - a vectorized :py:class:`~ray.rllib.env.base_env.BaseEnv`
    (containing m sub-environments) and a :py:class:`~ray.rllib.evaluation.sampler.SamplerInput` (either synchronous or asynchronous) which controls
    the environment data collection loop.
    In the online (environment is available) as well as the offline case (no environment),
    :py:class:`~ray.rllib.agents.trainer.Trainer` uses the :py:meth:`~ray.rllib.evaluation.rollout_worker.RolloutWorker.sample` method to
    get :py:class:`~ray.rllib.policy.sample_batch.SampleBatch` objects for training.


RolloutWorker- and related APIs
-------------------------------

.. toctree::
   :maxdepth: 1

   evaluation/rollout_worker.rst
   evaluation/sample_batch.rst
   evaluation/worker_set.rst
   evaluation/samplers.rst
   evaluation/policy_map.rst
