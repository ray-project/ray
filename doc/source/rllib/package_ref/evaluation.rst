.. _evaluation-reference-docs:

Evaluation/Rollout APIs
=======================

.. toctree::
   :maxdepth: 1

   evaluation/rollout_worker.rst
   evaluation/sample_batch.rst
   evaluation/worker_set.rst
   evaluation/samplers.rst
   evaluation/policy_map.rst

Data ingest via either environment rollouts or other data-generating methods
(e.g. reading from offline files) is done in RLlib by ``RolloutWorkers``, which sit
inside a ``WorkerSet`` (together with other parallel ``RolloutWorkers``) in the
RLlib ``Trainer`` (under the ``self.workers`` property):


.. https://docs.google.com/drawings/d/1OewMLAu6KZNon7zpDfZnTh9qiT6m-3M9wnkqWkQQMRc/edit
.. figure:: ../../images/rllib/rollout_worker_class_overview.svg
    :align: left

    **A typical RLlib WorkerSet setup inside an RLlib Trainer:** Each ``WorkerSet`` contains
    exactly one local RolloutWorker object and n @ray.remote ``RolloutWorkers`` (ray actors).
    The workers contain a policy map (with one or more policies), and - in case a simulator
    (env) is available - a vectorized BaseEnv (containing m sub-environments) and
    a ``Sampler`` (either synchronous or asynchronous) which controls the env data
    collection loop.
    In the online (env is available) as well as the offline case, ``Trainers`` use
    the ``RolloutWorkers`` `sample()` method to get ``SampleBatch`` objects for training.


