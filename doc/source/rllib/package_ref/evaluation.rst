.. _evaluation-reference-docs:

Evaluation/Rollout APIs
=======================

.. _rolloutworker-docs:

RolloutWorker
+++++++++++++

RolloutWorkers are used as ``@ray.remote`` actors to collect and return samples
from environments or offline files in parallel. An RLlib Trainer usually has
``num_workers`` RolloutWorkers plus a single "local" RolloutWorker (not ``@ray.remote``) in
its WorkerSet (`see below <WorkerSet>`_) under ``self.workers``.
Depending on its evaluation config settings, an additional ``WorkerSet`` with
RolloutWorkers for evaluation may be present in the Trainer under ``self.evaluation_workers``.

.. autoclass:: ray.rllib.evaluation.rollout_worker.RolloutWorker
    :special-members: __init__
    :members:

.. _workerset-docs:

WorkerSet
+++++++++

A set of RolloutWorkers containing n ``@ray.remote`` workers as well as a single "local"
RolloutWorker. WorkerSets expose some convenience methods to make calls on its individual
workers' own methods in parallel using e.g. ``ray.get()``.

.. autoclass:: ray.rllib.evaluation.worker_set.WorkerSet
    :special-members: __init__
    :members:


.. _sampler-docs:

Environment Samplers
++++++++++++++++++++

When a simulator (environment) is available, ``Samplers`` - child classes
of ``InputReader`` - are used to collect and return experiences from the envs.
For more details on InputReader used for offline RL (e.g. reading files of
pre-recorded data), see the :ref:`offline RL API reference here<offline-docs>`.

The base sampler API (SamplerInput) is defined as follows:

Base Sampler class (ray.rllib.evaluation.sampler.SamplerInput)
--------------------------------------------------------------


.. autoclass:: ray.rllib.evaluation.sampler.SamplerInput
    :members:

SyncSampler (ray.rllib.evaluation.sampler.SyncSampler)
------------------------------------------------------

The synchronous sampler starts stepping through and collecting samples from an
environment only when its ``next()`` method is called. Calling this method blocks
until a ``SampleBatch`` has been built and is returned.

.. autoclass:: ray.rllib.evaluation.sampler.SyncSampler
    :special-members: __init__
    :members:

AsyncSampler (ray.rllib.evaluation.sampler.AsyncSampler)
--------------------------------------------------------

The asynchronous sampler has a separate thread that keeps stepping through and
collecting samples from an environment in the background. Calling its ``next()`` method
gets the next enqueued SampleBatch from a queue and returns it immediately.

.. autoclass:: ray.rllib.evaluation.sampler.AsyncSampler
    :special-members: __init__
    :members:
