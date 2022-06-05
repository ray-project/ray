.. _rolloutworker-reference-docs:

RolloutWorker
=============

RolloutWorkers are used as ``@ray.remote`` actors to collect and return samples
from environments or offline files in parallel. An RLlib :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` usually has
``num_workers`` :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`s plus a
single "local" :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` (not ``@ray.remote``) in
its :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` under ``self.workers``.

Depending on its evaluation config settings, an additional :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` with
:py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`s for evaluation may be present in the :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`
under ``self.evaluation_workers``.

.. autoclass:: ray.rllib.evaluation.rollout_worker.RolloutWorker
    :special-members: __init__
    :members:
