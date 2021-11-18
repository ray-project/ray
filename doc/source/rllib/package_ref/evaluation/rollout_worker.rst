.. _rolloutworker-reference-docs:

RolloutWorker
=============

RolloutWorkers are used as ``@ray.remote`` actors to collect and return samples
from environments or offline files in parallel. An RLlib Trainer usually has
``num_workers`` RolloutWorkers plus a single "local" RolloutWorker (not ``@ray.remote``) in
its WorkerSet (`see below <WorkerSet>`_) under ``self.workers``.
Depending on its evaluation config settings, an additional ``WorkerSet`` with
RolloutWorkers for evaluation may be present in the Trainer under ``self.evaluation_workers``.

.. autoclass:: ray.rllib.evaluation.rollout_worker.RolloutWorker
    :special-members: __init__
    :members:
