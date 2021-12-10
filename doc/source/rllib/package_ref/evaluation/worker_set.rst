.. _workerset-reference-docs:

WorkerSet
=========

A set of :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` containing n ray remote workers as well as a single "local"
:py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` .

:py:class:`~ray.rllib.evaluation.worker_set.WorkerSet` exposes some convenience methods to make calls on its individual
workers' own methods in parallel using e.g. ``ray.get()``.

.. autoclass:: ray.rllib.evaluation.worker_set.WorkerSet
    :special-members: __init__
    :members:
