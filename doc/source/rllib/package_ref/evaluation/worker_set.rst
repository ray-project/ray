.. _workerset-reference-docs:

WorkerSet
=========

A set of RolloutWorkers containing n ``@ray.remote`` workers as well as a single "local"
RolloutWorker. WorkerSets expose some convenience methods to make calls on its individual
workers' own methods in parallel using e.g. ``ray.get()``.

.. autoclass:: ray.rllib.evaluation.worker_set.WorkerSet
    :special-members: __init__
    :members:

