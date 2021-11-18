.. _trainer-reference-docs:

Trainer API
===========

The ``Trainer`` class is the highest-level API in RLlib. It allows you to
train and evaluate policies, save an experiment's progress and restore from
a prior saved experiment when continuing an RL run. ``Trainer`` is a sub-class
of ``tune.Trainable`` and thus fully supports distributed hyperparameter tuning
for RL.

.. https://docs.google.com/drawings/d/1J0nfBMZ8cBff34e-nSPJZMM1jKOuUL11zFJm6CmWtJU/edit
.. figure:: ../../images/rllib/trainer_class_overview.svg
    :align: left

    **A typical RLlib Trainer object:** The components sitting inside a Trainer are
    normally: N ``WorkerSets`` (consisting of 1 local ``RolloutWorker`` and 0 or
    more @ray.remote ``RolloutWorkers``),
    one set of ``Policy/ies`` and their models per worker, and a (vectorized)
    RLlib ``BaseEnv`` per worker.


Building Custom Trainer Classes
+++++++++++++++++++++++++++++++

.. warning::
    It is not recommended anymore to use the `build_trainer()` utility function
    for creating custom Trainer sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    ``Trainer``.

In order to create a custom Trainer, simply sub-class the ``Trainer`` class
and override one or more of its methods. Those are in particular:

* get_default_policy_class
* get_default_config
* setup
* step_attempt
* execution_plan

`See here for a simple example on how to override Trainer <https://github.com/ray-project/ray/blob/master/rllib/agents/pg/pg.py>`_.


Trainer base class (ray.rllib.agents.trainer.Trainer)
+++++++++++++++++++++++++++++++++++++++++++++++++++++

.. autoclass:: ray.rllib.agents.trainer.Trainer
    :special-members: __init__
    :members:

