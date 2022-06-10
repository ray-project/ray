.. algorithm-reference-docs:

Algorithm API
=============

The :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class is the highest-level API in RLlib.
It allows you to train and evaluate policies, save an experiment's progress and restore from
a prior saved experiment when continuing an RL run.
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` is a sub-class
of :py:class:`~ray.tune.Trainable`
and thus fully supports distributed hyperparameter tuning for RL.

.. https://docs.google.com/drawings/d/1J0nfBMZ8cBff34e-nSPJZMM1jKOuUL11zFJm6CmWtJU/edit
.. figure:: ../images/trainer_class_overview.svg
    :align: left

    **A typical RLlib Algorithm object:** The components sitting inside an Algorithm are
    normally N :py:class:`~ray.rllib.evaluation.worker_set.WorkerSet`s
    (each consisting of one local :py:class:`~ray.rllib.evaluation.RolloutWorker`
    and zero or more \@ray.remote
    :py:class:`~ray.rllib.evaluation.RolloutWorker`s),
    a set of :py:class:`~ray.rllib.policy.Policy`(ies)
    and their NN models per worker, and a (already vectorized)
    RLlib :py:class:`~ray.rllib.env.base_env.BaseEnv` per worker.


Building Custom Algorithm Classes
---------------------------------

.. warning::
    As of Ray >= 1.9, it is no longer recommended to use the `build_trainer()` utility
    function for creating custom Algorithm sub-classes.
    Instead, follow the simple guidelines here for directly sub-classing from
    :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.

In order to create a custom Algorithm, sub-class the
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class
and override one or more of its methods. Those are in particular:

* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.get_default_config`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.validate_config`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.get_default_policy_class`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.setup`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.training_iteration`

`See here for an example on how to override Algorithm <https://github.com/ray-project/ray/blob/master/rllib/algorithms/pg/pg.py>`_.


Algorithm base class (ray.rllib.algorithms.algorithm.Algorithm)
---------------------------------------------------------------

.. autoclass:: ray.rllib.algorithms.algorithm.Algorithm
    :special-members: __init__
    :members:

..  static-members: get_default_config, execution_plan
