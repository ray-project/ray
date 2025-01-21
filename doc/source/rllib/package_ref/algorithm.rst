.. include:: /_includes/rllib/we_are_hiring.rst

.. _algorithm-reference-docs:

Algorithms
==========

.. include:: /_includes/rllib/new_api_stack.rst

The :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class is the highest-level API in RLlib responsible for **WHEN** and **WHAT** of RL algorithms.
Things like **WHEN** should we sample the algorithm, **WHEN** should we perform a neural network update, and so on.
The **HOW** will be delegated to components such as ``RolloutWorker``, etc..

It is the main entry point for RLlib users to interact with RLlib's algorithms.

It allows you to train and evaluate policies, save an experiment's progress and restore from
a prior saved experiment when continuing an RL run.
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` is a sub-class
of :py:class:`~ray.tune.trainable.Trainable`
and thus fully supports distributed hyperparameter tuning for RL.

.. https://docs.google.com/drawings/d/1J0nfBMZ8cBff34e-nSPJZMM1jKOuUL11zFJm6CmWtJU/edit
.. figure:: ../images/trainer_class_overview.svg
    :align: left

    **A typical RLlib Algorithm object:** Algorithms are normally comprised of
    N ``RolloutWorkers`` that
    orchestrated via a :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` object.
    Each worker own its own a set of ``Policy`` objects and their NN models per worker, plus a :py:class:`~ray.rllib.env.base_env.BaseEnv` instance per worker.

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

* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.setup`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.get_default_config`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.get_default_policy_class`
* :py:meth:`~ray.rllib.algorithms.algorithm.Algorithm.training_step`

`See here for an example on how to override Algorithm <https://github.com/ray-project/ray/blob/master/rllib/algorithms/ppo/ppo.py>`_.


.. _rllib-algorithm-api:

Algorithm API
-------------

.. currentmodule:: ray.rllib.algorithms.algorithm

Construction and setup
~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm
    ~Algorithm.setup
    ~Algorithm.get_default_config


Attributes
~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.config
    ~Algorithm.metrics
    ~Algorithm.env_runner_group
    ~Algorithm.env_runner
    ~Algorithm.eval_env_runner_group
    ~Algorithm.eval_env_runner
    ~Algorithm.learner_group
    ~Algorithm.offline_data

Training
~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.train
    ~Algorithm.training_step

Saving and restoring
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.save_to_path
    ~Algorithm.restore_from_path
    ~Algorithm.from_checkpoint
    ~Algorithm.get_state
    ~Algorithm.set_state


Evaluation
~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.evaluate

Multi Agent
~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.get_module
    ~Algorithm.add_policy
    ~Algorithm.remove_policy
