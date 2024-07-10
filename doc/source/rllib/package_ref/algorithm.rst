
.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. algorithm-reference-docs:

Algorithms
==========

The :py:class:`~ray.rllib.algorithms.algorithm.Algorithm` class is the highest-level API in RLlib responsible for **WHEN** and **WHAT** of RL algorithms. Things like **WHEN** should we sample the algorithm, **WHEN** should we perform a neural network update, and so on. The **HOW** will be delegated to components such as :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker`, etc.. It is the main entry point for RLlib users to interact with RLlib's algorithms.
It allows you to train and evaluate policies, save an experiment's progress and restore from
a prior saved experiment when continuing an RL run.
:py:class:`~ray.rllib.algorithms.algorithm.Algorithm` is a sub-class
of :py:class:`~ray.tune.trainable.Trainable`
and thus fully supports distributed hyperparameter tuning for RL.

.. https://docs.google.com/drawings/d/1J0nfBMZ8cBff34e-nSPJZMM1jKOuUL11zFJm6CmWtJU/edit
.. figure:: ../images/trainer_class_overview.svg
    :align: left

    **A typical RLlib Algorithm object:** Algorhtms are normally comprised of
    N :py:class:`~ray.rllib.evaluation.rollout_worker.RolloutWorker` that
    orchestrated via a :py:class:`~ray.rllib.env.env_runner_group.EnvRunnerGroup` object.
    Each worker own its own a set of :py:class:`~ray.rllib.policy.policy.Policy` objects and their NN models per worker, plus a :py:class:`~ray.rllib.env.base_env.BaseEnv` instance per worker.

.. _algo-config-api:

Algorithm Configuration API
----------------------------

The :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` class represents
the primary way of configuring and building an :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.
You don't use ``AlgorithmConfig`` directly in practice, but rather use its algorithm-specific
implementations such as :py:class:`~ray.rllib.algorithms.ppo.ppo.PPOConfig`, which each come
with their own set of arguments to their respective ``.training()`` method.

.. currentmodule:: ray.rllib.algorithms.algorithm_config

Constructor
~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig


Public methods
~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.copy
    ~AlgorithmConfig.validate
    ~AlgorithmConfig.freeze

Builder methods
~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.build
    ~AlgorithmConfig.build_learner_group
    ~AlgorithmConfig.build_learner

Configuration methods
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.callbacks
    ~AlgorithmConfig.debugging
    ~AlgorithmConfig.environment
    ~AlgorithmConfig.evaluation
    ~AlgorithmConfig.experimental
    ~AlgorithmConfig.fault_tolerance
    ~AlgorithmConfig.framework
    ~AlgorithmConfig.multi_agent
    ~AlgorithmConfig.offline_data
    ~AlgorithmConfig.python_environment
    ~AlgorithmConfig.reporting
    ~AlgorithmConfig.resources
    ~AlgorithmConfig.rl_module
    ~AlgorithmConfig.rollouts
    ~AlgorithmConfig.training

Getter methods
~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.get_default_learner_class
    ~AlgorithmConfig.get_default_rl_module_spec
    ~AlgorithmConfig.get_evaluation_config_object
    ~AlgorithmConfig.get_marl_module_spec
    ~AlgorithmConfig.get_multi_agent_setup
    ~AlgorithmConfig.get_rollout_fragment_length

Miscellaneous methods
~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.validate_train_batch_size_vs_rollout_fragment_length



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

Constructor
~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm
    ~Algorithm.setup
    ~Algorithm.get_default_config

Inference and Evaluation
~~~~~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.compute_actions
    ~Algorithm.compute_single_action
    ~Algorithm.evaluate

Saving and Restoring
~~~~~~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.from_checkpoint
    ~Algorithm.from_state
    ~Algorithm.get_weights
    ~Algorithm.set_weights
    ~Algorithm.export_model
    ~Algorithm.export_policy_checkpoint
    ~Algorithm.export_policy_model
    ~Algorithm.restore
    ~Algorithm.restore_workers
    ~Algorithm.save
    ~Algorithm.save_checkpoint


Training
~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.train
    ~Algorithm.training_step

Multi Agent
~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~Algorithm.add_policy
    ~Algorithm.remove_policy
