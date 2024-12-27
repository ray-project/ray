.. include:: /_includes/rllib/we_are_hiring.rst

.. include:: /_includes/rllib/new_api_stack.rst

.. _algorithm-config-reference-docs:

Algorithm Configuration API
----------------------------

The :py:class:`~ray.rllib.algorithms.algorithm_config.AlgorithmConfig` class represents
the user's gateway into configuring and building an RLlib :py:class:`~ray.rllib.algorithms.algorithm.Algorithm`.

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


Builder methods
~~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.build_algo
    ~AlgorithmConfig.build_learner_group
    ~AlgorithmConfig.build_learner


Configuration methods
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.callbacks
    ~AlgorithmConfig.debugging
    ~AlgorithmConfig.env_runners
    ~AlgorithmConfig.environment
    ~AlgorithmConfig.evaluation
    ~AlgorithmConfig.experimental
    ~AlgorithmConfig.fault_tolerance
    ~AlgorithmConfig.framework
    ~AlgorithmConfig.learners
    ~AlgorithmConfig.multi_agent
    ~AlgorithmConfig.offline_data
    ~AlgorithmConfig.python_environment
    ~AlgorithmConfig.reporting
    ~AlgorithmConfig.resources
    ~AlgorithmConfig.rl_module
    ~AlgorithmConfig.training


Properties
~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.is_multi_agent
    ~AlgorithmConfig.is_offline
    ~AlgorithmConfig.learner_class
    ~AlgorithmConfig.model_config
    ~AlgorithmConfig.rl_module_spec
    ~AlgorithmConfig.total_train_batch_size

Getter methods
~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.get_default_learner_class
    ~AlgorithmConfig.get_default_rl_module_spec
    ~AlgorithmConfig.get_evaluation_config_object
    ~AlgorithmConfig.get_multi_rl_module_spec
    ~AlgorithmConfig.get_multi_agent_setup
    ~AlgorithmConfig.get_rollout_fragment_length


Public methods
~~~~~~~~~~~~~~
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.copy
    ~AlgorithmConfig.validate
    ~AlgorithmConfig.freeze
