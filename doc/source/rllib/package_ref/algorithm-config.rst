.. include:: /_includes/rllib/we_are_hiring.rst

.. _algorithm-config-reference-docs:

Algorithm Configuration API
===========================

.. include:: /_includes/rllib/new_api_stack.rst

.. currentmodule:: ray.rllib.algorithms.algorithm_config

Constructor
-----------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig


Builder methods
---------------
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.build_algo
    ~AlgorithmConfig.build_learner_group
    ~AlgorithmConfig.build_learner


Properties
----------
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
--------------
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
--------------
.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~AlgorithmConfig.copy
    ~AlgorithmConfig.validate
    ~AlgorithmConfig.freeze


.. _rllib-algorithm-config-methods:

Configuration methods
---------------------

.. _rllib-config-env:

Configuring the RL Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.environment
    :noindex:


.. _rllib-config-training:

Configuring training behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.training
    :noindex:


.. _rllib-config-env-runners:

Configuring `EnvRunnerGroup` and `EnvRunner` actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.env_runners
    :noindex:

.. _rllib-config-learners:

Configuring `LearnerGroup` and `Learner` actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.learners
    :noindex:

.. _rllib-config-callbacks:

Configuring custom callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.callbacks
    :noindex:

.. _rllib-config-multi_agent:

Configuring multi-agent specific settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.multi_agent
    :noindex:

.. _rllib-config-offline_data:

Configuring offline RL specific settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.offline_data
    :noindex:

.. _rllib-config-evaluation:

Configuring evaluation settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.evaluation
    :noindex:

.. _rllib-config-framework:

Configuring deep learning framework settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.framework
    :noindex:

.. _rllib-config-reporting:

Configuring reporting settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.reporting
    :noindex:

.. _rllib-config-checkpointing:

Configuring checkpointing settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.checkpointing
    :noindex:

.. _rllib-config-debugging:

Configuring debugging settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.debugging
    :noindex:

.. _rllib-config-experimental:

Configuring experimental settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: ray.rllib.algorithms.algorithm_config.AlgorithmConfig.experimental
    :noindex:
