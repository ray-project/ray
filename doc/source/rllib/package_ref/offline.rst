.. include:: /_includes/rllib/we_are_hiring.rst

.. _new-api-offline-reference-docs:

Offline RL API
==============

.. include:: /_includes/rllib/new_api_stack.rst

Configuring Offline RL
----------------------

.. currentmodule:: ray.rllib.algorithms.algorithm_config

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.offline_data
    AlgorithmConfig.learners

Configuring Offline Recording EnvRunners
----------------------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    AlgorithmConfig.env_runners

Constructing a Recording EnvRunner
----------------------------------

.. currentmodule:: ray.rllib.offline.offline_env_runner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflineSingleAgentEnvRunner

Constructing OfflineData
------------------------

.. currentmodule:: ray.rllib.offline.offline_data

.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflineData
    OfflineData.__init__

Sampling from Offline Data
--------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflineData.sample
    OfflineData.default_map_batches_kwargs
    OfflineData.default_iter_batches_kwargs

Constructing an OfflinePreLearner
---------------------------------

.. currentmodule:: ray.rllib.offline.offline_prelearner

.. autosummary::
    :nosignatures:
    :toctree: doc/

    OfflinePreLearner
    OfflinePreLearner.__init__

Transforming Data with an OfflinePreLearner
-------------------------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    SCHEMA
    OfflinePreLearner.__call__
    OfflinePreLearner._map_to_episodes
    OfflinePreLearner._map_sample_batch_to_episode
    OfflinePreLearner._should_module_be_updated
    OfflinePreLearner.default_prelearner_buffer_class
    OfflinePreLearner.default_prelearner_buffer_kwargs
