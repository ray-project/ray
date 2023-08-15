
Ray AIR Configurations
======================

.. TODO(ml-team): Add a general configuration guide that covers all of these configs.

.. note::

    We are changing the import path of the configurations classes from `ray.air` to `ray.train` starting from Ray 2.7, 
    please see the :ref:`Ray Train API reference <ray-train-configs-api>` for the latest APIs.

.. currentmodule:: ray.train

.. autosummary::

    RunConfig
    ScalingConfig
    CheckpointConfig
    FailureConfig

.. currentmodule:: ray

.. autosummary::

    tune.TuneConfig
    tune.syncer.SyncConfig
