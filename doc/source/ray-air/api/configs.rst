
Ray AIR Configurations
======================

.. TODO(ml-team): Add a general AIR configuration guide that covers all of these configs.

.. currentmodule:: ray


.. note::

    We are changing the import path of the config classes from `ray.air` to `ray.train` since Ray 2.7, 
    please refer to the :ref:`Ray Train API page <ray-train-configs-api>` for more info.

.. autosummary::

    air.RunConfig
    air.ScalingConfig
    air.CheckpointConfig
    air.FailureConfig

.. autosummary::

    tune.TuneConfig
    tune.syncer.SyncConfig
