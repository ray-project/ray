Syncing in Tune (tune.SyncConfig, tune.Syncer)
==============================================

.. _tune-syncconfig:

.. currentmodule:: ray.tune.syncer

Tune Syncing Configuration
--------------------------

.. autosummary::
    :toctree: doc/

    SyncConfig

.. _tune-syncer:

Remote Storage Syncer Interface (tune.Syncer)
---------------------------------------------

.. autosummary::
    :toctree: doc/

    Syncer
    Syncer.sync_up
    Syncer.sync_down
    Syncer.delete
    Syncer.wait
    Syncer.wait_or_retry


Tune Built-in Syncers
---------------------

.. autosummary::
    :toctree: doc/

    SyncerCallback
    _DefaultSyncer
    _BackgroundSyncer

