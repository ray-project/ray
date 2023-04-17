Syncing in Tune (tune.SyncConfig, tune.Syncer)
==============================================

.. seealso::

    See :doc:`this user guide </tune/tutorials/tune-storage>` for more details and examples.


.. currentmodule:: ray.tune.syncer

.. _tune-sync-config:

Tune Syncing Configuration
--------------------------

.. autosummary::
    :toctree: doc/

    SyncConfig

.. _tune-syncer:

Remote Storage Syncer Interface (tune.Syncer)
---------------------------------------------

Constructor
~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    Syncer


Syncer Methods to Implement
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

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

