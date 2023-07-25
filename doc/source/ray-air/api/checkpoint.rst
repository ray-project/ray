.. _air-checkpoint-ref:
.. _checkpoint-api-ref:

Ray AIR Checkpoint
==================

.. seealso::

    See :ref:`this API reference section <train-framework-specific-ckpts>` for
    framework-specific checkpoints used with AIR's library integrations.

Constructor Options
-------------------

.. currentmodule:: ray.air.checkpoint

.. autosummary::
    :toctree: doc/

    Checkpoint

.. autosummary::
    :toctree: doc/

    Checkpoint.from_dict
    Checkpoint.from_bytes
    Checkpoint.from_directory
    Checkpoint.from_uri
    Checkpoint.from_checkpoint

Checkpoint Properties
---------------------

.. autosummary::
    :toctree: doc/

    Checkpoint.uri
    Checkpoint.get_internal_representation
    Checkpoint.get_preprocessor
    Checkpoint.set_preprocessor

Checkpoint Format Conversions
-----------------------------

.. autosummary::
    :toctree: doc/

    Checkpoint.to_dict
    Checkpoint.to_bytes
    Checkpoint.to_directory
    Checkpoint.as_directory
    Checkpoint.to_uri
