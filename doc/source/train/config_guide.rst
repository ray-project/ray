.. _train-config:

Configurations User Guide
=========================

The following overviews how to configure scale-out, run options, and fault-tolerance for Train.
For more details on how to configure data ingest, also refer to :ref:`air-ingest`.

Scaling configuration (``ScalingConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The scaling configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the scaling configuration are :ref:`tunable <air-tuner-search-space>`.

:class:`ScalingConfig API reference <ray.air.config.ScalingConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __scaling_config_start__
    :end-before: __scaling_config_end__


Run configuration (``RunConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The run configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the run configuration are :ref:`not tunable <air-tuner-search-space>`.

:class:`RunConfig API reference <ray.air.config.RunConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __run_config_start__
    :end-before: __run_config_end__

Failure configuration (``FailureConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The failure configuration specifies how training failures should be dealt with.

As part of the RunConfig, the properties of the failure configuration
are :ref:`not tunable <air-tuner-search-space>`.

:class:`FailureConfig API reference <ray.air.config.FailureConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __failure_config_start__
    :end-before: __failure_config_end__

Sync configuration (``SyncConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The sync configuration specifies how to synchronize checkpoints between the
Ray cluster and remote storage.

As part of the RunConfig, the properties of the sync configuration
are :ref:`not tunable <air-tuner-search-space>`.

:class:`SyncConfig API reference <ray.tune.syncer.SyncConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __sync_config_start__
    :end-before: __sync_config_end__


Checkpoint configuration (``CheckpointConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The checkpoint configuration specifies how often to checkpoint training state
and how many checkpoints to keep.

As part of the RunConfig, the properties of the checkpoint configuration
are :ref:`not tunable <air-tuner-search-space>`.

:class:`CheckpointConfig API reference <ray.air.config.CheckpointConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_start__
    :end-before: __checkpoint_config_end__

