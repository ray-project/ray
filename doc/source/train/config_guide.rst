.. _train-config:

Ray Train Configuration User Guide
==================================

The following overviews how to configure scale-out, run options, and fault-tolerance for Train.
For more details on how to configure data ingest, also refer to :ref:`air-ingest`.

Scaling Configurations in Train (``ScalingConfig``)
---------------------------------------------------

The scaling configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the scaling configuration are :ref:`tunable <tune-search-space-tutorial>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __scaling_config_start__
    :end-before: __scaling_config_end__

.. seealso::

    See the :class:`~ray.air.ScalingConfig` API reference.

.. _train-run-config:

Run Configuration in Train (``RunConfig``)
------------------------------------------

``RunConfig`` is a configuration object used in Ray Train to define the experiment
spec that corresponds to a call to ``trainer.fit()``.

It includes settings such as the experiment name, storage path for results,
stopping conditions, custom callbacks, checkpoint configuration, verbosity level,
and logging options.

Many of these settings are configured through other config objects and passed through
the ``RunConfig``. The following sub-sections contain descriptions of these configs.

The properties of the run configuration are :ref:`not tunable <tune-search-space-tutorial>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __run_config_start__
    :end-before: __run_config_end__

.. seealso::

    See the :class:`~ray.air.RunConfig` API reference.

    See :ref:`tune-storage-options` for storage configuration examples (related to ``storage_path``).


Failure configurations in Train (``FailureConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The failure configuration specifies how training failures should be dealt with.

As part of the RunConfig, the properties of the failure configuration
are :ref:`not tunable <tune-search-space-tutorial>`.


.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __failure_config_start__
    :end-before: __failure_config_end__

.. seealso::

    See the :class:`~ray.air.FailureConfig` API reference.


Checkpoint configurations in Train (``CheckpointConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The checkpoint configuration specifies how often to checkpoint training state
and how many checkpoints to keep.

As part of the RunConfig, the properties of the checkpoint configuration
are :ref:`not tunable <tune-search-space-tutorial>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_start__
    :end-before: __checkpoint_config_end__

Trainers of certain frameworks including :class:`~ray.train.xgboost.XGBoostTrainer`,
:class:`~ray.train.lightgbm.LightGBMTrainer`, and :class:`~ray.train.huggingface.TransformersTrainer`
implement checkpointing out of the box. For these trainers, checkpointing can be
enabled by setting the checkpoint frequency within the :class:`~ray.air.CheckpointConfig`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_ckpt_freq_start__
    :end-before: __checkpoint_config_ckpt_freq_end__

.. warning::

    ``checkpoint_frequency`` and other parameters do *not* work for trainers
    that accept a custom training loop such as :class:`~ray.train.torch.TorchTrainer`,
    since checkpointing is fully user-controlled.

.. seealso::

    See the :class:`~ray.air.CheckpointConfig` API reference.
    
**[Experimental] Distributed Checkpoints**: For model parallel workloads where the models do not fit in a single GPU worker, 
it will be important to save and upload the model that is partitioned across different workers. You 
can enable this by setting `_checkpoint_keep_all_ranks=True` to retain the model checkpoints across workers,
and `_checkpoint_upload_from_workers=True` to upload their checkpoints to cloud directly in :class:`~ray.air.CheckpointConfig`. This functionality works for any trainer that inherits from :class:`~ray.train.data_parallel_trainer.DataParallelTrainer`.


Synchronization configurations in Train (``tune.SyncConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``tune.SyncConfig`` specifies how synchronization of results
and checkpoints should happen in a distributed Ray cluster.

As part of the RunConfig, the properties of the failure configuration
are :ref:`not tunable <tune-search-space-tutorial>`.

.. note::

    This configuration is mostly relevant to running multiple Train runs with a
    Ray Tune. See :ref:`tune-storage-options` for a guide on using the ``SyncConfig``.

.. seealso::

    See the :class:`~ray.tune.syncer.SyncConfig` API reference.


Environment Variables in Ray Train
----------------------------------

Some behavior of Ray Train can be controlled using environment variables.

Please also see the :ref:`Ray Tune environment variables <tune-env-vars>`.

* **RAY_AIR_FULL_TRACEBACKS**: If set to 1, will print full tracebacks for training functions,
  including internal code paths. Otherwise, abbreviated tracebacks that only show user code
  are printed. Defaults to 0 (disabled).
* **RAY_AIR_NEW_OUTPUT**: If set to 0, this disables
  the `experimental new console output <https://github.com/ray-project/ray/issues/36949>`_.

