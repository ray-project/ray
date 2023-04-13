.. _train-config:

Ray Train Configuration User Guide
==================================

The following overviews how to configure scale-out, run options, and fault-tolerance for Train.
For more details on how to configure data ingest, also refer to :ref:`air-ingest`.

Scaling Configurations in Train (``ScalingConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The scaling configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the scaling configuration are :ref:`tunable <air-tuner-search-space>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __scaling_config_start__
    :end-before: __scaling_config_end__

.. seealso::

    See the :class:`~ray.air.ScalingConfig` API reference.

Run Configuration in Train (``RunConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The run configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the run configuration are :ref:`not tunable <air-tuner-search-space>`.

.. _train-config-sync:

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
are :ref:`not tunable <air-tuner-search-space>`.


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
are :ref:`not tunable <air-tuner-search-space>`.

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_start__
    :end-before: __checkpoint_config_end__

Trainers of certain frameworks including :class:`~ray.train.xgboost.XGBoostTrainer`,
:class:`~ray.train.lightgbm.LightGBMTrainer`, and :class:`~ray.train.huggingface.HuggingFaceTrainer`
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
