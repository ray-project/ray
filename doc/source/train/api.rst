
.. _train-api:

Ray Train API
=============

.. _train-api-trainer:

Trainer
-------

.. autoclass:: ray.train.Trainer
    :members:

.. _train-api-iterator:

TrainingIterator
~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.TrainingIterator
    :members:

.. _train-api-backend-config:

Backend Configurations
----------------------

.. _train-api-torch-config:

TorchConfig
~~~~~~~~~~~

.. autoclass:: ray.train.torch.TorchConfig

.. _train-api-tensorflow-config:

TensorflowConfig
~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.tensorflow.TensorflowConfig

.. _train-api-horovod-config:

HorovodConfig
~~~~~~~~~~~~~

.. autoclass:: ray.train.horovod.HorovodConfig

.. _train-api-backend-interfaces:

Backend interfaces (for developers only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Backend
+++++++

.. autoclass:: ray.train.backend.Backend

BackendConfig
+++++++++++++

.. autoclass:: ray.train.backend.BackendConfig


Callbacks
---------

.. _train-api-callback:

TrainingCallback
~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.TrainingCallback
    :members:

.. _train-api-print-callback:

PrintCallback
~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.PrintCallback

.. _train-api-json-logger-callback:

JsonLoggerCallback
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.JsonLoggerCallback

.. _train-api-tbx-logger-callback:

TBXLoggerCallback
~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.TBXLoggerCallback

.. _train-api-mlflow-logger-callback:

MLflowLoggerCallback
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.MLflowLoggerCallback


.. _train-api-torch-tensorboard-profiler-callback:

TorchTensorboardProfilerCallback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.TorchTensorboardProfilerCallback

ResultsPreprocessors
~~~~~~~~~~~~~~~~~~~~

.. _train-api-results-preprocessor:

ResultsPreprocessor
+++++++++++++++++++

.. autoclass:: ray.train.callbacks.results_preprocessors.ResultsPreprocessor
    :members:

SequentialResultsPreprocessor
+++++++++++++++++++++++++++++++

.. autoclass:: ray.train.callbacks.results_preprocessors.SequentialResultsPreprocessor

IndexedResultsPreprocessor
+++++++++++++++++++++++++++++++

.. autoclass:: ray.train.callbacks.results_preprocessors.IndexedResultsPreprocessor

ExcludedKeysResultsPreprocessor
+++++++++++++++++++++++++++++++

.. autoclass:: ray.train.callbacks.results_preprocessors.ExcludedKeysResultsPreprocessor

Checkpointing
-------------

.. _train-api-checkpoint-strategy:

CheckpointStrategy
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.CheckpointStrategy

.. _train-api-func-utils:

Training Function Utilities
---------------------------

train.report
~~~~~~~~~~~~

.. autofunction::  ray.train.report

train.load_checkpoint
~~~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.train.load_checkpoint

train.save_checkpoint
~~~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.train.save_checkpoint

train.get_dataset_shard
~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.train.get_dataset_shard

train.world_rank
~~~~~~~~~~~~~~~~

.. autofunction::  ray.train.world_rank

train.local_rank
~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.local_rank

train.world_size
~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.world_size

.. _train-api-torch-utils:

PyTorch Training Function Utilities
-----------------------------------

train.torch.prepare_model
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_model

.. _train-api-torch-prepare-data-loader:

train.torch.prepare_data_loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_data_loader

train.torch.prepare_optimizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_optimizer


train.torch.backward
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.backward


train.torch.get_device
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.get_device

train.torch.enable_reproducibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.enable_reproducibility

.. _train-api-torch-worker-profiler:

train.torch.accelerate
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.accelerate

train.torch.TorchWorkerProfiler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.torch.TorchWorkerProfiler
    :members:

.. _train-api-tensorflow-utils:

TensorFlow Training Function Utilities
--------------------------------------

train.tensorflow.prepare_dataset_shard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.tensorflow.prepare_dataset_shard