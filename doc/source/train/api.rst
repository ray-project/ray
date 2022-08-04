.. _train-api:

Ray Train API
=============

This page covers advanced configurations for specific frameworks using Train.

For different high level trainers and their usage, take a look at the :ref:`AIR Trainer package reference <air-trainer-ref>`.

.. _train-api-backend-config:

Backend Configurations
----------------------

.. _train-api-torch-config:

TorchConfig
~~~~~~~~~~~

.. autoclass:: ray.train.torch.TorchConfig
    :noindex:

.. _train-api-tensorflow-config:

TensorflowConfig
~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.tensorflow.TensorflowConfig
    :noindex:

.. _train-api-horovod-config:

HorovodConfig
~~~~~~~~~~~~~

.. autoclass:: ray.train.horovod.HorovodConfig
    :noindex:

.. _train-api-backend-interfaces:

Backend interfaces (for developers only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Backend
+++++++

.. autoclass:: ray.train.backend.Backend

BackendConfig
+++++++++++++

.. autoclass:: ray.train.backend.BackendConfig


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

.. _train-api-torch-prepare-model:

train.torch.prepare_model
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_model
    :noindex:

.. _train-api-torch-prepare-data-loader:

train.torch.prepare_data_loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_data_loader
    :noindex:

train.torch.prepare_optimizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_optimizer
    :noindex:


train.torch.backward
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.backward
    :noindex:

.. _train-api-torch-get-device:

train.torch.get_device
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.get_device
    :noindex:

train.torch.enable_reproducibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.enable_reproducibility
    :noindex:

.. _train-api-torch-worker-profiler:

train.torch.accelerate
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.accelerate
    :noindex:

train.torch.TorchWorkerProfiler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.torch.TorchWorkerProfiler
    :members:
    :noindex:

.. _train-api-tensorflow-utils:

TensorFlow Training Function Utilities
--------------------------------------

train.tensorflow.prepare_dataset_shard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.tensorflow.prepare_dataset_shard
    :noindex:


Deprecated APIs
---------------

These APIs are deprecated and will be removed in a future Ray release:

- ray.train.Trainer
- ray.train.callbacks.*
