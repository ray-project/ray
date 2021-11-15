
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


Callbacks
---------

.. _train-api-callback:

TrainingCallback
~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.TrainingCallback
    :members:

.. _train-api-json-logger-callback:

JsonLoggerCallback
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.JsonLoggerCallback

.. _train-api-tbx-logger-callback:

TBXLoggerCallback
~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.callbacks.TBXLoggerCallback

Checkpointing
-------------

.. _train-api-checkpoint-strategy:

CheckpointStrategy
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.train.CheckpointStrategy

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

train.world_rank
~~~~~~~~~~~~~~~~

.. autofunction::  ray.train.world_rank

train.local_rank
~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.local_rank

train.world_size
~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.world_size

PyTorch Training Function Utilities
-----------------------------------

train.torch.prepare_model
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_model

train.torch.prepare_data_loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.prepare_data_loader

train.torch.get_device
~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: ray.train.torch.get_device