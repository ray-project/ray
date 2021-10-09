
.. _sgd-api:

RaySGD API
==========

.. _sgd-api-trainer:

Trainer
-------

.. autoclass:: ray.sgd.Trainer
    :members:

.. _sgd-api-iterator:

SGDIterator
~~~~~~~~~~~

.. autoclass:: ray.sgd.SGDIterator
    :members:

.. _sgd-api-backend-config:

Backend Configurations
----------------------

.. _sgd-api-torch-config:

TorchConfig
~~~~~~~~~~~

.. autoclass:: ray.sgd.TorchConfig

.. _sgd-api-tensorflow-config:

TensorflowConfig
~~~~~~~~~~~~~~~~

.. autoclass:: ray.sgd.TensorflowConfig

.. _sgd-api-horovod-config:

HorovodConfig
~~~~~~~~~~~~~

.. autoclass:: ray.sgd.HorovodConfig


Callbacks
---------

.. _sgd-api-callback:

SGDCallback
~~~~~~~~~~~

.. autoclass:: ray.sgd.SGDCallback
    :members:

.. _sgd-api-json-logger-callback:

JsonLoggerCallback
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.sgd.callbacks.JsonLoggerCallback

.. _sgd-api-tbx-logger-callback:

TBXLoggerCallback
~~~~~~~~~~~~~~~~~

.. autoclass:: ray.sgd.callbacks.TBXLoggerCallback

Checkpointing
-------------

.. _sgd-api-checkpoint-strategy:

CheckpointStrategy
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.sgd.CheckpointStrategy

Training Function Utilities
---------------------------

sgd.report
~~~~~~~~~~

.. autofunction::  ray.sgd.report

sgd.load_checkpoint
~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.sgd.load_checkpoint

sgd.save_checkpoint
~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.sgd.save_checkpoint

sgd.world_rank
~~~~~~~~~~~~~~

.. autofunction::  ray.sgd.world_rank

sgd.local_rank
~~~~~~~~~~~~~~

.. autofunction:: ray.sgd.local_rank