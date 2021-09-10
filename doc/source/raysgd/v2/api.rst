.. _sgd-api:

RaySGD API
==========

.. _sgd-api-trainer:

Trainer
-------

.. autoclass:: ray.util.sgd.v2.Trainer
    :members:

.. _sgd-api-backend-config:

BackendConfigs
--------------

.. autoclass:: ray.util.sgd.v2.BackendConfig

.. _sgd-api-torch-config:

TorchConfig
~~~~~~~~~~~

.. autoclass:: ray.util.sgd.v2.TorchConfig

.. _sgd-api-tensorflow-config:

TensorflowConfig
~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.v2.TensorflowConfig

.. _sgd-api-horovod-config:

HorovodConfig
~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.v2.HorovodConfig

.. _sgd-api-callback:

Callbacks
----------

.. autoclass:: ray.util.sgd.v2.SGDCallback
    :members:

.. _sgd-api-json-logger-callback:

JsonLoggerCallback
~~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.v2.callbacks.JsonLoggerCallback

.. _sgd-api-tbx-logger-callback:

TBXLoggerCallback
~~~~~~~~~~~~~~~~~

.. autoclass:: ray.util.sgd.v2.callbacks.TBXLoggerCallback


CheckpointStrategy
------------------

.. autoclass:: ray.util.sgd.v2.CheckpointStrategy

Training Function Utilities
---------------------------

sgd.report
~~~~~~~~~~

.. autofunction::  ray.util.sgd.v2.report

sgd.load_checkpoint
~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.util.sgd.v2.load_checkpoint

sgd.save_checkpoint
~~~~~~~~~~~~~~~~~~~

.. autofunction::  ray.util.sgd.v2.save_checkpoint

sgd.world_rank
~~~~~~~~~~~~~~

.. autofunction::  ray.util.sgd.v2.world_rank