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


Deprecated APIs
---------------

These APIs are deprecated and will be removed in a future Ray release:

- ray.train.Trainer
- ray.train.callbacks.*
