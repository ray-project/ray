.. _tune-integration:

External library integrations (tune.integration)
================================================

.. contents::
    :local:
    :depth: 1


.. _tune-integration-docker:

Docker (tune.integration.docker)
--------------------------------

.. autofunction:: ray.tune.integration.docker.DockerSyncer


.. _tune-integration-keras:

Keras (tune.integration.keras)
------------------------------------------------------

.. autoclass:: ray.tune.integration.keras.TuneReportCallback

.. autoclass:: ray.tune.integration.keras.TuneReportCheckpointCallback

.. _tune-integration-kubernetes:

Kubernetes (tune.integration.kubernetes)
----------------------------------------

.. autofunction:: ray.tune.integration.kubernetes.NamespacedKubernetesSyncer

.. _tune-integration-mxnet:

MXNet (tune.integration.mxnet)
------------------------------

.. autoclass:: ray.tune.integration.mxnet.TuneReportCallback

.. autoclass:: ray.tune.integration.mxnet.TuneCheckpointCallback


.. _tune-integration-pytorch-lightning:

PyTorch Lightning (tune.integration.pytorch_lightning)
------------------------------------------------------

.. autoclass:: ray.tune.integration.pytorch_lightning.TuneReportCallback

.. autoclass:: ray.tune.integration.pytorch_lightning.TuneReportCheckpointCallback

.. _tune-integration-torch:

Torch (tune.integration.torch)
------------------------------

.. autofunction:: ray.tune.integration.torch.DistributedTrainableCreator

.. autofunction:: ray.tune.integration.torch.distributed_checkpoint_dir

.. autofunction:: ray.tune.integration.torch.is_distributed_trainable


.. _tune-integration-horovod:

Horovod (tune.integration.horovod)
----------------------------------

.. autofunction:: ray.tune.integration.horovod.DistributedTrainableCreator

.. _tune-integration-wandb:

Weights and Biases (tune.integration.wandb)
-------------------------------------------

:ref:`See also here <tune-wandb>`.

.. autoclass:: ray.tune.integration.wandb.WandbLoggerCallback

.. autofunction:: ray.tune.integration.wandb.wandb_mixin


.. _tune-integration-xgboost:

XGBoost (tune.integration.xgboost)
----------------------------------

.. autoclass:: ray.tune.integration.xgboost.TuneReportCallback

.. autoclass:: ray.tune.integration.xgboost.TuneReportCheckpointCallback
