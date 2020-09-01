.. _tune-integration:

External library integrations (tune.integration)
================================================

.. contents::
    :local:
    :depth: 1

.. _tune-integration-kubernetes:

Kubernetes (tune.integration.kubernetes)
----------------------------------------

.. autofunction:: ray.tune.integration.kubernetes.NamespacedKubernetesSyncer

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

.. _tune-integration-wandb:

Weights and Biases (tune.integration.wandb)
-------------------------------------------

:ref:`See also here <tune-wandb>`.

.. autoclass:: ray.tune.integration.wandb.WandbLogger

.. autofunction:: ray.tune.integration.wandb.wandb_mixin