RaySGD: Distributed Training Wrappers
=====================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

RaySGD is a lightweight library for distributed deep learning, providing thin wrappers around PyTorch and TensorFlow native modules for data parallel training.

The main features are:

  - **Ease of use**: Scale PyTorch's native ``DistributedDataParallel`` and TensorFlow's ``tf.distribute.MirroredStrategy`` without needing to monitor individual nodes.
  - **Composability**: RaySGD is built on top of the Ray Actor API, enabling seamless integration with existing Ray applications such as RLlib, Tune, and Ray.Serve.
  - **Scale up and down**: Start on single CPU. Scale up to multi-node, multi-CPU, or multi-GPU clusters by changing 2 lines of code.

.. note::

  This API is new and may be revised in future Ray releases. If you encounter
  any bugs, please file an `issue on GitHub`_.


Getting Started
---------------

You can start a ``TorchTrainer`` with the following:

.. code-block:: python

    import numpy as np
    import torch
    import torch.nn as nn
    from torch import distributed

    from ray.util.sgd import TorchTrainer
    from ray.util.sgd.examples.train_example import LinearDataset


    def model_creator(config):
        return nn.Linear(1, 1)


    def optimizer_creator(model, config):
        """Returns optimizer."""
        return torch.optim.SGD(model.parameters(), lr=1e-2)


    def data_creator(batch_size, config):
        """Returns training dataloader, validation dataloader."""
        return LinearDataset(2, 5),  LinearDataset(2, 5, size=400)

    ray.init()

    trainer1 = TorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        loss_creator=nn.MSELoss,
        num_replicas=2,
        use_gpu=True,
        batch_size=512,
        backend="nccl")

    stats = trainer1.train()
    print(stats)
    trainer1.shutdown()
    print("success!")

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/26EMwdahdgm7Lscy9>`_!
