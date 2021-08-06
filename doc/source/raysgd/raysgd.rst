.. _sgd-index:

=====================================
RaySGD: Distributed Training Wrappers
=====================================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

RaySGD is a lightweight library for distributed deep learning, providing thin wrappers around PyTorch and TensorFlow native modules for data parallel training.

The main features are:

  - **Ease of use**: Scale PyTorch's native ``DistributedDataParallel`` and TensorFlow's ``tf.distribute.MirroredStrategy`` without needing to monitor individual nodes.
  - **Composability**: RaySGD is built on top of the Ray Actor API, enabling seamless integration with existing Ray applications such as RLlib, Tune, and Ray.Serve.
  - **Scale up and down**: Start on single CPU. Scale up to multi-node, multi-CPU, or multi-GPU clusters by changing 2 lines of code.


Getting Started
---------------

You can start a ``TorchTrainer`` with the following:

.. code-block:: python

    import ray
    from ray.util.sgd import TorchTrainer
    from ray.util.sgd.torch import TrainingOperator
    from ray.util.sgd.torch.examples.train_example import LinearDataset

    import torch
    from torch.utils.data import DataLoader

    class CustomTrainingOperator(TrainingOperator):
        def setup(self, config):
            # Load data.
            train_loader = DataLoader(LinearDataset(2, 5), config["batch_size"])
            val_loader = DataLoader(LinearDataset(2, 5), config["batch_size"])

            # Create model.
            model = torch.nn.Linear(1, 1)

            # Create optimizer.
            optimizer = torch.optim.SGD(model.parameters(), lr=1e-2)

            # Create loss.
            loss = torch.nn.MSELoss()

            # Register model, optimizer, and loss.
            self.model, self.optimizer, self.criterion = self.register(
                models=model,
                optimizers=optimizer,
                criterion=loss)

            # Register data loaders.
            self.register_data(train_loader=train_loader, validation_loader=val_loader)


    ray.init()

    trainer1 = TorchTrainer(
        training_operator_cls=CustomTrainingOperator,
        num_workers=2,
        use_gpu=False,
        config={"batch_size": 64})

    stats = trainer1.train()
    print(stats)
    trainer1.shutdown()
    print("success!")


.. tip:: We are rolling out a lighter-weight version of RaySGD in a future version of Ray. See the documentation :ref:`here <sgd-v2-docs>`.
