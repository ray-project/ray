.. _train-pytorch:

Get Started with PyTorch
========================

This tutorial walks through the process of converting an existing PyTorch script to use Ray Train.

Learn how to:

1. Configure a model to run distributed and on the correct CPU/GPU device.
2. Configure a dataloader to shard data across the :ref:`workers <train-overview-worker>` and place data on the correct CPU or GPU device.
3. Configure a :ref:`training function <train-overview-training-function>` to report metrics and save checkpoints.
4. Configure :ref:`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for a training job.
5. Launch a distributed training job with a :class:`~ray.train.torch.TorchTrainer` class.

Quickstart
----------

For reference, the final code is as follows:

.. testcode::
    :skipif: True

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Your PyTorch training code here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. `train_func` is the Python code that executes on each distributed training worker.
2. :class:`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
3. :class:`~ray.train.torch.TorchTrainer` launches the distributed training job.

Compare a PyTorch training script with and without Ray Train.

.. tab-set::

    .. tab-item:: PyTorch

        .. This snippet isn't tested because it doesn't use any Ray code.

        .. testcode::
            :skipif: True

            import tempfile
            import torch
            from torchvision.models import resnet18
            from torchvision.datasets import FashionMNIST
            from torchvision.transforms import ToTensor, Normalize, Compose
            from torch.utils.data import DataLoader
            from torch.optim import Adam
            from torch.nn import CrossEntropyLoss

            # Model, Loss, Optimizer
            model = resnet18(num_classes=10)
            model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
            criterion = CrossEntropyLoss()
            optimizer = Adam(model.parameters(), lr=0.001)

            # Data
            transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
            train_data = FashionMNIST(root='./data', train=True, download=True, transform=transform)
            train_loader = DataLoader(train_data, batch_size=128, shuffle=True)

            # Training
            for epoch in range(10):
                for images, labels in train_loader:
                    outputs = model(images)
                    loss = criterion(outputs, labels)
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

                checkpoint_dir = tempfile.gettempdir()
                checkpoint_path = checkpoint_dir + "/model.checkpoint"
                torch.save(model.state_dict(), checkpoint_path)



    .. tab-item:: PyTorch + Ray Train

        .. code-block:: python
            :emphasize-lines: 9, 10, 12, 17, 18, 26, 27, 41, 42, 44-49

            import tempfile
            import torch
            from torchvision.models import resnet18
            from torchvision.datasets import FashionMNIST
            from torchvision.transforms import ToTensor, Normalize, Compose
            from torch.utils.data import DataLoader
            from torch.optim import Adam
            from torch.nn import CrossEntropyLoss
            from ray.train.torch import TorchTrainer
            from ray.train import ScalingConfig, Checkpoint

            def train_func(config):

                # Model, Loss, Optimizer
                model = resnet18(num_classes=10)
                model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
                # [1] Prepare model.
                model = ray.train.torch.prepare_model(model)
                criterion = CrossEntropyLoss()
                optimizer = Adam(model.parameters(), lr=0.001)

                # Data
                transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
                train_data = FashionMNIST(root='./data', train=True, download=True, transform=transform)
                train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
                # [2] Prepare dataloader.
                train_loader = ray.train.torch.prepare_data_loader(train_loader)

                # Training
                for epoch in range(10):
                    for images, labels in train_loader:
                        outputs = model(images)
                        loss = criterion(outputs, labels)
                        optimizer.zero_grad()
                        loss.backward()
                        optimizer.step()

                    checkpoint_dir = tempfile.gettempdir()
                    checkpoint_path = checkpoint_dir + "/model.checkpoint"
                    torch.save(model.state_dict(), checkpoint_path)
                    # [3] Report metrics and checkpoint.
                    ray.train.report({"loss": loss.item()}, checkpoint=Checkpoint.from_directory(checkpoint_dir))

            # [4] Configure scaling and resource requirements.
            scaling_config = ScalingConfig(num_workers=2, use_gpu=True)

            # [5] Launch distributed training job.
            trainer = TorchTrainer(train_func, scaling_config=scaling_config)
            result = trainer.fit()

Set up a training function
--------------------------

First, update your training code to support distributed training.
Begin by wrapping your code in a :ref:`training function <train-overview-training-function>`:

.. testcode::
    :skipif: True

    def train_func(config):
        # Your PyTorch training code here.

Each distributed training worker executes this function.

Set up a model
^^^^^^^^^^^^^^

Use the :func:`ray.train.torch.prepare_model` utility function to:

1. Move your model to the correct device.
2. Wrap it in ``DistributedDataParallel``.

.. code-block:: diff

    -from torch.nn.parallel import DistributedDataParallel
    +import ray.train.torch

     def train_func(config):

         ...

         # Create model.
         model = ...

         # Set up distributed training and device placement.
    -    device_id = ... # Your logic to get the right device.
    -    model = model.to(device_id or "cpu")
    -    model = DistributedDataParallel(model, device_ids=[device_id])
    +    model = ray.train.torch.prepare_model(model)

         ...

Set up a dataset
^^^^^^^^^^^^^^^^

.. TODO: Update this to use Ray Data.

Use the :func:`ray.train.torch.prepare_data_loader` utility function, which:

1. Adds a ``DistributedSampler`` to your ``DataLoader``.
2. Moves the batches to the right device.

Note that this step isn't necessary if you're passing in Ray Data to your Trainer.
See :ref:`data-ingest-torch`.

.. code-block:: diff

     from torch.utils.data import DataLoader
    -from torch.utils.data import DistributedSampler
    +import ray.train.torch

     def train_func(config):

         ...

         dataset = ...

         data_loader = DataLoader(dataset, batch_size=worker_batch_size)
    -    data_loader = DataLoader(dataset, batch_size=worker_batch_size, sampler=DistributedSampler(dataset))
    +    data_loader = ray.train.torch.prepare_data_loader(data_loader)

         for X, y in data_loader:
    -        X = X.to_device(device)
    -        y = y.to_device(device)

         ...

.. tip::
    Keep in mind that ``DataLoader`` takes in a ``batch_size`` which is the batch size for each worker.
    The global batch size can be calculated from the worker batch size (and vice-versa) with the following equation:

    .. testcode::
        :skipif: True
        
        global_batch_size = worker_batch_size * ray.train.get_context().get_world_size()


Report checkpoints and metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To monitor progress, you can report intermediate metrics and checkpoints using the :func:`ray.train.report` utility function.

.. code-block:: diff

    +import ray.train
    +from ray.train import Checkpoint

     def train_func(config):

         ...
         torch.save(model.state_dict(), f"{checkpoint_dir}/model.pth"))
    +    metrics = {"loss": loss.item()} # Training/validation metrics.
    +    checkpoint = Checkpoint.from_directory(checkpoint_dir) # Build a Ray Train checkpoint from a directory
    +    ray.train.report(metrics=metrics, checkpoint=checkpoint)

         ...

For more details, see :ref:`train-monitoring-and-logging` and :ref:`train-checkpointing`.


Configure scale and GPUs
------------------------

Outside of your training function, create a :class:`~ray.train.ScalingConfig` object to configure:

1. :class:`num_workers <ray.train.ScalingConfig>` - The number of distributed training worker processes.
2. :class:`use_gpu <ray.train.ScalingConfig>` - Whether each worker should use a GPU (or CPU).

.. testcode::

    from ray.train import ScalingConfig
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)


For more details, see :ref:`train_scaling_config`.

Launch a training job
---------------------

Tying this all together, you can now launch a distributed training job
with a :class:`~ray.train.torch.TorchTrainer`.

.. testcode::
    :hide:

    from ray.train import ScalingConfig

    train_func = lambda: None
    scaling_config = ScalingConfig(num_workers=1)

.. testcode::

    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

Access training results
-----------------------

After training completes, a :class:`~ray.train.Result` object is returned which contains
information about the training run, including the metrics and checkpoints reported during training.

.. testcode::

    result.metrics     # The metrics reported during training.
    result.checkpoint  # The latest checkpoint reported during training.
    result.path     # The path where logs are stored.
    result.error       # The exception that was raised, if training failed.

.. TODO: Add results guide

Next steps
----------

After you have converted your PyTorch training script to use Ray Train:

* See :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :ref:`Examples <train-examples>` for end-to-end examples of how to use Ray Train.
* Dive into the :ref:`API Reference <train-api>` for more details on the classes and methods used in this tutorial.
