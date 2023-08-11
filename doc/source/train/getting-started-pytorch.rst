.. _train-pytorch:

Getting Started with PyTorch
============================

This tutorial will walk you through the process of converting an existing PyTorch script to use Ray Train.

By the end of this, you will learn how to:

1. Configure your model so that it runs distributed and is placed on the correct CPU/GPU device.
2. Configure your dataloader so that it is sharded across the workers and place data on the correct CPU/GPU device.
3. Configure your training function to report metrics and save checkpoints.
4. Configure scale and CPU/GPU resource requirements for your training job.
5. Launch your distributed training job with a :class:`~ray.train.torch.TorchTrainer`.

Quickstart
----------

Before we begin, you can expect that the final code will look something like this:

.. code-block:: python

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Your PyTorch training code here.
    
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. Your `train_func` will be the Python code that is executed on each distributed training worker.
2. Your `ScalingConfig` will define the number of distributed training workers and whether to use GPUs.
3. Your `TorchTrainer` will launch the distributed training job.

Let's compare a PyTorch training script with and without Ray Train.

.. tabs::

    .. group-tab:: PyTorch

        .. code-block:: python

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

                

    .. group-tab:: PyTorch + Ray Train

        .. code-block:: python
       
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


Now, let's get started!

Setting up your training function
---------------------------------

First, you'll want to update your training code to support distributed training. 
You can begin by wrapping your code in a function:

.. code-block:: python

    def train_func(config):
        # Your PyTorch training code here.

This function will be executed on each distributed training worker.

Setting up your model
^^^^^^^^^^^^^^^^^^^^^

Use the :func:`ray.train.torch.prepare_model` utility function. This will:

1. Move your model to the right device.
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

Setting up your dataset
^^^^^^^^^^^^^^^^^^^^^^^

.. TODO: Update this to use Ray Data.

Use the :func:`ray.train.torch.prepare_data_loader` utility function. This will: 

1. Add a ``DistributedSampler`` to your ``DataLoader``.
2. Move the batches to the right device. 

Note that this step is not necessary if you are passing in Ray Data to your Trainer
(see :ref:`data-ingest-torch`):

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

    .. code-block:: python

        global_batch_size = worker_batch_size * ray.train.get_context().get_world_size()


Reporting metrics and checkpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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


Configuring scale and GPUs
---------------------------

Outside of your training function, create a :class:`~ray.train.ScalingConfig` object to configure:

1. `num_workers` - The number of distributed training worker processes.
2. `use_gpu` - Whether each worker should use a GPU (or CPU).

.. code-block:: python

    from ray.train import ScalingConfig
    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)


For more details, see :ref:`train_scaling_config`.

Launching your training job
---------------------------

Tying this all together, you can now launch a distributed training job 
with a :class:`~ray.train.torch.TorchTrainer`.

.. code-block:: python

    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

Accessing training results
--------------------------

After training completes, a :class:`~ray.train.Result` object will be returned which contains
information about the training run, including the metrics and checkpoints reported during training.

.. code-block:: python

    result.metrics     # The metrics reported during training.
    result.checkpoint  # The latest checkpoint reported during training.
    result.log_dir     # The path where logs are stored.
    result.error       # The exception that was raised, if training failed.

.. TODO: Add results guide

Next steps
----------

Congratulations! You have successfully converted your PyTorch training script to use Ray Train.

* Head over to the :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :ref:`Examples <train-examples>` for end-to-end examples of how to use Ray Train.
* Dive into the :ref:`API Reference <train-api>` for more details on the classes and methods used in this tutorial.