.. _train-pytorch-lightning:

Get Started with PyTorch Lightning
==================================

This tutorial walks through the process of converting an existing PyTorch Lightning script to use Ray Train.

Learn how to:

1. Configure the Lightning Trainer so that it runs distributed with Ray and on the correct CPU or GPU device.
2. Configure :ref:`training function <train-overview-training-function>` to report metrics and save checkpoints.
3. Configure :ref:`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for a training job.
4. Launch a distributed training job with a :class:`~ray.train.torch.TorchTrainer`.

Quickstart
----------

For reference, the final code is as follows:

.. testcode::
    :skipif: True

    from ray.train.torch import TorchTrainer
    from ray.train import ScalingConfig

    def train_func(config):
        # Your PyTorch Lightning training code here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. `train_func` is the Python code that executes on each distributed training worker.
2. :class:`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
3. :class:`~ray.train.torch.TorchTrainer` launches the distributed training job.

Compare a PyTorch Lightning training script with and without Ray Train.

.. tabs::

    .. group-tab:: PyTorch Lightning

        .. This snippet isn't tested because it doesn't use any Ray code.

        .. testcode::
            :skipif: True

            import torch
            from torchvision.models import resnet18
            from torchvision.datasets import FashionMNIST
            from torchvision.transforms import ToTensor, Normalize, Compose
            from torch.utils.data import DataLoader
            import lightning.pytorch as pl

            # Model, Loss, Optimizer
            class ImageClassifier(pl.LightningModule):
                def __init__(self):
                    super(ImageClassifier, self).__init__()
                    self.model = resnet18(num_classes=10)
                    self.model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
                    self.criterion = torch.nn.CrossEntropyLoss()
                
                def forward(self, x):
                    return self.model(x)
                
                def training_step(self, batch, batch_idx):
                    x, y = batch
                    outputs = self.forward(x)
                    loss = self.criterion(outputs, y)
                    self.log("loss", loss, on_step=True, prog_bar=True)
                    return loss
                    
                def configure_optimizers(self):
                    return torch.optim.Adam(self.model.parameters(), lr=0.001)

            # Data
            transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
            train_data = FashionMNIST(root='./data', train=True, download=True, transform=transform)
            train_dataloader = DataLoader(train_data, batch_size=128, shuffle=True)

            # Training
            model = ImageClassifier()
            trainer = pl.Trainer(max_epochs=10)
            trainer.fit(model, train_dataloaders=train_dataloader)

                

    .. group-tab:: PyTorch Lightning + Ray Train

        .. code-block:: python
            :emphasize-lines: 8-10, 34, 43, 48-50, 52, 53, 55-60

            import torch
            from torchvision.models import resnet18
            from torchvision.datasets import FashionMNIST
            from torchvision.transforms import ToTensor, Normalize, Compose
            from torch.utils.data import DataLoader
            import lightning.pytorch as pl

            from ray.train.torch import TorchTrainer
            from ray.train import ScalingConfig
            import ray.train.lightning

            # Model, Loss, Optimizer
            class ImageClassifier(pl.LightningModule):
                def __init__(self):
                    super(ImageClassifier, self).__init__()
                    self.model = resnet18(num_classes=10)
                    self.model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
                    self.criterion = torch.nn.CrossEntropyLoss()
                
                def forward(self, x):
                    return self.model(x)
                
                def training_step(self, batch, batch_idx):
                    x, y = batch
                    outputs = self.forward(x)
                    loss = self.criterion(outputs, y)
                    self.log("loss", loss, on_step=True, prog_bar=True)
                    return loss
                    
                def configure_optimizers(self):
                    return torch.optim.Adam(self.model.parameters(), lr=0.001)
       

            def train_func(config):

                # Data
                transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
                train_data = FashionMNIST(root='./data', train=True, download=True, transform=transform)
                train_dataloader = DataLoader(train_data, batch_size=128, shuffle=True)

                # Training
                model = ImageClassifier()
                # [1] Configure PyTorch Lightning Trainer.
                trainer = pl.Trainer(
                    max_epochs=10,
                    devices="auto",
                    accelerator="auto",
                    strategy=ray.train.lightning.RayDDPStrategy(),
                    plugins=[ray.train.lightning.RayLightningEnvironment()],
                    callbacks=[ray.train.lightning.RayTrainReportCallback()],
                )
                trainer = ray.train.lightning.prepare_trainer(trainer)
                trainer.fit(model, train_dataloaders=train_dataloader)

            # [2] Configure scaling and resource requirements.
            scaling_config = ScalingConfig(num_workers=2, use_gpu=True)

            # [3] Launch distributed training job.
            trainer = TorchTrainer(train_func, scaling_config=scaling_config)
            result = trainer.fit()            


Set up a training function
--------------------------

First, update your training code to support distributed training. 
Begin by wrapping your code in a :ref:`training function <train-overview-training-function>`:

.. testcode::
    :skipif: True

    def train_func(config):
        # Your PyTorch Lightning training code here.

Each distributed training worker executes this function.


Ray Train sets up your distributed process group on each worker. You only need to 
make a few changes to your Lightning Trainer definition.

.. code-block:: diff

     import lightning.pytorch as pl
    -from pl.strategies import DDPStrategy
    -from pl.plugins.environments import LightningEnvironment
    +import ray.train.lightning 

     def train_func(config):
         ...
         model = MyLightningModule(...)
         datamodule = MyLightningDataModule(...)
        
         trainer = pl.Trainer(
    -        devices=[0,1,2,3],
    -        strategy=DDPStrategy(),
    -        plugins=[LightningEnvironment()],
    +        devices="auto",
    +        accelerator="auto",
    +        strategy=ray.train.lightning.RayDDPStrategy(),
    +        plugins=[ray.train.lightning.RayLightningEnvironment()]
         )
    +    trainer = ray.train.lightning.prepare_trainer(trainer)
        
         trainer.fit(model, datamodule=datamodule)

The following sections discuss each change.

Configure the distributed strategy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ray Train offers several sub-classed distributed strategies for Lightning. 
These strategies retain the same argument list as their base strategy classes. 
Internally, they configure the root device and the distributed 
sampler arguments.
    
- :class:`~ray.train.lightning.RayDDPStrategy` 
- :class:`~ray.train.lightning.RayFSDPStrategy` 
- :class:`~ray.train.lightning.RayDeepSpeedStrategy` 


.. code-block:: diff

     import lightning.pytorch as pl
    -from pl.strategies import DDPStrategy
    +import ray.train.lightning

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        strategy=DDPStrategy(),
    +        strategy=ray.train.lightning.RayDDPStrategy(),
             ...
         )
         ...

Configure the Ray cluster environment plugin
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Ray Train also provides a :class:`~ray.train.lightning.RayLightningEnvironment` class
as a specification for the Ray Cluster. This utility class configures the worker's 
local, global, and node rank and world size.


.. code-block:: diff

     import lightning.pytorch as pl
    -from pl.plugins.environments import LightningEnvironment
    +import ray.train.lightning

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        plugins=[LightningEnvironment()],
    +        plugins=[ray.train.lightning.RayLightningEnvironment()],
             ...
         )
         ...


Configure parallel devices
^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition, Ray TorchTrainer has already configured the correct 
``CUDA_VISIBLE_DEVICES`` for you. One should always use all available 
GPUs by setting ``devices="auto"`` and ``acelerator="auto"``.


.. code-block:: diff

     import lightning.pytorch as pl

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        devices=[0,1,2,3],
    +        devices="auto",
    +        accelerator="auto",
             ...
         )
         ...



Report checkpoints and metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To persist your checkpoints and monitor training progress, add a 
:class:`ray.train.lightning.RayTrainReportCallback` utility callback to your Trainer. 

                    
.. code-block:: diff

     import lightning.pytorch as pl
     from ray.train.lightning import RayTrainReportCallback

     def train_func(config):
         ...
         trainer = pl.Trainer(
             ...
    -        callbacks=[...],
    +        callbacks=[..., RayTrainReportCallback()],
         )
         ...


Reporting metrics and checkpoints to Ray Train enables you to support :ref:`fault-tolerant training <train-fault-tolerance>` and :ref:`hyperparameter optimization <train-tune>`. 
Note that the :class:`ray.train.lightning.RayTrainReportCallback` class only provides a simple implementation, and can be :ref:`further customized <train-dl-saving-checkpoints>`.

Prepare your Lightning Trainer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, pass your Lightning Trainer into
:meth:`~ray.train.lightning.prepare_trainer` to validate 
your configurations. 


.. code-block:: diff

     import lightning.pytorch as pl
     import ray.train.lightning

     def train_func(config):
         ...
         trainer = pl.Trainer(...)
    +    trainer = ray.train.lightning.prepare_trainer(trainer)
         ...


Configure scale and GPUs
------------------------

Outside of your training function, create a :class:`~ray.train.ScalingConfig` object to configure:

1. `num_workers` - The number of distributed training worker processes.
2. `use_gpu` - Whether each worker should use a GPU (or CPU).

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

See :ref:`train-run-config` for more configuration options for `TorchTrainer`.

Access training results
-----------------------

After training completes, Ray Train returns a :class:`~ray.train.Result` object, which contains
information about the training run, including the metrics and checkpoints reported during training.

.. testcode::

    result.metrics     # The metrics reported during training.
    result.checkpoint  # The latest checkpoint reported during training.
    result.path     # The path where logs are stored.
    result.error       # The exception that was raised, if training failed.

.. TODO: Add results guide

Next steps
---------- 

After you have converted your PyTorch Lightning training script to use Ray Train:

* See :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :ref:`Examples <train-examples>` for end-to-end examples of how to use Ray Train.
* Consult the :ref:`API Reference <train-api>` for more details on the classes and methods from this tutorial.

Version Compatibility
---------------------

Ray Train is tested with `pytorch_lightning` versions `1.6.5` and `2.0.4`. For full compatibility, use ``pytorch_lightning>=1.6.5`` . 
Earlier versions aren't prohibited but may result in unexpected issues. If you run into any compatibility issues, consider upgrading your PyTorch Lightning version or 
`file an issue <https://github.com/ray-project/ray/issues>`_. 

.. note::

    If you are using Lightning 2.x, please use the import path `lightning.pytorch.xxx` instead of `pytorch_lightning.xxx`.
