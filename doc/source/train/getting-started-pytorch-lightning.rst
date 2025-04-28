.. _train-pytorch-lightning:

Get Started with Distributed Training using PyTorch Lightning
=============================================================

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

    def train_func():
        # Your PyTorch Lightning training code here.

    scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
    trainer = TorchTrainer(train_func, scaling_config=scaling_config)
    result = trainer.fit()

1. `train_func` is the Python code that executes on each distributed training worker.
2. :class:`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
3. :class:`~ray.train.torch.TorchTrainer` launches the distributed training job.

Compare a PyTorch Lightning training script with and without Ray Train.

.. tab-set::

    .. tab-item:: PyTorch Lightning

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
                    self.model.conv1 = torch.nn.Conv2d(
                        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
                    )
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
            transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
            train_data = FashionMNIST(root='./data', train=True, download=True, transform=transform)
            train_dataloader = DataLoader(train_data, batch_size=128, shuffle=True)

            # Training
            model = ImageClassifier()
            trainer = pl.Trainer(max_epochs=10)
            trainer.fit(model, train_dataloaders=train_dataloader)


    .. tab-item:: PyTorch Lightning + Ray Train

        .. code-block:: python
            :emphasize-lines: 11-12, 38, 52-57, 59, 63, 66-73

            import os
            import tempfile

            import torch
            from torch.utils.data import DataLoader
            from torchvision.models import resnet18
            from torchvision.datasets import FashionMNIST
            from torchvision.transforms import ToTensor, Normalize, Compose
            import lightning.pytorch as pl

            import ray.train.lightning
            from ray.train.torch import TorchTrainer

            # Model, Loss, Optimizer
            class ImageClassifier(pl.LightningModule):
                def __init__(self):
                    super(ImageClassifier, self).__init__()
                    self.model = resnet18(num_classes=10)
                    self.model.conv1 = torch.nn.Conv2d(
                        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
                    )
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


            def train_func():
                # Data
                transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
                data_dir = os.path.join(tempfile.gettempdir(), "data")
                train_data = FashionMNIST(root=data_dir, train=True, download=True, transform=transform)
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
                    # [1a] Optionally, disable the default checkpointing behavior
                    # in favor of the `RayTrainReportCallback` above.
                    enable_checkpointing=False,
                )
                trainer = ray.train.lightning.prepare_trainer(trainer)
                trainer.fit(model, train_dataloaders=train_dataloader)

            # [2] Configure scaling and resource requirements.
            scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)

            # [3] Launch distributed training job.
            trainer = TorchTrainer(
                train_func,
                scaling_config=scaling_config,
                # [3a] If running in a multi-node cluster, this is where you
                # should configure the run's persistent storage that is accessible
                # across all worker nodes.
                # run_config=ray.train.RunConfig(storage_path="s3://..."),
            )
            result: ray.train.Result = trainer.fit()

            # [4] Load the trained model.
            with result.checkpoint.as_directory() as checkpoint_dir:
                model = ImageClassifier.load_from_checkpoint(
                    os.path.join(
                        checkpoint_dir,
                        ray.train.lightning.RayTrainReportCallback.CHECKPOINT_NAME,
                    ),
                )


Set up a training function
--------------------------

.. include:: ./common/torch-configure-train_func.rst

Ray Train sets up your distributed process group on each worker. You only need to
make a few changes to your Lightning Trainer definition.

.. code-block:: diff

     import lightning.pytorch as pl
    -from pl.strategies import DDPStrategy
    -from pl.plugins.environments import LightningEnvironment
    +import ray.train.lightning

     def train_func():
         ...
         model = MyLightningModule(...)
         datamodule = MyLightningDataModule(...)

         trainer = pl.Trainer(
    -        devices=[0, 1, 2, 3],
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

     def train_func():
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

     def train_func():
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

     def train_func():
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

     def train_func():
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

     def train_func():
         ...
         trainer = pl.Trainer(...)
    +    trainer = ray.train.lightning.prepare_trainer(trainer)
         ...


.. include:: ./common/torch-configure-run.rst


Next steps
----------

After you have converted your PyTorch Lightning training script to use Ray Train:

* See :ref:`User Guides <train-user-guides>` to learn more about how to perform specific tasks.
* Browse the :doc:`Examples <examples>` for end-to-end examples of how to use Ray Train.
* Consult the :ref:`API Reference <train-api>` for more details on the classes and methods from this tutorial.

Version Compatibility
---------------------

Ray Train is tested with `pytorch_lightning` versions `1.6.5` and `2.1.2`. For full compatibility, use ``pytorch_lightning>=1.6.5`` .
Earlier versions aren't prohibited but may result in unexpected issues. If you run into any compatibility issues, consider upgrading your PyTorch Lightning version or
`file an issue <https://github.com/ray-project/ray/issues>`_.

.. note::

    If you are using Lightning 2.x, please use the import path `lightning.pytorch.xxx` instead of `pytorch_lightning.xxx`.

.. _lightning-trainer-migration-guide:

LightningTrainer Migration Guide
--------------------------------

Ray 2.4 introduced the `LightningTrainer`, and exposed a
`LightningConfigBuilder` to define configurations for `pl.LightningModule`
and `pl.Trainer`.

It then instantiates the model and trainer objects and runs a pre-defined
training function in a black box.

This version of the LightningTrainer API was constraining and limited
your ability to manage the training functionality.

Ray 2.7 introduced the newly unified :class:`~ray.train.torch.TorchTrainer` API, which offers
enhanced transparency, flexibility, and simplicity. This API is more aligned
with standard PyTorch Lightning scripts, ensuring users have better
control over their native Lightning code.


.. tab-set::

    .. tab-item:: (Deprecating) LightningTrainer

        .. This snippet isn't tested because it raises a hard deprecation warning.

        .. testcode::
            :skipif: True

            from ray.train.lightning import LightningConfigBuilder, LightningTrainer

            config_builder = LightningConfigBuilder()
            # [1] Collect model configs
            config_builder.module(cls=MyLightningModule, lr=1e-3, feature_dim=128)

            # [2] Collect checkpointing configs
            config_builder.checkpointing(monitor="val_accuracy", mode="max", save_top_k=3)

            # [3] Collect pl.Trainer configs
            config_builder.trainer(
                max_epochs=10,
                accelerator="gpu",
                log_every_n_steps=100,
            )

            # [4] Build datasets on the head node
            datamodule = MyLightningDataModule(batch_size=32)
            config_builder.fit_params(datamodule=datamodule)

            # [5] Execute the internal training function in a black box
            ray_trainer = LightningTrainer(
                lightning_config=config_builder.build(),
                scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
                run_config=RunConfig(
                    checkpoint_config=CheckpointConfig(
                        num_to_keep=3,
                        checkpoint_score_attribute="val_accuracy",
                        checkpoint_score_order="max",
                    ),
                )
            )
            result = ray_trainer.fit()

            # [6] Load the trained model from an opaque Lightning-specific checkpoint.
            lightning_checkpoint = result.checkpoint
            model = lightning_checkpoint.get_model(MyLightningModule)



    .. tab-item:: (New API) TorchTrainer

        .. This snippet isn't tested because it runs with 4 GPUs, and CI is only run with 1.

        .. testcode::
            :skipif: True

            import os

            import lightning.pytorch as pl

            import ray.train
            from ray.train.torch import TorchTrainer
            from ray.train.lightning import (
                RayDDPStrategy,
                RayLightningEnvironment,
                RayTrainReportCallback,
                prepare_trainer
            )

            def train_func():
                # [1] Create a Lightning model
                model = MyLightningModule(lr=1e-3, feature_dim=128)

                # [2] Report Checkpoint with callback
                ckpt_report_callback = RayTrainReportCallback()

                # [3] Create a Lighting Trainer
                trainer = pl.Trainer(
                    max_epochs=10,
                    log_every_n_steps=100,
                    # New configurations below
                    devices="auto",
                    accelerator="auto",
                    strategy=RayDDPStrategy(),
                    plugins=[RayLightningEnvironment()],
                    callbacks=[ckpt_report_callback],
                )

                # Validate your Lightning trainer configuration
                trainer = prepare_trainer(trainer)

                # [4] Build your datasets on each worker
                datamodule = MyLightningDataModule(batch_size=32)
                trainer.fit(model, datamodule=datamodule)

            # [5] Explicitly define and run the training function
            ray_trainer = TorchTrainer(
                train_func,
                scaling_config=ray.train.ScalingConfig(num_workers=4, use_gpu=True),
                run_config=ray.train.RunConfig(
                    checkpoint_config=ray.train.CheckpointConfig(
                        num_to_keep=3,
                        checkpoint_score_attribute="val_accuracy",
                        checkpoint_score_order="max",
                    ),
                )
            )
            result = ray_trainer.fit()

            # [6] Load the trained model from a simplified checkpoint interface.
            checkpoint: ray.train.Checkpoint = result.checkpoint
            with checkpoint.as_directory() as checkpoint_dir:
                print("Checkpoint contents:", os.listdir(checkpoint_dir))
                checkpoint_path = os.path.join(checkpoint_dir, "checkpoint.ckpt")
                model = MyLightningModule.load_from_checkpoint(checkpoint_path)
