.. _migration-guide:

Migration Guides for LightningTrainer, TransformersTrainer, and AccelerateTrainer
=================================================================================

In Ray 2.7, we're excited to introduce a unified approach for Torch-based training frameworks
using the :class:`~ray.train.torch.TorchTrainer` API. 

Framework-specific trainers include **LightningTrainer**, **TransformersTrainer**, and **AccelerateTrainer** 
will be deprecated. Users can now run the PyTorch Lightning, HuggingFace Transformers, 
and HuggingFace Accelerate code all through **TorchTrainer**.

This change allows users to make minimal changes to their existing code to accommodate Ray Train,
And provide greater flexibility and transparency for training logic.

.. tab-set::

    .. tab-item:: PyTorch Lightning

        The `LightningTrainer` was added in Ray 2.4, and exposes a  
        `LightningConfigBuilder` to define configurations for `pl.LightningModule` 
        and `pl.Trainer`. 
        
        It then instantiates the model and trainer objects and runs a pre-defined 
        training loop in a black box.

        .. code-block:: python
            
            from ray.train.lightning import LightningConfigBuilder, LightningTrainer

            config_builder = LightningConfigBuilder()
            # [1] Collect model configs
            config_builder.module(cls=MNISTClassifier, lr=1e-3, feature_dim=128)
            # [2] Collect checkpointing configs
            config_builder.checkpointing(monitor="val_accuracy", mode="max", save_top_k=3)
            # [3] Collect pl.Trainer configs
            config_builder.trainer(
                max_epochs=10,
                accelerator="gpu",
                log_every_n_steps=100,
                logger=CSVLogger("./logs"),
            )

            # [4] Collect datasets as config
            datamodule = MNISTDataModule(batch_size=32)
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
            ray_trainer.fit()

        This version of our LightningTrainer API was constraining and limited 
        the users' ability to manage the training functionality.
        
        We're pleased to introduce the newly unified TorchTrainer API, which offers 
        enhanced transparency, flexibility, and simplicity. This API is more aligned
        with standard PyTorch Lightning scripts, ensuring users have better 
        control over their native Lightning code.


        .. code-block:: python
            
            import pytorch_lightning as pl
            from ray.train.torch import TorchTrainer
            from ray.train.lightning import (
                RayDDPStrategy, 
                RayLightningEnvironment,
                RayTrainReportCallback,
                prepare_trainer
            ) 

            def train_func_per_worker():
                # [1] Create a Lightning model
                model = MNISTClassifier(lr=1e-3, feature_dim=128)

                # [2] Report Checkpoint with callback
                ckpt_report_callback = RayTrainReportCallback()

                # [3] Create a Lighting Trainer
                trainer = pl.Trainer(
                    max_epochs=10,
                    accelerator="gpu",
                    log_every_n_steps=100,
                    logger=CSVLogger("./logs"),
                    # New configurations below
                    devices="auto",
                    strategy=RayDDPStrategy(),
                    plugins=[RayLightningEnvironment()],
                    callbacks=[ckpt_report_callback],
                )
                trainer = prepare_trainer(trainer)

                # [4] Build your datasets
                datamodule = MNISTDataModule(batch_size=32)
                trainer.fit(model, datamodule=datamodule)

            # [5] Explicitly define and run the training function
            ray_trainer = TorchTrainer(
                train_func_per_worker,
                scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
                run_config=RunConfig(
                    checkpoint_config=CheckpointConfig(
                        num_to_keep=3,
                        checkpoint_score_attribute="val_accuracy",
                        checkpoint_score_order="max",
                    ),
                )
            )

            ray_trainer.fit()

        For more information, please refer to our :ref:`TorchTrainer User Guide <train-pytorch-overview>`.
