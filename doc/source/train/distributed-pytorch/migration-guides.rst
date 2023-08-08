Migration Guides for LightningTrainer, TransformersTrainer, and AccelerateTrainer
=================================================================================

In version 2.7, we're excited to introduce a unified approach for Torch-based trainers 
using the :class:`~ray.train.torch.TorchTrainer` API. This enhancement allows more 
flexible and centralized integration with PyTorch Lightning, HuggingFace Transformers, 
and HuggingFace Accelerate, all under the TorchTrainer umbrella.

.. tab-set::

    .. tab-item:: PyTorch Lightning

        Previously we proposed `LightningTrainer` in Ray 2.4, which leverages 
        `LightningConfigBuilder` to aggregate configurations for `pl.LightningModule` 
        and `pl.Trainer`. 
        
        It instantiates the model and trainer object internally and runs a pre-defined 
        training loop in a black box.

        .. code-block:: python
            
            from ray.train.lightning import LightningConfigBuilder, LightningTrainer

            config_builder = LightningConfigBuilder()
            config_builder.module(cls=MNISTClassifier, lr=1e-3, feature_dim=128)
            config_builder.checkpointing(monitor="val_accuracy", mode="max", save_top_k=3)
            config_builder.trainer(
                max_epochs=10,
                accelerator="gpu",
                log_every_n_steps=100,
                logger=CSVLogger("./logs"),
            )

            datamodule = MNISTDataModule(batch_size=32)
            config_builder.fit_params(datamodule=datamodule)

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

        The earlier version of our LightningTrainer API was constraining, reducing 
        the users' ability to manage the internal training function.
        
        We're pleased to introduce the new unified TorchTrainer API, which offers 
        enhanced transparency, flexibility, and simplicity. This refined training 
        function aligns closely with standard PyTorch Lightning scripts, ensuring 
        users have complete control and customization options.


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
                model = MNISTClassifier(lr=1e-3, feature_dim=128)
                datamodule = MNISTDataModule(batch_size=32)

                trainer = pl.Trainer(
                    max_epochs=10,
                    accelerator="gpu",
                    log_every_n_steps=100,
                    logger=CSVLogger("./logs"),
                    # New configurations below
                    devices="auto",
                    strategy=RayDDPStrategy(),
                    plugins=[RayLightningEnvironment()],
                    callbacks=[RayTrainReportCallback()],
                )
                trainer = prepare_trainer(trainer)

                trainer.fit(model, datamodule=datamodule)

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

    .. tab-item:: HF Transformers 
    
    .. tab-item:: HF Accelerate

