import pytorch_lightning as pl
from pytorch_lightning.core import datamodule
from ray.train import lightning
import torch.nn as nn
import numpy as np
from ray.train.lightning import LightningConfig, LightningTrainer
from torch.utils.data import DataLoader
import torch
import ray
from ray.train.tests.dummy_preprocessor import DummyPreprocessor
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
import pytest

from ray.train.tests.test_lightning_utils import (
    LinearModule, DoubleLinearModule, DummyDataModule
)


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(accelerator, datasource):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    lightning_config = LightningConfig()
    lightning_config.set_module_class(LinearModule)
    lightning_config.set_module_init_config(input_dim=32, output_dim=4)
    lightning_config.set_trainer_init_config(max_epochs=num_epochs, accelerator=accelerator)

    datamodule = DummyDataModule(batch_size, dataset_size)
    train_loader = datamodule.train_dataloader()
    val_loader = datamodule.val_dataloader()

    if datasource == "dataloader":
        lightning_config.set_trainer_fit_params(train_dataloaders=train_loader, val_dataloaders=val_loader)
    if datasource == "datamodule":
        lightning_config.set_trainer_fit_params(datamodule=datamodule)

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=(accelerator=="gpu"))
    
    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config
    )

    results = trainer.fit()
    # TODO(yunxuanx): Add assertion after support metrics logging
    # assert results.metrics["epoch"] == num_epochs - 1
    # assert results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    # assert "loss" in results.metrics


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_ray_data(accelerator):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    train_dataset = ray.data.from_numpy(dataset)
    val_dataset = ray.data.from_numpy(dataset)

    lightning_config = LightningConfig()
    lightning_config.set_module_class(LinearModule)
    lightning_config.set_module_init_config(input_dim=32, output_dim=4)
    lightning_config.set_trainer_init_config(max_epochs=num_epochs, accelerator=accelerator)

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=(accelerator=="gpu"))
    
    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "val": val_dataset},
        dataset_iter_config={"batch_size": batch_size}
    )

    results = trainer.fit()


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_categorical_ray_data(accelerator):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    input_1 = np.random.rand(dataset_size, 32).astype(np.float32)
    input_2 = np.random.rand(dataset_size, 32).astype(np.float32)
    pd = convert_batch_type_to_pandas({"input_1": input_1, "input_2": input_2})
    train_dataset = ray.data.from_pandas(pd)
    val_dataset = ray.data.from_pandas(pd)

    lightning_config = LightningConfig()
    lightning_config.set_module_class(DoubleLinearModule)
    lightning_config.set_module_init_config(input_dim_1=32, input_dim_2=32, output_dim=4)
    lightning_config.set_trainer_init_config(max_epochs=num_epochs, accelerator=accelerator)

    scaling_config = ray.air.ScalingConfig(num_workers=num_workers, use_gpu=(accelerator=="gpu"))
    
    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "val": val_dataset},
        dataset_iter_config={"batch_size": batch_size}
    )

    results = trainer.fit()

if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
    