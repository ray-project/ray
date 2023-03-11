import numpy as np
from ray.train.lightning import LightningConfigBuilder, LightningTrainer
import ray
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
import pytest

from ray.train.tests._lightning_utils import (
    LinearModule,
    DoubleLinearModule,
    DummyDataModule,
)


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(accelerator, datasource):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    config_builder = LightningConfigBuilder()
    config_builder.set_module_class(LinearModule)
    config_builder.set_module_init_config(input_dim=32, output_dim=4)
    config_builder.set_trainer_init_config(
        max_epochs=num_epochs, accelerator=accelerator
    )
    lightning_config = config_builder.build()

    datamodule = DummyDataModule(batch_size, dataset_size)
    train_loader = datamodule.train_dataloader()
    val_loader = datamodule.val_dataloader()

    if datasource == "dataloader":
        config_builder.set_trainer_fit_params(
            train_dataloaders=train_loader, val_dataloaders=val_loader
        )
    if datasource == "datamodule":
        config_builder.set_trainer_fit_params(datamodule=datamodule)

    scaling_config = ray.air.ScalingConfig(
        num_workers=num_workers, use_gpu=(accelerator == "gpu")
    )

    trainer = LightningTrainer(
        lightning_config=lightning_config, scaling_config=scaling_config
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_ray_data(accelerator):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    train_dataset = ray.data.from_numpy(dataset)
    val_dataset = ray.data.from_numpy(dataset)

    config_builder = LightningConfigBuilder()
    config_builder.set_module_class(LinearModule)
    config_builder.set_module_init_config(input_dim=32, output_dim=4)
    config_builder.set_trainer_init_config(
        max_epochs=num_epochs, accelerator=accelerator
    )
    lightning_config = config_builder.build()

    scaling_config = ray.air.ScalingConfig(
        num_workers=num_workers, use_gpu=(accelerator == "gpu")
    )

    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "val": val_dataset},
        datasets_iter_config={"batch_size": batch_size},
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


@pytest.mark.parametrize("accelerator", ["gpu"])
def test_trainer_with_categorical_ray_data(accelerator):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    # Create simple categorical ray dataset
    input_1 = np.random.rand(dataset_size, 32).astype(np.float32)
    input_2 = np.random.rand(dataset_size, 32).astype(np.float32)
    pd = convert_batch_type_to_pandas({"input_1": input_1, "input_2": input_2})
    train_dataset = ray.data.from_pandas(pd)
    val_dataset = ray.data.from_pandas(pd)

    config_builder = LightningConfigBuilder()
    config_builder.set_module_class(DoubleLinearModule)
    config_builder.set_module_init_config(input_dim_1=32, input_dim_2=32, output_dim=4)
    config_builder.set_trainer_init_config(
        max_epochs=num_epochs, accelerator=accelerator
    )

    lightning_config = config_builder.build()
    scaling_config = ray.air.ScalingConfig(
        num_workers=num_workers, use_gpu=(accelerator == "gpu")
    )

    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "val": val_dataset},
        datasets_iter_config={"batch_size": batch_size},
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics
    assert results.checkpoint


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
