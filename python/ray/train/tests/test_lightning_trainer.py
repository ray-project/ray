import pytest
import numpy as np

import ray
from ray.train.lightning import LightningConfigBuilder, LightningTrainer
from ray.air.util.data_batch_conversion import _convert_batch_type_to_pandas
from ray.train.tests.lightning_test_utils import (
    LinearModule,
    DoubleLinearModule,
    DummyDataModule,
)


@pytest.fixture
def ray_start_6_cpus_2_gpus():
    address_info = ray.init(num_cpus=6, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_config_builder():
    class DummyClass:
        def __init__(self) -> None:
            pass

    with pytest.raises(
        ValueError, match="'module_class' must be a subclass of 'pl.LightningModule'!"
    ):
        LightningConfigBuilder().module(cls=DummyClass).build()

    with pytest.raises(
        ValueError, match="'module_class' must be a class, not a class instance."
    ):
        model = LinearModule(1, 1)
        LightningConfigBuilder().module(cls=model).build()

    with pytest.raises(
        TypeError, match="trainer\(\) takes 1 positional argument but 3 were given"
    ):
        LightningConfigBuilder().module(cls=LinearModule).trainer(10, 100)

    with pytest.raises(
        ValueError, match="LightningTrainer currently supports 'ddp' and 'fsdp'"
    ):
        LightningConfigBuilder().strategy(name="dummy_strategy")

    config = (
        LightningConfigBuilder()
        .module(cls=LinearModule, input_dim=10)
        .trainer(log_every_n_steps=100)
        .fit_params(datamodule=DummyDataModule())
        .build()
    )
    assert config["_module_init_config"]["input_dim"] == 10
    assert config["_trainer_init_config"]["log_every_n_steps"] == 100
    assert not config["_strategy_config"]
    assert not config["_model_checkpoint_config"]


@pytest.mark.parametrize("strategy", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(
    ray_start_6_cpus_2_gpus, strategy, accelerator, datasource
):
    if accelerator == "cpu" and strategy == "fsdp":
        return

    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    config_builder = (
        LightningConfigBuilder()
        .module(LinearModule, input_dim=32, output_dim=4, strategy=strategy)
        .trainer(max_epochs=num_epochs, accelerator=accelerator)
        .strategy(strategy)
    )

    datamodule = DummyDataModule(batch_size, dataset_size)
    train_loader = datamodule.train_dataloader()
    val_loader = datamodule.val_dataloader()

    if datasource == "dataloader":
        config_builder.fit_params(
            train_dataloaders=train_loader, val_dataloaders=val_loader
        )
    if datasource == "datamodule":
        config_builder.fit_params(datamodule=datamodule)

    scaling_config = ray.air.ScalingConfig(
        num_workers=num_workers, use_gpu=(accelerator == "gpu")
    )

    trainer = LightningTrainer(
        lightning_config=config_builder.build(), scaling_config=scaling_config
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


@pytest.mark.parametrize("strategy", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_ray_data(ray_start_6_cpus_2_gpus, strategy, accelerator):
    if accelerator == "cpu" and strategy == "fsdp":
        return

    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    train_dataset = ray.data.from_numpy(dataset)
    val_dataset = ray.data.from_numpy(dataset)

    lightning_config = (
        LightningConfigBuilder()
        .module(cls=LinearModule, input_dim=32, output_dim=4, strategy=strategy)
        .trainer(max_epochs=num_epochs, accelerator=accelerator)
        .strategy(strategy)
        .build()
    )

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


@pytest.mark.parametrize("accelerator", ["cpu"])
def test_trainer_with_categorical_ray_data(ray_start_6_cpus_2_gpus, accelerator):
    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    # Create simple categorical ray dataset
    input_1 = np.random.rand(dataset_size, 32).astype(np.float32)
    input_2 = np.random.rand(dataset_size, 32).astype(np.float32)
    pd = _convert_batch_type_to_pandas({"input_1": input_1, "input_2": input_2})
    train_dataset = ray.data.from_pandas(pd)
    val_dataset = ray.data.from_pandas(pd)

    lightning_config = (
        LightningConfigBuilder()
        .module(
            DoubleLinearModule,
            input_dim_1=32,
            input_dim_2=32,
            output_dim=4,
        )
        .trainer(max_epochs=num_epochs, accelerator=accelerator)
        .build()
    )

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
