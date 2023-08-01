import pytest
import numpy as np

import ray
from ray.train.torch import TorchTrainer
from ray.train.lightning import (
    get_devices,
    RayDDPStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
)

from ray.air import session
from ray.air.config import ScalingConfig
from ray.train.tests.lightning_test_utils import (
    LinearModule,
    DummyDataModule,
    RayTrainReportCallback,
)
import pytorch_lightning as pl


@pytest.fixture
def ray_start_6_cpus_2_gpus():
    address_info = ray.init(num_cpus=6, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("strategy_name", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(
    ray_start_6_cpus_2_gpus, strategy_name, accelerator, datasource
):
    if accelerator == "cpu" and strategy_name == "fsdp":
        return

    num_workers = 2
    num_epochs = 4
    batch_size = 8
    dataset_size = 256

    strategy_map = {"ddp": RayDDPStrategy(), "fsdp": RayFSDPStrategy()}

    def train_loop():
        model = LinearModule(input_dim=32, output_dim=4, strategy=strategy_name)

        strategy = strategy_map[strategy_name]

        trainer = pl.Trainer(
            max_epochs=num_epochs,
            devices=get_devices(),
            accelerator=accelerator,
            strategy=strategy,
            plugins=[RayLightningEnvironment()],
            callbacks=[RayTrainReportCallback()],
        )

        datamodule = DummyDataModule(batch_size, dataset_size)

        if datasource == "dataloader":
            trainer.fit(
                model,
                train_dataloaders=datamodule.train_dataloader(),
                val_dataloaders=datamodule.val_dataloader(),
            )
        if datasource == "datamodule":
            trainer.fit(model, datamodule=datamodule)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=(accelerator == "gpu")),
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["steps"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


@pytest.mark.parametrize("strategy_name", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_ray_data(ray_start_6_cpus_2_gpus, strategy_name, accelerator):
    if accelerator == "cpu" and strategy_name == "fsdp":
        return

    num_epochs = 4
    batch_size = 8
    num_workers = 2
    dataset_size = 256

    strategy_map = {"ddp": RayDDPStrategy(), "fsdp": RayFSDPStrategy()}

    dataset = np.random.rand(dataset_size, 32).astype(np.float32)
    train_dataset = ray.data.from_numpy(dataset)
    val_dataset = ray.data.from_numpy(dataset)

    def train_loop():
        model = LinearModule(input_dim=32, output_dim=4, strategy=strategy_name)

        strategy = strategy_map[strategy_name]

        trainer = pl.Trainer(
            max_epochs=num_epochs,
            devices=get_devices(),
            accelerator=accelerator,
            strategy=strategy,
            plugins=[RayLightningEnvironment()],
            callbacks=[RayTrainReportCallback()],
        )

        train_data_iterable = session.get_dataset_shard("train").iter_torch_batches(
            batch_size=batch_size
        )
        val_data_iterable = session.get_dataset_shard("val").iter_torch_batches(
            batch_size=batch_size
        )

        trainer.fit(
            model,
            train_dataloaders=train_data_iterable,
            val_dataloaders=val_data_iterable,
        )

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=(accelerator == "gpu")),
        datasets={"train": train_dataset, "val": val_dataset},
    )

    results = trainer.fit()
    assert results.metrics["epoch"] == num_epochs - 1
    assert (
        results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
