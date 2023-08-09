import pytest
import numpy as np
import os

import ray
from ray.train.torch import TorchTrainer
from ray.train.lightning import (
    RayDeepSpeedStrategy,
    RayDDPStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback,
)

from ray.train import ScalingConfig
from ray.train.tests.lightning_test_utils import (
    LinearModule,
    DummyDataModule,
)
import pytorch_lightning as pl


@pytest.fixture
def ray_start_6_cpus_2_gpus():
    address_info = ray.init(num_cpus=6, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_6_cpus_4_gpus():
    address_info = ray.init(num_cpus=6, num_gpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("strategy_name", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(
    ray_start_6_cpus_2_gpus, strategy_name, accelerator, datasource
):
    """Test basic ddp and fsdp training with dataloader and datamodule."""

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
            devices="auto",
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
        results.metrics["step"] == num_epochs * dataset_size / num_workers / batch_size
    )
    assert "loss" in results.metrics
    assert "val_loss" in results.metrics


@pytest.mark.parametrize("strategy_name", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu", "gpu"])
def test_trainer_with_ray_data(ray_start_6_cpus_2_gpus, strategy_name, accelerator):
    """Test Data integration with ddp and fsdp."""

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
            devices="auto",
            accelerator=accelerator,
            strategy=strategy,
            plugins=[RayLightningEnvironment()],
            callbacks=[RayTrainReportCallback()],
        )

        train_data_iterable = ray.train.get_dataset_shard("train").iter_torch_batches(
            batch_size=batch_size
        )
        val_data_iterable = ray.train.get_dataset_shard("val").iter_torch_batches(
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


@pytest.mark.parametrize("stage", [1, 2, 3])
def test_deepspeed_zero_stages(ray_start_6_cpus_4_gpus, tmpdir, stage):
    num_epochs = 5
    batch_size = 8
    num_workers = 4
    dataset_size = 256

    def train_loop():
        model = LinearModule(input_dim=32, output_dim=4, strategy="deepspeed")

        strategy = RayDeepSpeedStrategy(stage=stage)

        trainer = pl.Trainer(
            max_epochs=num_epochs,
            devices="auto",
            accelerator="gpu",
            strategy=strategy,
            plugins=[RayLightningEnvironment()],
            callbacks=[RayTrainReportCallback()],
        )

        datamodule = DummyDataModule(batch_size, dataset_size)
        trainer.fit(model, datamodule=datamodule)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=True),
    )

    result = trainer.fit()

    # Check all deepspeed model/optimizer shards are saved
    all_files = os.listdir(
        f"{result.checkpoint.path}/ckpt_epoch_{num_epochs-1}/checkpoint.ckpt"
    )
    for rank in range(num_workers):
        full_model = "mp_rank_00_model_states.pt"
        model_shard = f"zero_pp_rank_{rank}_mp_rank_00_model_states.pt"
        optim_shard = f"zero_pp_rank_{rank}_mp_rank_00_optim_states.pt"

        assert (
            optim_shard in all_files
        ), f"[stage-{stage}] Optimizer states `{optim_shard}` doesn't exist!"

        if stage == 3:
            assert (
                model_shard in all_files
            ), f"[stage-{stage}] Model states {model_shard} doesn't exist!"
        else:
            assert (
                full_model in all_files
            ), f"[stage-{stage}] Model states {full_model} doesn't exist!"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
