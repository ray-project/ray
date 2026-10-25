import os
from tempfile import TemporaryDirectory

import numpy as np
import pytest

import ray
from ray.train import Checkpoint, ScalingConfig
from ray.train.lightning import (
    RayDDPStrategy,
    RayDeepSpeedStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback,
)
from ray.train.lightning._lightning_utils import import_lightning
from ray.train.tests.lightning_test_utils import DummyDataModule, LinearModule
from ray.train.torch import TorchTrainer

pl = import_lightning()


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


def test_custom_checkpoint_callback(ray_start_6_cpus_2_gpus):
    """A custom callback must save on every worker to avoid a DDP deadlock."""

    class CustomCheckpointCallback(pl.Callback):
        def on_train_epoch_end(self, trainer, pl_module):
            with TemporaryDirectory() as tmpdir:
                checkpoint = None
                if trainer.current_epoch == 0:
                    checkpoint_path = os.path.join(tmpdir, "ckpt.pt")
                    trainer.save_checkpoint(checkpoint_path, weights_only=False)
                    checkpoint = Checkpoint.from_directory(tmpdir)
                ray.train.report(
                    {"epoch": trainer.current_epoch}, checkpoint=checkpoint
                )
                trainer.strategy.barrier()

    def train_loop():
        trainer = pl.Trainer(
            max_epochs=1,
            devices="auto",
            accelerator="cpu",
            strategy=RayDDPStrategy(),
            plugins=[RayLightningEnvironment()],
            callbacks=[CustomCheckpointCallback()],
        )
        trainer.fit(LinearModule(input_dim=32, output_dim=4), DummyDataModule(8, 16))

    result = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    ).fit()

    with result.checkpoint.as_directory() as checkpoint_dir:
        assert os.path.exists(os.path.join(checkpoint_dir, "ckpt.pt"))


def test_report_callback_checkpoint_frequency(ray_start_6_cpus_2_gpus):
    """`checkpoint_frequency` should skip reporting (metrics and checkpoint) on
    non-matching epochs."""

    num_workers = 2
    num_epochs = 6
    batch_size = 8
    dataset_size = 256
    checkpoint_frequency = 3

    def train_loop():
        model = LinearModule(input_dim=32, output_dim=4, strategy="ddp")

        trainer = pl.Trainer(
            max_epochs=num_epochs,
            devices="auto",
            accelerator="cpu",
            strategy=RayDDPStrategy(),
            plugins=[RayLightningEnvironment()],
            callbacks=[
                RayTrainReportCallback(checkpoint_frequency=checkpoint_frequency)
            ],
        )
        datamodule = DummyDataModule(batch_size, dataset_size)
        trainer.fit(model, datamodule=datamodule)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=False),
    )
    result = trainer.fit()

    # The last report comes from the last checkpointed epoch.
    assert result.metrics["epoch"] == num_epochs - 1

    # Only epochs where (epoch + 1) % checkpoint_frequency == 0 should have
    # reported: epochs 2 and 5 out of 0..5.
    expected_checkpoints = num_epochs // checkpoint_frequency
    assert len(result.best_checkpoints) == expected_checkpoints
    checkpointed_epochs = sorted(
        metrics["epoch"] for _, metrics in result.best_checkpoints
    )
    assert checkpointed_epochs == [
        epoch for epoch in range(num_epochs) if (epoch + 1) % checkpoint_frequency == 0
    ]


def test_report_callback_invalid_checkpoint_frequency():
    """`checkpoint_frequency` must be a positive integer."""
    with pytest.raises(ValueError):
        RayTrainReportCallback(checkpoint_frequency=0)
    with pytest.raises(TypeError):
        RayTrainReportCallback(checkpoint_frequency=1.5)
    with pytest.raises(TypeError):
        RayTrainReportCallback(checkpoint_frequency="2")
    with pytest.raises(TypeError):
        RayTrainReportCallback(checkpoint_frequency=True)


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
    all_files = os.listdir(f"{result.checkpoint.path}/checkpoint.ckpt/checkpoint")
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
