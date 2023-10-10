import os
from pathlib import Path

import numpy as np
import pytest
import pytorch_lightning as pl
from pytorch_lightning.callbacks import ModelCheckpoint

import ray
from ray.air.util.data_batch_conversion import _convert_batch_type_to_pandas
from ray.train import CheckpointConfig, RunConfig
from ray.train.constants import MODEL_KEY
from ray.train.lightning import LightningConfigBuilder, LightningTrainer
from ray.train.tests.lightning_test_utils import (
    DoubleLinearModule,
    DummyDataModule,
    LinearModule,
)
from ray.train.trainer import TrainingFailedError
from ray.tune import Callback


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_6_cpus():
    address_info = ray.init(num_cpus=6)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class FailureInjectionCallback(Callback):
    """Inject failure at the configured iteration number."""

    def __init__(self, fail_marker_path, num_iters=2):
        self.num_iters = num_iters
        self.fail_marker_path = fail_marker_path

    def on_trial_save(self, iteration, trials, trial, **info):
        if self.fail_marker_path.exists():
            return

        if trial.last_result["training_iteration"] == self.num_iters:
            print(f"Failing after {self.num_iters} iters...")
            self.fail_marker_path.touch()
            raise RuntimeError


def test_native_trainer_restore(ray_start_4_cpus_2_gpus):
    """Test restoring trainer in the Lightning's native way."""
    num_epochs = 2
    batch_size = 8
    num_workers = 2
    dataset_size = 64

    # Create simple categorical ray dataset
    input_1 = np.random.rand(dataset_size, 32).astype(np.float32)
    input_2 = np.random.rand(dataset_size, 32).astype(np.float32)
    pd = _convert_batch_type_to_pandas({"input_1": input_1, "input_2": input_2})
    train_dataset = ray.data.from_pandas(pd)
    val_dataset = ray.data.from_pandas(pd)

    config_builder = (
        LightningConfigBuilder()
        .module(
            DoubleLinearModule,
            input_dim_1=32,
            input_dim_2=32,
            output_dim=4,
        )
        .trainer(max_epochs=num_epochs, accelerator="gpu")
    )

    lightning_config = config_builder.build()

    scaling_config = ray.train.ScalingConfig(num_workers=num_workers, use_gpu=True)

    trainer = LightningTrainer(
        lightning_config=lightning_config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "val": val_dataset},
        datasets_iter_config={"batch_size": batch_size},
    )
    results = trainer.fit()

    # Resume training for another 2 epochs
    num_epochs += 2

    with results.checkpoint.as_directory() as tmpdir:
        ckpt_path = os.path.join(tmpdir, MODEL_KEY)

        lightning_config = (
            config_builder.fit_params(ckpt_path=ckpt_path)
            .trainer(max_epochs=num_epochs)
            .build()
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
            results.metrics["step"]
            == num_epochs * dataset_size / num_workers / batch_size
        )
        assert "loss" in results.metrics
        assert "val_loss" in results.metrics
        assert results.checkpoint


@pytest.mark.parametrize("resume_from_ckpt_path", [True, False])
def test_air_trainer_restore(
    ray_start_6_cpus, monkeypatch, tmpdir, resume_from_ckpt_path
):
    """Test restore for LightningTrainer from a failed/interrupted trail."""
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", str(tmpdir))
    exp_name = "air_trainer_restore_test"

    datamodule = DummyDataModule(8, 256)
    train_loader = datamodule.train_dataloader()
    val_loader = datamodule.val_dataloader()

    max_epochs = 5
    init_epoch = 1 if resume_from_ckpt_path else 0
    error_epoch = 2

    # init_epoch -> [error_epoch] -> max_epoch
    training_iterations = max_epochs - init_epoch

    lightning_config = (
        LightningConfigBuilder()
        .module(LinearModule, input_dim=32, output_dim=4)
        .trainer(max_epochs=max_epochs, accelerator="cpu")
        .fit_params(train_dataloaders=train_loader, val_dataloaders=val_loader)
    )

    if resume_from_ckpt_path:
        ckpt_dir = f"{tmpdir}/ckpts"
        callback = ModelCheckpoint(dirpath=ckpt_dir, save_last=True)
        pl_trainer = pl.Trainer(
            max_epochs=init_epoch, accelerator="cpu", callbacks=[callback]
        )
        pl_model = LinearModule(input_dim=32, output_dim=4)
        pl_trainer.fit(pl_model, train_dataloaders=train_loader)
        lightning_config.fit_params(ckpt_path=f"{ckpt_dir}/last.ckpt")

    scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=False)

    fail_marker_path = Path(tmpdir) / "fail_marker"

    trainer = LightningTrainer(
        lightning_config=lightning_config.build(),
        scaling_config=scaling_config,
        run_config=RunConfig(
            name=exp_name,
            storage_path=str(tmpdir),
            checkpoint_config=CheckpointConfig(num_to_keep=2),
            callbacks=[
                FailureInjectionCallback(
                    fail_marker_path=fail_marker_path, num_iters=error_epoch
                )
            ],
        ),
    )

    with pytest.raises(TrainingFailedError):
        result = trainer.fit()

    trainer = LightningTrainer.restore(str(tmpdir / exp_name))
    result = trainer.fit()

    assert not result.error
    assert result.metrics["training_iteration"] == training_iterations
    assert tmpdir / exp_name in Path(result.path).parents

    # TODO(justinvyu): fix this in 2.8
    # The experiment state file is not updated to point to the latest checkpoint
    # before we raise a failure. Trainer.restore() always try to restore from the
    # previous checkpoint.
    # iterations_since_restore = max_epochs - init_epoch - error_epoch
    # assert result.metrics["iterations_since_restore"] == iterations_since_restore


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
