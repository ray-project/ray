import os

import pytest

import ray
from ray.train import CheckpointConfig, RunConfig, ScalingConfig
from ray.train.lightning import (
    RayDDPStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback,
)
from ray.train.lightning._lightning_utils import import_lightning
from ray.train.tests.lightning_test_utils import DummyDataModule, LinearModule
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.api.report_config import CheckpointUploadMode
from ray.train.v2.api.validation_config import ValidationConfig, ValidationTaskConfig

pl = import_lightning()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


@pytest.mark.parametrize("strategy_name", ["ddp", "fsdp"])
@pytest.mark.parametrize("accelerator", ["cpu"])
# @pytest.mark.parametrize("accelerator", ["cpu", "gpu"])  # TODO: Enable GPU test
@pytest.mark.parametrize("datasource", ["dataloader", "datamodule"])
def test_trainer_with_native_dataloader(
    ray_start_4_cpus, strategy_name, accelerator, datasource
):
    """Test basic ddp and fsdp training with dataloader and datamodule."""

    if accelerator == "cpu" and strategy_name == "fsdp":
        return

    num_workers = 2
    num_epochs = 1
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


def test_async_checkpointing_and_validation(ray_start_4_cpus, tmp_path):
    """Test lightning training with async checkpointing and validation."""

    num_workers = 2
    num_epochs = 2
    batch_size = 8
    dataset_size = 256

    @ray.remote
    class TmpdirPrefixActor:
        def __init__(self):
            self.tmpdir_prefixes = []

        def set_tmpdir_prefix(self, tmpdir_prefix):
            self.tmpdir_prefixes.append(tmpdir_prefix)

        def get_tmpdir_prefixes(self):
            return self.tmpdir_prefixes

    tmpdir_prefix_actor = TmpdirPrefixActor.remote()

    def validation_fn(checkpoint):
        assert checkpoint.path is not None
        checkpoint_file = checkpoint.path + "/checkpoint.ckpt"
        assert os.path.exists(
            checkpoint_file
        ), f"Checkpoint file not found: {checkpoint_file}"
        return {"val_score": 1}

    def train_loop():
        model = LinearModule(input_dim=32, output_dim=4, strategy="ddp")
        callback = RayTrainReportCallback(
            checkpoint_upload_mode=CheckpointUploadMode.ASYNC,
            validation=ValidationTaskConfig(fn_kwargs={}),
        )
        ray.get(tmpdir_prefix_actor.set_tmpdir_prefix.remote(callback.tmpdir_prefix))
        trainer = pl.Trainer(
            max_epochs=num_epochs,
            devices="auto",
            accelerator="cpu",
            strategy=RayDDPStrategy(),
            plugins=[RayLightningEnvironment()],
            callbacks=[callback],
        )

        datamodule = DummyDataModule(batch_size, dataset_size)
        trainer.fit(model, datamodule=datamodule)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(num_workers=num_workers),
        validation_config=ValidationConfig(fn=validation_fn),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            checkpoint_config=CheckpointConfig(
                num_to_keep=1, checkpoint_score_attribute="val_score"
            ),
        ),
    )

    results = trainer.fit()
    assert results.error is None
    assert "loss" in results.metrics
    assert results.best_checkpoints is not None
    assert len(results.best_checkpoints) == 1
    assert results.best_checkpoints[0][1]["val_score"] == 1
    # Seems pyarrow.fs.FileSystem's delete_dir can leave an empty dir behind.
    for path in ray.get(tmpdir_prefix_actor.get_tmpdir_prefixes.remote()):
        assert not os.path.exists(path) or not any(os.scandir(path))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
