import pytest

from ray.train import ScalingConfig
from ray.train.lightning import (
    RayDDPStrategy,
    RayFSDPStrategy,
    RayLightningEnvironment,
    RayTrainReportCallback,
)
from ray.train.lightning._lightning_utils import import_lightning
from ray.train.tests.lightning_test_utils import DummyDataModule, LinearModule
from ray.train.torch import TorchTrainer

pl = import_lightning()


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


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
