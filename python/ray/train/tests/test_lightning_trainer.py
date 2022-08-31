from unittest.mock import patch

import pytest

from pytorch_lightning import LightningModule
import ray.data
from torchvision.datasets import MNIST
from torchvision.transforms import ToTensor
from ray.data.extensions import TensorArray
from pandas import DataFrame
from ray.air.config import ScalingConfig
from ray.train.lightning import LightningTrainer
import torch.nn


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()

# model adapted from
# https://pytorch-lightning.readthedocs.io/en/stable/common/lightning_module.html#starter-example
class LitModel(LightningModule):
    def __init__(self, learning_rate=0.02):
        super().__init__()
        self.learning_rate = learning_rate
        self.layer1 = torch.nn.Linear(28 * 28, 10)

    def forward(self, x):
        return torch.relu(self.layer1(x.view(x.size(0), -1)))

    def training_step(self, batch, batch_idx):
        x = batch["image"]
        y = batch["label"]
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("train_loss", loss, on_step=False, on_epoch=True, sync_dist=True)
        return loss

    def validation_step(self, batch, batch_idx):
        x = batch["image"]
        y = batch["label"]
        y_hat = self(x)
        loss = torch.nn.functional.cross_entropy(y_hat, y)
        self.log("val_loss", loss, on_step=False, on_epoch=True, sync_dist=True)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)

def convert_batch_to_pandas(batch):
    images = TensorArray([image.numpy() for image, _ in batch])
    labels = [label for _, label in batch]
    return DataFrame({"image": images, "label": labels})

ray_train = ray.data.read_datasource(
    ray.data.datasource.SimpleTorchDatasource(),
    parallelism=1,
    dataset_factory=lambda: MNIST(
        "~/data", train=True, download=True, transform=ToTensor()
    ),
).map_batches(convert_batch_to_pandas)
ray_validation = ray.data.read_datasource(
    ray.data.datasource.SimpleTorchDatasource(),
    parallelism=1,
    dataset_factory=lambda: MNIST(
        "~/data", train=False, download=True, transform=ToTensor()
    ),
).map_batches(convert_batch_to_pandas)

scaling_config = ScalingConfig(num_workers=2, use_gpu=False)

def test_e2e(ray_start_4_cpus):
    trainer = LightningTrainer(
        LitModel,
        lightning_module_init_config={
            "learning_rate": 0.02
        },
        trainer_init_config={"max_epochs": 4},
        scaling_config=scaling_config,
        datasets={"train": ray_train, "val": ray_validation},
    )
    result = trainer.fit()

    # assert result.metrics["epoch"] == 4  # actually 3, see `max_epochs`
    # assert result.metrics["training_iteration"] == 4  # actually 3, see `max_epochs`
    assert result.checkpoint

    trainer2 = LightningTrainer(
        LitModel,
        lightning_module_init_config={
            "learning_rate": 0.02
        },
        trainer_init_config={"max_epochs": 5},  # this will train for 1 epoch: 5 - 4 = 1
        scaling_config=scaling_config,
        datasets={"train": ray_train, "val": ray_validation},
        resume_from_checkpoint=result.checkpoint,
    )
    result2 = trainer2.fit()

    assert result2.metrics["epoch"] == 5
    assert result2.metrics["training_iteration"] == 1
    assert result2.checkpoint

    # predictor = BatchPredictor.from_checkpoint(
    #     result2.checkpoint,
    #     LightningPredictor,
    # )

    # predictions = predictor.predict(ray.data.from_pandas(prompts))
    # assert predictions.count() == 3


def test_tune(ray_start_4_cpus):
    trainer = LightningTrainer(
        LitModel,
        scaling_config=scaling_config,
        datasets={TRAIN_DATASET_KEY: ray_train, "val": ray_validation},
    )

    tuner = Tuner(
        trainer,
        param_space={
            "lightning_module_init_config": {
                "learning_rate": tune.loguniform(1e-4, 1e-1),
            },
            "trainer_init_config": {
                "max_epochs": tune.grid_search([4, 10, 16]),
            },
        },
        tune_config=TuneConfig(num_samples=2),
    )
    results = tuner.fit()

def test_checkpoint(ray_start_4_cpus):
    lightning_module_init_config = {
        "learning_rate": 0.02
    }
    untrained_model = LitModel(**lightning_module_init_config)
    untrained_model_ckpt = LightningCheckpoint.from_model(untrained_model, lightning_module_init_config)
    assert untrained_model_ckpt.get_model().learning_rate == lightning_module_init_config["learning_rate"]
    assert untrained_model_ckpt.get_model(LitModel).learning_rate == lightning_module_init_config["learning_rate"]

    trainer = LightningTrainer(
        LitModel,
        lightning_module_init_config=lightning_module_init_config,
        trainer_init_config={"max_epochs": 1},
        scaling_config=scaling_config,
        datasets={"train": ray_train, "val": ray_validation},
    )
    checkpoint = LightningCheckpoint.from_checkpoint(trainer.fit().checkpoint)
    with pytest.raises(ValueError, match=MODEL_CLS_KEY):
        checkpoint.get_model()
    assert checkpoint.get_model(LitModel).learning_rate == lightning_module_init_config["learning_rate"]

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
