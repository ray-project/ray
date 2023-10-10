import shutil
import tempfile
import unittest
import pytorch_lightning as pl
from ray.train import CheckpointConfig
import torch
from ray.air.constants import TRAINING_ITERATION

from torch.utils.data import DataLoader, Dataset

from ray import train, tune
from ray.tune.integration.pytorch_lightning import TuneReportCheckpointCallback


class _MockDataset(Dataset):
    def __init__(self, values):
        self.values = values

    def __getitem__(self, index):
        return self.values[index]

    def __len__(self):
        return len(self.values)


class _MockModule(pl.LightningModule):
    def __init__(self, loss, acc):
        super().__init__()

        self.loss = torch.tensor(loss)
        self.acc = torch.tensor(acc)

    def forward(self, *args, **kwargs):
        return self.loss

    def backward(self, loss, optimizer, optimizer_idx):
        return None

    def training_step(self, train_batch, batch_idx):
        return {"loss": self.loss, "acc": self.acc}

    def validation_step(self, val_batch, batch_idx):
        return {"val_loss": self.loss * 1.1, "val_acc": self.acc * 0.9}

    def validation_epoch_end(self, outputs):
        avg_val_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
        avg_val_acc = torch.stack([x["val_acc"] for x in outputs]).mean()
        self.log("avg_val_loss", avg_val_loss)
        self.log("avg_val_acc", avg_val_acc)

    def configure_optimizers(self):
        return None

    def train_dataloader(self):
        return DataLoader(_MockDataset(list(range(10))), batch_size=1)

    def val_dataloader(self):
        return DataLoader(_MockDataset(list(range(10))), batch_size=1)


class PyTorchLightningIntegrationTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testReportCallbackUnnamed(self):
        def train_fn(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=1,
                callbacks=[
                    TuneReportCheckpointCallback(
                        on="validation_end", save_checkpoints=False
                    )
                ],
            )
            trainer.fit(module)

        analysis = tune.run(train_fn, stop={TRAINING_ITERATION: 1})

        self.assertEqual(analysis.trials[0].last_result["avg_val_loss"], 10.0 * 1.1)

    def testReportCallbackNamed(self):
        def train_fn(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=1,
                callbacks=[
                    TuneReportCheckpointCallback(
                        metrics={"tune_loss": "avg_val_loss"},
                        on="validation_end",
                        save_checkpoints=False,
                    )
                ],
            )
            trainer.fit(module)

        analysis = tune.run(train_fn, stop={TRAINING_ITERATION: 1})

        self.assertEqual(analysis.trials[0].last_result["tune_loss"], 10.0 * 1.1)

    def testCheckpointCallback(self):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmpdir))

        def train_fn(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=10,
                callbacks=[
                    TuneReportCheckpointCallback(
                        filename="trainer.ckpt", on=["train_epoch_end"]
                    )
                ],
            )
            trainer.fit(module)

        checkpoint_config = CheckpointConfig(num_to_keep=100)
        tuner = tune.Tuner(
            train_fn,
            run_config=train.RunConfig(
                stop={TRAINING_ITERATION: 10},
                storage_path=tmpdir,
                checkpoint_config=checkpoint_config,
            ),
        )
        results = tuner.fit()

        # 1 checkpoint per epoch
        self.assertEqual(len(results[0].best_checkpoints), 10)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(sys.argv[1:] + ["-v", __file__]))
