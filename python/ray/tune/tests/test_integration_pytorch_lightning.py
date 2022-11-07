import os
import shutil
import tempfile
import unittest
import pytorch_lightning as pl
import torch
from ray.tune.result import TRAINING_ITERATION

from torch.utils.data import DataLoader, Dataset

from ray import tune
from ray.tune.integration.pytorch_lightning import (
    TuneReportCallback,
    TuneReportCheckpointCallback,
    _TuneCheckpointCallback,
)


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
        def train(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=1, callbacks=[TuneReportCallback(on="validation_end")]
            )
            trainer.fit(module)

        analysis = tune.run(train, stop={TRAINING_ITERATION: 1})

        self.assertEqual(analysis.trials[0].last_result["avg_val_loss"], 10.0 * 1.1)

    def testReportCallbackNamed(self):
        def train(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=1,
                callbacks=[
                    TuneReportCallback(
                        {"tune_loss": "avg_val_loss"}, on="validation_end"
                    )
                ],
            )
            trainer.fit(module)

        analysis = tune.run(train, stop={TRAINING_ITERATION: 1})

        self.assertEqual(analysis.trials[0].last_result["tune_loss"], 10.0 * 1.1)

    def testCheckpointCallback(self):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmpdir))

        def train(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=1,
                callbacks=[
                    _TuneCheckpointCallback(
                        "trainer.ckpt", on=["batch_end", "train_end"]
                    )
                ],
            )
            trainer.fit(module)

        analysis = tune.run(
            train,
            stop={TRAINING_ITERATION: 10},
            keep_checkpoints_num=100,
            local_dir=tmpdir,
        )

        checkpoints = [
            dir
            for dir in os.listdir(analysis.trials[0].logdir)
            if dir.startswith("checkpoint")
        ]
        # 10 checkpoints after each batch, 1 checkpoint at end
        self.assertEqual(len(checkpoints), 11)

    def testReportCheckpointCallback(self):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(tmpdir))

        def train(config):
            module = _MockModule(10.0, 20.0)
            trainer = pl.Trainer(
                max_epochs=1,
                callbacks=[
                    TuneReportCheckpointCallback(
                        ["avg_val_loss"], "trainer.ckpt", on="validation_end"
                    )
                ],
            )
            trainer.fit(module)

        analysis = tune.run(
            train,
            stop={TRAINING_ITERATION: 10},
            keep_checkpoints_num=100,
            local_dir=tmpdir,
        )

        checkpoints = [
            dir
            for dir in os.listdir(analysis.trials[0].logdir)
            if dir.startswith("checkpoint")
        ]
        # 1 checkpoint after the validation step
        self.assertEqual(len(checkpoints), 1)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(sys.argv[1:] + ["-v", __file__]))
