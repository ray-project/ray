# flake8: noqa
# yapf: disable

# __import_lightning_begin__
import torch
import pytorch_lightning as pl
from torch.utils.data import DataLoader, random_split
from torch.nn import functional as F
from torchvision.datasets import MNIST
from torchvision import transforms
import os
# __import_lightning_end__

# __import_tune_begin__
import shutil
from functools import partial
from tempfile import mkdtemp
from pytorch_lightning.callbacks import Callback
from pytorch_lightning.loggers import TensorBoardLogger
from pytorch_lightning.utilities.cloud_io import load as pl_load
from ray import tune
from ray.tune import CLIReporter
from ray.tune.schedulers import ASHAScheduler, PopulationBasedTraining
# __import_tune_end__


# __lightning_begin__
class LightningMNISTClassifier(pl.LightningModule):
    """
    This has been adapted from
    https://towardsdatascience.com/from-pytorch-to-pytorch-lightning-a-gentle-introduction-b371b7caaf09
    """

    def __init__(self, config, data_dir=None):
        super(LightningMNISTClassifier, self).__init__()

        self.data_dir = data_dir or os.getcwd()

        self.layer_1_size = config["layer_1_size"]
        self.layer_2_size = config["layer_2_size"]
        self.lr = config["lr"]
        self.batch_size = config["batch_size"]

        # mnist images are (1, 28, 28) (channels, width, height)
        self.layer_1 = torch.nn.Linear(28 * 28, self.layer_1_size)
        self.layer_2 = torch.nn.Linear(self.layer_1_size, self.layer_2_size)
        self.layer_3 = torch.nn.Linear(self.layer_2_size, 10)

    def forward(self, x):
        batch_size, channels, width, height = x.size()
        x = x.view(batch_size, -1)

        x = self.layer_1(x)
        x = torch.relu(x)

        x = self.layer_2(x)
        x = torch.relu(x)

        x = self.layer_3(x)
        x = torch.log_softmax(x, dim=1)

        return x

    def cross_entropy_loss(self, logits, labels):
        return F.nll_loss(logits, labels)

    def accuracy(self, logits, labels):
        _, predicted = torch.max(logits.data, 1)
        correct = (predicted == labels).sum().item()
        accuracy = correct / len(labels)
        return torch.tensor(accuracy)

    def training_step(self, train_batch, batch_idx):
        x, y = train_batch
        logits = self.forward(x)
        loss = self.cross_entropy_loss(logits, y)
        accuracy = self.accuracy(logits, y)

        logs = {"ptl/train_loss": loss, "ptl/train_accuracy": accuracy}
        return {"loss": loss, "log": logs}

    def validation_step(self, val_batch, batch_idx):
        x, y = val_batch
        logits = self.forward(x)
        loss = self.cross_entropy_loss(logits, y)
        accuracy = self.accuracy(logits, y)

        return {"val_loss": loss, "val_accuracy": accuracy}

    def validation_epoch_end(self, outputs):
        avg_loss = torch.stack([x["val_loss"] for x in outputs]).mean()
        avg_acc = torch.stack([x["val_accuracy"] for x in outputs]).mean()
        logs = {"ptl/val_loss": avg_loss, "ptl/val_accuracy": avg_acc}

        return {
            "avg_val_loss": avg_loss,
            "avg_val_accuracy": avg_acc,
            "log": logs
        }

    @staticmethod
    def download_data(data_dir):
        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307, ), (0.3081, ))
        ])
        return MNIST(data_dir, train=True, download=True, transform=transform)

    def prepare_data(self):
        mnist_train = self.download_data(self.data_dir)

        self.mnist_train, self.mnist_val = random_split(
            mnist_train, [55000, 5000])

    def train_dataloader(self):
        return DataLoader(self.mnist_train, batch_size=int(self.batch_size))

    def val_dataloader(self):
        return DataLoader(self.mnist_val, batch_size=int(self.batch_size))

    def configure_optimizers(self):
        optimizer = torch.optim.Adam(self.parameters(), lr=self.lr)
        return optimizer


def train_mnist(config):
    model = LightningMNISTClassifier(config)
    trainer = pl.Trainer(max_epochs=10, show_progress_bar=False)

    trainer.fit(model)
# __lightning_end__


# __tune_callback_begin__
class TuneReportCallback(Callback):
    def on_validation_end(self, trainer, pl_module):
        tune.report(
            loss=trainer.callback_metrics["avg_val_loss"].item(),
            mean_accuracy=trainer.callback_metrics["avg_val_accuracy"].item())
# __tune_callback_end__


# __tune_train_begin__
def train_mnist_tune(config, data_dir=None, num_epochs=10, num_gpus=0):
    model = LightningMNISTClassifier(config, data_dir)
    trainer = pl.Trainer(
        max_epochs=num_epochs,
        gpus=num_gpus,
        logger=TensorBoardLogger(
            save_dir=tune.get_trial_dir(), name="", version="."),
        progress_bar_refresh_rate=0,
        callbacks=[TuneReportCallback()])

    trainer.fit(model)
# __tune_train_end__


# __tune_checkpoint_callback_begin__
class CheckpointCallback(Callback):
    def on_validation_end(self, trainer, pl_module):
        path = tune.make_checkpoint_dir(trainer.global_step)
        trainer.save_checkpoint(os.path.join(path, "checkpoint"))
        tune.save_checkpoint(path)
# __tune_checkpoint_callback_end__


# __tune_train_checkpoint_begin__
def train_mnist_tune_checkpoint(
    config,
    checkpoint=None,
    data_dir=None,
    num_epochs=10,
    num_gpus=0):
    trainer = pl.Trainer(
        max_epochs=num_epochs,
        gpus=num_gpus,
        logger=TensorBoardLogger(
            save_dir=tune.get_trial_dir(), name="", version="."),
        progress_bar_refresh_rate=0,
        callbacks=[CheckpointCallback(),
                   TuneReportCallback()])
    if checkpoint:
        # Currently, this leads to errors:
        # model = LightningMNISTClassifier.load_from_checkpoint(
        #     os.path.join(checkpoint, "checkpoint"))
        # Workaround:
        ckpt = pl_load(
            os.path.join(checkpoint, "checkpoint"),
            map_location=lambda storage, loc: storage)
        model = LightningMNISTClassifier._load_model_state(ckpt, config=config)
        trainer.current_epoch = ckpt["epoch"]
    else:
        model = LightningMNISTClassifier(
            config=config, data_dir=data_dir)

    trainer.fit(model)
# __tune_train_checkpoint_end__


# __tune_asha_begin__
def tune_mnist_asha(num_samples=10, num_epochs=10, gpus_per_trial=0):
    data_dir = mkdtemp(prefix="mnist_data_")
    LightningMNISTClassifier.download_data(data_dir)

    config = {
        "layer_1_size": tune.choice([32, 64, 128]),
        "layer_2_size": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128]),
    }

    scheduler = ASHAScheduler(
        metric="loss",
        mode="min",
        max_t=num_epochs,
        grace_period=1,
        reduction_factor=2)

    reporter = CLIReporter(
        parameter_columns=["layer_1_size", "layer_2_size", "lr", "batch_size"],
        metric_columns=["loss", "mean_accuracy", "training_iteration"])

    tune.run(
        partial(
            train_mnist_tune,
            data_dir=data_dir,
            num_epochs=num_epochs,
            num_gpus=gpus_per_trial),
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        name="tune_mnist_asha")

    shutil.rmtree(data_dir)
# __tune_asha_end__


# __tune_pbt_begin__
def tune_mnist_pbt(num_samples=10, num_epochs=10, gpus_per_trial=0):
    data_dir = mkdtemp(prefix="mnist_data_")
    LightningMNISTClassifier.download_data(data_dir)

    config = {
        "layer_1_size": tune.choice([32, 64, 128]),
        "layer_2_size": tune.choice([64, 128, 256]),
        "lr": 1e-3,
        "batch_size": 64,
    }

    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="loss",
        mode="min",
        perturbation_interval=4,
        hyperparam_mutations={
            "lr": lambda: tune.loguniform(1e-4, 1e-1).func(None),
            "batch_size": [32, 64, 128]
        })

    reporter = CLIReporter(
        parameter_columns=["layer_1_size", "layer_2_size", "lr", "batch_size"],
        metric_columns=["loss", "mean_accuracy", "training_iteration"])

    tune.run(
        partial(
            train_mnist_tune_checkpoint,
            data_dir=data_dir,
            num_epochs=num_epochs,
            num_gpus=gpus_per_trial),
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        name="tune_mnist_pbt")

    shutil.rmtree(data_dir)
# __tune_pbt_end__


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_mnist_asha(num_samples=1, num_epochs=1, gpus_per_trial=0)
        tune_mnist_pbt(num_samples=1, num_epochs=1, gpus_per_trial=0)
    else:
        # ASHA scheduler
        tune_mnist_asha(num_samples=10, num_epochs=10, gpus_per_trial=0)
        # Population based training
        tune_mnist_pbt(num_samples=10, num_epochs=10, gpus_per_trial=0)
