# __lightning_begin__
import torch
from pytorch_lightning import Callback
from ray import tune
from ray.tune import CLIReporter
from ray.tune.schedulers import ASHAScheduler
import pytorch_lightning as pl
from torch.utils.data import DataLoader, random_split
from torch.nn import functional as F
from torchvision.datasets import MNIST
from torchvision import transforms
import os


class LightningMNISTClassifier(pl.LightningModule):
    """
    This has been adapted from
    https://towardsdatascience.com/from-pytorch-to-pytorch-lightning-a-gentle-introduction-b371b7caaf09
    """

    def __init__(self, config):
        super(LightningMNISTClassifier, self).__init__()

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

        logs = {"train_loss": loss, "train_accuracy": accuracy}
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
        tensorboard_logs = {"val_loss": avg_loss, "val_accuracy": avg_acc}

        return {
            "avg_val_loss": avg_loss,
            "avg_val_accuracy": avg_acc,
            "log": tensorboard_logs
        }

    def prepare_data(self):
        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307, ), (0.3081, ))
        ])
        mnist_train = MNIST(
            os.getcwd(), train=True, download=True, transform=transform)

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
class LightningCallback(Callback):
    def on_validation_end(self, trainer, pl_module):
        tune.report(
            loss=trainer.callback_metrics["avg_val_loss"],
            mean_accuracy=trainer.callback_metrics["avg_val_accuracy"])


# __tune_callback_end__


# __tune_train_begin__
def train_mnist_tune(config):
    model = LightningMNISTClassifier(config)
    trainer = pl.Trainer(
        max_epochs=10,
        progress_bar_refresh_rate=0,
        callbacks=[LightningCallback()])

    trainer.fit(model)


# __tune_train_end__

# __tune_begin__
if __name__ == "__main__":
    config = {
        "layer_1_size": tune.choice([64, 128]),
        "layer_2_size": tune.choice([64, 128]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([32, 64, 128])
    }
    scheduler = ASHAScheduler(
        metric="loss",
        mode="min",
        max_t=10,
        grace_period=1,
        reduction_factor=2)
    reporter = CLIReporter(
        metric_columns=["loss", "mean_accuracy", "training_iteration"])
    tune.run(
        train_mnist_tune,
        resources_per_trial={"cpu": 1},
        config=config,
        num_samples=10,
        scheduler=scheduler,
        progress_reporter=reporter)
# __tune_end__
