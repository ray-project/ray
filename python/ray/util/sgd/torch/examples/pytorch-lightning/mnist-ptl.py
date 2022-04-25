import argparse

# __import_begin__
import os

# Pytorch imports
import torch
from torch.optim import Adam
from torch.utils.data import DataLoader, random_split
from torch.nn import functional as F
from torchvision import transforms
from torchvision.datasets import MNIST

# Ray imports
from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

# PTL imports
from pytorch_lightning.core.lightning import LightningModule

# __import_end__


# __ptl_begin__
class LitMNIST(LightningModule):
    # We take in an additional config parameter here. But this is not required.
    def __init__(self, config):
        super().__init__()

        # mnist images are (1, 28, 28) (channels, width, height)
        self.layer_1 = torch.nn.Linear(28 * 28, 128)
        self.layer_2 = torch.nn.Linear(128, 256)
        self.layer_3 = torch.nn.Linear(256, 10)

        self.config = config

    def forward(self, x):
        batch_size, channels, width, height = x.size()

        # (b, 1, 28, 28) -> (b, 1*28*28)
        x = x.view(batch_size, -1)
        x = self.layer_1(x)
        x = torch.relu(x)
        x = self.layer_2(x)
        x = torch.relu(x)
        x = self.layer_3(x)

        x = torch.log_softmax(x, dim=1)
        return x

    def configure_optimizers(self):
        return Adam(self.parameters(), lr=self.config["lr"])

    def setup(self, stage):
        # transforms for images
        transform = transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        )

        # prepare transforms standard to MNIST
        mnist_train = MNIST(
            os.path.expanduser("~/data"), train=True, download=True, transform=transform
        )

        self.mnist_train, self.mnist_val = random_split(mnist_train, [55000, 5000])

    def train_dataloader(self):
        return DataLoader(self.mnist_train, batch_size=self.config["batch_size"])

    def val_dataloader(self):
        return DataLoader(self.mnist_val, batch_size=self.config["batch_size"])

    def training_step(self, batch, batch_idx):
        x, y = batch
        logits = self(x)
        loss = F.nll_loss(logits, y)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        logits = self(x)
        loss = F.nll_loss(logits, y)
        _, predicted = torch.max(logits.data, 1)
        num_correct = (predicted == y).sum().item()
        num_samples = y.size(0)
        return {"val_loss": loss.item(), "val_acc": num_correct / num_samples}


# __ptl_end__


# __train_begin__
def train_mnist(num_workers=1, use_gpu=False, num_epochs=5):
    Operator = TrainingOperator.from_ptl(LitMNIST)
    trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=num_workers,
        config={"lr": 1e-3, "batch_size": 64},
        use_gpu=use_gpu,
        use_tqdm=True,
    )
    for i in range(num_epochs):
        stats = trainer.train()
        print(stats)

    print(trainer.validate())
    print("Saving model checkpoint to ./model.pt")
    trainer.save("./model.pt")
    print("Model Checkpointed!")
    trainer.shutdown()
    print("success!")


# __train_end__

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--num-epochs", type=int, default=5, help="How many epochs to train for."
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=2)
        args.num_epochs = 1
    elif args.server_address:
        ray.init(f"ray://{args.server_address}")
    else:
        ray.init(address=args.address)
    train_mnist(
        num_workers=args.num_workers, use_gpu=args.use_gpu, num_epochs=args.num_epochs
    )
