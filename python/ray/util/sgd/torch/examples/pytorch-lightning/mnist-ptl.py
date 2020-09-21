import torch
from pytorch_lightning import Trainer
from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd.utils import BATCH_SIZE
from torch.nn import functional as F
from torch import nn
from pytorch_lightning.core.lightning import LightningModule
from torch.optim import Adam


class LitMNIST(LightningModule):

    def __init__(self):
        super().__init__()

        # mnist images are (1, 28, 28) (channels, width, height)
        self.layer_1 = torch.nn.Linear(28 * 28, 128)
        self.layer_2 = torch.nn.Linear(128, 256)
        self.layer_3 = torch.nn.Linear(256, 10)

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

    def training_step(self, batch, batch_idx):
        x, y = batch
        logits = self(x)
        loss = F.nll_loss(logits, y)
        return loss

    def configure_optimizers(self):
        return Adam(self.parameters(), lr=1e-3)

    def prepare_data(self):
        # transforms for images
        transform = transforms.Compose([transforms.ToTensor(),
                                        transforms.Normalize((0.1307,),
                                                             (0.3081,))])

        # prepare transforms standard to MNIST
        mnist_train = MNIST(os.getcwd(), train=True, download=True,
                            transform=transform)
        mnist_test = MNIST(os.getcwd(), train=False, download=True,
                           transform=transform)

        self.mnist_train, self.mnist_val = random_split(mnist_train,
                                                        [55000, 5000])

    def train_dataloader(self):
        return DataLoader(self.mnist_train, batch_size=64)

    def val_dataloader(self):
        return DataLoader(self.mnist_val, batch_size=64)

    def training_step(self, batch, batch_idx):
        x, y = batch
        logits = self(x)
        loss = F.nll_loss(logits, y)
        return loss

from torch.utils.data import DataLoader, random_split
from torchvision.datasets import MNIST
import os
from torchvision import datasets, transforms

# transforms
# prepare transforms standard to MNIST
# transform = transforms.Compose([transforms.ToTensor(),
#                                 transforms.Normalize((0.1307,),
#                                                      (0.3081,))])

# data
# mnist_train = MNIST(os.getcwd(), train=True, download=False,
#                     transform=transform)
# mnist_train = DataLoader(mnist_train, batch_size=64)

# model = LitMNIST()
# trainer = Trainer(max_epochs=1)
# trainer.fit(model, mnist_train)

Operator = TrainingOperator.from_ptl(LitMNIST)
trainer = TorchTrainer(
        training_operator_cls=Operator,
        num_workers=1,
        config={
            # this will be split across workers.
            BATCH_SIZE: 128 * 2
        },
        use_gpu=False,
        use_fp16=False,
        use_local=True,
)
train_stats = trainer.train()
print(train_stats)
