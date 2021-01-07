#!/usr/bin/env python

import argparse
import os
import torch
import torch.nn as nn
import torch.optim as optim
import torch.utils.data
import torchvision.datasets as datasets
import torchvision.transforms as transforms
import numpy as np
from filelock import FileLock

from tqdm import trange

from torch.autograd import Variable
from torch.nn import functional as F
from scipy.stats import entropy

import ray
from ray.util.sgd import TorchTrainer
from ray.util.sgd.utils import override
from ray.util.sgd.torch import TrainingOperator

MODEL_PATH = os.path.expanduser("~/.ray/models/mnist_cnn.pt")


class Generator(nn.Module):
    def __init__(self, latent_vector_size, features=32, num_channels=1):
        super(Generator, self).__init__()
        self.latent_vector_size = latent_vector_size
        self.main = nn.Sequential(
            # input is Z, going into a convolution
            nn.ConvTranspose2d(
                latent_vector_size, features * 4, 4, 1, 0, bias=False),
            nn.BatchNorm2d(features * 4),
            nn.ReLU(True),
            nn.ConvTranspose2d(
                features * 4, features * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features * 2),
            nn.ReLU(True),
            nn.ConvTranspose2d(features * 2, features, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features),
            nn.ReLU(True),
            nn.ConvTranspose2d(features, num_channels, 4, 2, 1, bias=False),
            nn.Tanh())

    def forward(self, x):
        return self.main(x)


class Discriminator(nn.Module):
    def __init__(self, features=32, num_channels=1):
        super(Discriminator, self).__init__()
        self.main = nn.Sequential(
            nn.Conv2d(num_channels, features, 4, 2, 1, bias=False),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features, features * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features * 2), nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features * 2, features * 4, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features * 4), nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features * 4, 1, 4, 1, 0, bias=False), nn.Sigmoid())

    def forward(self, x):
        return self.main(x)


class LeNet(nn.Module):
    """LeNet for MNist classification, used for inception_score."""

    def __init__(self):
        super(LeNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


def weights_init(m):
    classname = m.__class__.__name__
    if classname.find("Conv") != -1:
        nn.init.normal_(m.weight.data, 0.0, 0.02)
    elif classname.find("BatchNorm") != -1:
        nn.init.normal_(m.weight.data, 1.0, 0.02)
        nn.init.constant_(m.bias.data, 0)


class GANOperator(TrainingOperator):
    def setup(self, config):
        discriminator = Discriminator()
        discriminator.apply(weights_init)

        generator = Generator(
            latent_vector_size=config.get("latent_vector_size", 100))
        generator.apply(weights_init)
        models = (discriminator, generator)

        discriminator_opt = optim.Adam(
            discriminator.parameters(),
            lr=config.get("lr", 0.01),
            betas=(0.5, 0.999))
        generator_opt = optim.Adam(
            generator.parameters(),
            lr=config.get("lr", 0.01),
            betas=(0.5, 0.999))
        optimizers = (discriminator_opt, generator_opt)

        with FileLock(".ray.lock"):
            dataset = datasets.MNIST(
                root="~/mnist/",
                download=True,
                transform=transforms.Compose([
                    transforms.Resize(32),
                    transforms.ToTensor(),
                    transforms.Normalize((0.5, ), (0.5, )),
                ]))
        if config.get("test_mode"):
            dataset = torch.utils.data.Subset(dataset, list(range(64)))
        train_dataloader = torch.utils.data.DataLoader(
            dataset, batch_size=config.get("batch_size", 32))

        self.models, self.optimizers, self.criterion = self.register(
            models=models, optimizers=optimizers, criterion=nn.BCELoss())
        self.register_data(
            train_loader=train_dataloader, validation_loader=None)

        self.model = self.models[0]
        self.optimizer = self.optimizers[0]

        self.classifier = LeNet()
        self.classifier.load_state_dict(
            torch.load(config["classification_model_path"]))
        self.classifier.eval()

    def inception_score(self, imgs, batch_size=32, splits=1):
        """Calculate the inception score of the generated images."""
        N = len(imgs)
        dataloader = torch.utils.data.DataLoader(imgs, batch_size=batch_size)
        up = nn.Upsample(
            size=(28, 28),
            mode="bilinear",
            align_corners=False  # This is to reduce user warnings from torch.
        ).type(torch.FloatTensor)

        def get_pred(x):
            x = up(x)
            x = self.classifier(x)
            return F.softmax(x, dim=1).data.cpu().numpy()

        # Obtain predictions for the fake provided images
        preds = np.zeros((N, 10))
        for i, batch in enumerate(dataloader, 0):
            batch = batch.type(torch.FloatTensor)
            batchv = Variable(batch)
            batch_size_i = batch.size()[0]
            preds[i * batch_size:i * batch_size +
                  batch_size_i] = get_pred(batchv)

        # Now compute the mean kl-div
        split_scores = []
        for k in range(splits):
            part = preds[k * (N // splits):(k + 1) * (N // splits), :]
            py = np.mean(part, axis=0)
            scores = []
            for i in range(part.shape[0]):
                pyx = part[i, :]
                scores.append(entropy(pyx, py))
            split_scores.append(np.exp(np.mean(scores)))

        return np.mean(split_scores), np.std(split_scores)

    @override(TrainingOperator)
    def train_batch(self, batch, batch_info):
        """Trains on one batch of data from the data creator."""
        real_label = 1.0
        fake_label = 0.
        discriminator, generator = self.models
        optimD, optimG = self.optimizers

        # Compute a discriminator update for real images
        discriminator.zero_grad()
        # self.device is set automatically
        real_cpu = batch[0].to(self.device)
        batch_size = real_cpu.size(0)
        label = torch.full((batch_size, ), real_label, device=self.device)
        output = discriminator(real_cpu).view(-1)
        errD_real = self.criterion(output, label)
        errD_real.backward()

        # Compute a discriminator update for fake images
        noise = torch.randn(
            batch_size,
            self.config.get("latent_vector_size", 100),
            1,
            1,
            device=self.device)
        fake = generator(noise)
        label.fill_(fake_label)
        output = discriminator(fake.detach()).view(-1)
        errD_fake = self.criterion(output, label)
        errD_fake.backward()
        errD = errD_real + errD_fake

        # Update the discriminator
        optimD.step()

        # Update the generator
        generator.zero_grad()
        label.fill_(real_label)
        output = discriminator(fake).view(-1)
        errG = self.criterion(output, label)
        errG.backward()
        optimG.step()

        is_score, is_std = self.inception_score(fake)

        return {
            "loss_g": errG.item(),
            "loss_d": errD.item(),
            "inception": is_score,
            "num_samples": batch_size
        }


def train_example(num_workers=1, use_gpu=False, test_mode=False):
    config = {
        "test_mode": test_mode,
        "batch_size": 16 if test_mode else 512 // num_workers,
        "classification_model_path": MODEL_PATH
    }
    trainer = TorchTrainer(
        training_operator_cls=GANOperator,
        num_workers=num_workers,
        config=config,
        use_gpu=use_gpu,
        use_tqdm=True)

    from tabulate import tabulate
    pbar = trange(5, unit="epoch")
    for itr in pbar:
        stats = trainer.train(info=dict(epoch_idx=itr, num_epochs=5))
        pbar.set_postfix(dict(loss_g=stats["loss_g"], loss_d=stats["loss_d"]))
        formatted = tabulate([stats], headers="keys")
        if itr > 0:  # Get the last line of the stats.
            formatted = formatted.split("\n")[-1]
        pbar.write(formatted)

    return trainer


if __name__ == "__main__":
    import urllib.request
    # Download a pre-trained MNIST model for inception score calculation.
    # This is a tiny model (<100kb).
    if not os.path.exists(MODEL_PATH):
        print("downloading model")
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        urllib.request.urlretrieve(
            "https://github.com/ray-project/ray/raw/master/python/ray/tune/"
            "examples/pbt_dcgan_mnist/mnist_cnn.pt", MODEL_PATH)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use to connect to a cluster.")
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    args = parser.parse_args()
    if args.smoke_test:
        ray.init(num_cpus=2)
    else:
        ray.init(address=args.address)

    trainer = train_example(
        num_workers=args.num_workers,
        use_gpu=args.use_gpu,
        test_mode=args.smoke_test)
    models = trainer.get_model()
