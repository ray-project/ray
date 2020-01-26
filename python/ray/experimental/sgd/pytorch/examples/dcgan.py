#!/usr/bin/env python

import argparse
import os
import torch
import torch.nn as nn
import torch.optim as optim
import torch.utils.data
import torchvision.datasets as dset
import torchvision.transforms as transforms
import numpy as np

from torch.autograd import Variable
from torch.nn import functional as F
from scipy.stats import entropy

import ray
from ray.experimental.sgd import PyTorchTrainer

# Training parameters
TRAIN_BATCHES = 5
# Number of channels in the training images. For color images this is 3
num_channels = 1

# Size of z latent vector (i.e. size of generator input)
latent_vector_size = 100

# Size of feature maps in generator
features_g = 32

# Size of feature maps in discriminator
features_d = 32


def data_creator(config):
    return dset.MNIST(
        root="~/mnist/",
        download=True,
        transform=transforms.Compose([
            transforms.Resize(32),
            transforms.ToTensor(),
            transforms.Normalize((0.5, ), (0.5, )),
        ]))


def weights_init(m):
    classname = m.__class__.__name__
    if classname.find("Conv") != -1:
        nn.init.normal_(m.weight.data, 0.0, 0.02)
    elif classname.find("BatchNorm") != -1:
        nn.init.normal_(m.weight.data, 1.0, 0.02)
        nn.init.constant_(m.bias.data, 0)


class Generator(nn.Module):
    def __init__(self):
        super(Generator, self).__init__()
        self.main = nn.Sequential(
            # input is Z, going into a convolution
            nn.ConvTranspose2d(
                latent_vector_size, features_g * 4, 4, 1, 0, bias=False),
            nn.BatchNorm2d(features_g * 4),
            nn.ReLU(True),
            nn.ConvTranspose2d(
                features_g * 4, features_g * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_g * 2),
            nn.ReLU(True),
            nn.ConvTranspose2d(
                features_g * 2, features_g, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_g),
            nn.ReLU(True),
            nn.ConvTranspose2d(features_g, num_channels, 4, 2, 1, bias=False),
            nn.Tanh())

    def forward(self, input):
        return self.main(input)


class Discriminator(nn.Module):
    def __init__(self):
        super(Discriminator, self).__init__()
        self.main = nn.Sequential(
            nn.Conv2d(num_channels, features_d, 4, 2, 1, bias=False),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features_d, features_d * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_d * 2), nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features_d * 2, features_d * 4, 4, 2, 1, bias=False),
            nn.BatchNorm2d(features_d * 4), nn.LeakyReLU(0.2, inplace=True),
            nn.Conv2d(features_d * 4, 1, 4, 1, 0, bias=False), nn.Sigmoid())

    def forward(self, input):
        return self.main(input)


class Net(nn.Module):
    """LeNet for MNist classification, used for inception_score."""

    def __init__(self):
        super(Net, self).__init__()
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


def inception_score(imgs, batch_size=32, splits=1):
    N = len(imgs)
    dtype = torch.FloatTensor
    dataloader = torch.utils.data.DataLoader(imgs, batch_size=batch_size)
    cm = Net()
    cm.load_state_dict(torch.load(model_path))
    cm.eval()
    up = nn.Upsample(size=(28, 28), mode="bilinear").type(dtype)

    def get_pred(x):
        x = up(x)
        x = cm(x)
        return F.softmax(x).data.cpu().numpy()

    preds = np.zeros((N, 10))
    for i, batch in enumerate(dataloader, 0):
        batch = batch.type(dtype)
        batchv = Variable(batch)
        batch_size_i = batch.size()[0]
        preds[i * batch_size:i * batch_size + batch_size_i] = get_pred(batchv)

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


def model_creator(config):
    netD = Discriminator()
    netD.apply(weights_init)

    netG = Generator()
    netG.apply(weights_init)
    return netD, netG


def train(models, dataloader, criterion, optimizers, config):
    netD, netG = models
    optimD, optimG = optimizers
    real_label = 1
    fake_label = 0
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    for i, data in enumerate(dataloader, 0):
        if i >= TRAIN_BATCHES and config.get("test_mode"):
            break

        netD.zero_grad()
        real_cpu = data[0].to(device)
        b_size = real_cpu.size(0)
        label = torch.full((b_size, ), real_label, device=device)
        output = netD(real_cpu).view(-1)
        errD_real = criterion(output, label)
        errD_real.backward()

        noise = torch.randn(b_size, latent_vector_size, 1, 1, device=device)
        fake = netG(noise)
        label.fill_(fake_label)
        output = netD(fake.detach()).view(-1)
        errD_fake = criterion(output, label)
        errD_fake.backward()
        errD = errD_real + errD_fake
        optimD.step()

        netG.zero_grad()
        label.fill_(real_label)
        output = netD(fake).view(-1)
        errG = criterion(output, label)
        errG.backward()
        optimG.step()

        is_score, is_std = inception_score(fake)

    return {
        "loss_g": errG.item(),
        "loss_d": errD.item(),
        "inception": is_score
    }


def optimizer_creator(models, config):
    net_d, net_g = models
    discriminator_opt = optim.Adam(
        net_d.parameters(), lr=config.get("lr", 0.01), betas=(0.5, 0.999))
    generator_opt = optim.Adam(
        net_g.parameters(), lr=config.get("lr", 0.01), betas=(0.5, 0.999))
    return discriminator_opt, generator_opt


def train_example(num_replicas=1, use_gpu=False, test_mode=False):
    config = {"test_mode": test_mode}
    trainer = PyTorchTrainer(
        model_creator,
        data_creator,
        optimizer_creator,
        nn.BCELoss,
        train_function=train,
        validation_function=False,
        num_replicas=num_replicas,
        config=config,
        use_gpu=use_gpu,
        batch_size=16 if test_mode else 512,
        backend="nccl" if use_gpu else "gloo")
    for i in range(10):
        stats = trainer.train(max_retries=3)
        print(stats)

    return trainer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    args, _ = parser.parse_known_args()
    ray.init(address=args.address)

    path = os.path.dirname(ray.__file__)
    model_path = os.path.join(
        path, "experimental/sgd/pytorch/examples/mnist_cnn.pt")
    # load the pretrained mnist classification model for inception_score

    trainer = train_example(
        num_replicas=args.num_replicas,
        use_gpu=args.use_gpu,
        test_mode=args.smoke_test)
    models = trainer.get_model()
