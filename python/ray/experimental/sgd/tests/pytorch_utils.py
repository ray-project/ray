from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import torch
import torch.nn as nn
import torch.utils.data
import torchvision
import torchvision.transforms as transforms
import torchvision.models as models


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1024):
        x = np.random.random(size).astype(np.float32) * 10
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def model_creator(config):
    return nn.Linear(1, 1)


def optimizer_creator(model, config):
    """Returns criterion, optimizer"""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def mse_loss(*args):
    return nn.MSELoss()


def toy_data_creator(config):
    """Returns training set, validation set"""
    return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


def cifar_creator(config):
    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])  # meanstd transformation

    transform_test = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.4914, 0.4822, 0.4465),
                             (0.2023, 0.1994, 0.2010)),
    ])
    from filelock import FileLock
    with FileLock(os.path.expanduser("~/data.lock")):
        trainset = torchvision.datasets.CIFAR10(
            root=config["data_dir"],
            train=True,
            download=True,
            transform=transform_train)
    valset = torchvision.datasets.CIFAR10(
        root=config["data_dir"], train=False, download=False, transform=transform_test)
    return trainset, valset


def imagenet_creator(config):
    trainset = torchvision.datasets.ImageNet(
        root=config["data_dir"],
        transform=transforms.Compose([
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225))
        ]),
        split="train")
    trainset = torch.utils.data.Subset(trainset, range(500 * 256))
    valset = torchvision.datasets.ImageNet(
        root=config["data_dir"],
        transform=transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=(0.485, 0.456, 0.406), std=(0.229, 0.224, 0.225))
        ]),
        split="val")
    return trainset, valset
