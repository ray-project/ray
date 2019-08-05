from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch
import torch.nn as nn
import torch.utils.data
import torchvision.transforms as transforms
import torchvision.datasets as datasets
import torchvision.models as models


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
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
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)
    return criterion, optimizer


def xe_optimizer_creator(model, config):
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)
    return criterion, optimizer


def data_creator(config):
    """Returns training set, validation set"""
    return LinearDataset(2, 5), LinearDataset(2, 5, size=400)


def resnet_creator(config):
    model_cls = models.__dict__["resnet50"]
    return model_cls(pretrained=False)


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
    with FileLock("~/data.lock"):
        trainset = torchvision.datasets.CIFAR10(
            root='~/data',
            train=True,
            download=True,
            transform=transform_train)
    valset = torchvision.datasets.CIFAR10(
        root='~/data',
        train=False,
        download=False,
        transform=transform_test)
    return trainset, valset
