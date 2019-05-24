from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import torch
import torch.nn as nn
import torch.utils.data


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


def data_creator(config):
    """Returns training set, validation set"""
    return LinearDataset(2, 5), LinearDataset(2, 5, size=400)
