from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import torch
import torch.nn as nn
import numpy as np

import ray
from ray.experimental.sgd.pytorch.pytorch_runner import PyTorchRunner


def model_creator(config):
    return nn.Sequential(nn.Linear(1, 16), nn.ReLU(), nn.Linear(16, 1),
                         nn.Sigmoid())


def optimizer_creator(model, config):
    """Returns criterion, optimizer"""
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(),
                                lr=0.1,
                                momentum=1e-6,
                                weight_decay=1e-6)
    return criterion, optimizer


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b + N(0, 1)"""

    def __init__(self, a, b, size=1000):
        x = np.random.random(size).astype(np.float32) * 10
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b +
                                  np.random.randn(size).astype(np.float32))

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def data_creator(config):
    """What should this return?"""
    return LinearDataset(2, 5), None


ray.init()

runner = PyTorchRunner(model_creator, data_creator, optimizer_creator)
port = int(4000 + np.random.choice(np.r_[:4000]))
ip = ray.services.get_node_ip_address()
address = "tcp://{ip}:{port}".format(ip=ip, port=port)
runner.setup(address, 0, 1)

for _ in range(10):
    print(runner.step())
