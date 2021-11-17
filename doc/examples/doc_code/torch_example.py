# flake8: noqa
"""
This file holds code for the Torch best-practices guide in the documentation.

It ignores yapf because yapf doesn't allow comments right after code blocks,
but we put comments right after code blocks to prevent large white spaces
in the documentation.
"""
# yapf: disable
# __torch_model_start__
import argparse

import torch
import torch.nn as nn
import torch.nn.functional as F


class Model(nn.Module):
    def __init__(self):
        super(Model, self).__init__()
        self.conv1 = nn.Conv2d(1, 20, 5, 1)
        self.conv2 = nn.Conv2d(20, 50, 5, 1)
        self.fc1 = nn.Linear(4 * 4 * 50, 500)
        self.fc2 = nn.Linear(500, 10)

    def forward(self, x):
        x = F.relu(self.conv1(x))
        x = F.max_pool2d(x, 2, 2)
        x = F.relu(self.conv2(x))
        x = F.max_pool2d(x, 2, 2)
        x = x.view(-1, 4 * 4 * 50)
        x = F.relu(self.fc1(x))
        x = self.fc2(x)
        return F.log_softmax(x, dim=1)


# __torch_model_end__
# yapf: enable

# yapf: disable
# __torch_helper_start__
from filelock import FileLock
from torchvision import datasets, transforms


def train(model, device, train_loader, optimizer):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        # This break is for speeding up the tutorial.
        if batch_idx * len(data) > 1024:
            return
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, device, test_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            data, target = data.to(device), target.to(device)
            output = model(data)

            # sum up batch loss
            test_loss += F.nll_loss(
                output, target, reduction="sum").item()
            pred = output.argmax(
                dim=1,
                keepdim=True)
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(test_loader.dataset)
    return {
        "loss": test_loss,
        "accuracy": 100. * correct / len(test_loader.dataset)
    }


def dataset_creator(use_cuda, data_dir):
    kwargs = {"num_workers": 1, "pin_memory": True} if use_cuda else {}
    with FileLock("./data.lock"):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                data_dir,
                train=True,
                download=True,
                transform=transforms.Compose([
                    transforms.ToTensor(),
                    transforms.Normalize((0.1307,), (0.3081,))
                ])),
            batch_size=128,
            shuffle=True,
            **kwargs)
    test_loader = torch.utils.data.DataLoader(
        datasets.MNIST(
            data_dir,
            train=False,
            transform=transforms.Compose([
                transforms.ToTensor(),
                transforms.Normalize((0.1307,), (0.3081,))
            ])),
        batch_size=128,
        shuffle=True,
        **kwargs)

    return train_loader, test_loader


# __torch_helper_end__
# yapf: enable

# yapf: disable
# __torch_net_start__
import torch.optim as optim


class Network(object):
    def __init__(self, lr=0.01, momentum=0.5, data_dir="~/data"):
        use_cuda = torch.cuda.is_available()
        self.device = device = torch.device("cuda" if use_cuda else "cpu")
        self.train_loader, self.test_loader = dataset_creator(use_cuda, data_dir)

        self.model = Model().to(device)
        self.optimizer = optim.SGD(
            self.model.parameters(), lr=lr, momentum=momentum)

    def train(self):
        train(self.model, self.device, self.train_loader, self.optimizer)
        return test(self.model, self.device, self.test_loader)

    def get_weights(self):
        return self.model.state_dict()

    def set_weights(self, weights):
        self.model.load_state_dict(weights)

    def save(self):
        torch.save(self.model.state_dict(), "mnist_cnn.pt")


parser = argparse.ArgumentParser()
parser.add_argument(
    "--data-dir",
    type=str,
    default="~/data/",
    help="Set the path of the dataset."
)
args = parser.parse_args()

net = Network(data_dir=args.data_dir)
net.train()
# __torch_net_end__
# yapf: enable

# yapf: disable
# __torch_ray_start__
import ray

ray.init()

RemoteNetwork = ray.remote(Network)
# Use the below instead of `ray.remote(network)` to leverage the GPU.
# RemoteNetwork = ray.remote(num_gpus=1)(Network)
# __torch_ray_end__
# yapf: enable

# yapf: disable
# __torch_actor_start__
NetworkActor = RemoteNetwork.remote()
NetworkActor2 = RemoteNetwork.remote()

ray.get([NetworkActor.train.remote(), NetworkActor2.train.remote()])
# __torch_actor_end__
# yapf: enable

# yapf: disable
# __weight_average_start__
weights = ray.get(
    [NetworkActor.get_weights.remote(),
     NetworkActor2.get_weights.remote()])

from collections import OrderedDict

averaged_weights = OrderedDict(
    [(k, (weights[0][k] + weights[1][k]) / 2) for k in weights[0]])

weight_id = ray.put(averaged_weights)
[
    actor.set_weights.remote(weight_id)
    for actor in [NetworkActor, NetworkActor2]
]
ray.get([actor.train.remote() for actor in [NetworkActor, NetworkActor2]])
