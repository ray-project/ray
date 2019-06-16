# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import argparse
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

import ray
from ray import tune
from ray.tune import track
from ray.tune.schedulers import AsyncHyperBandScheduler

EPOCH_SIZE = 2048
TEST_SIZE = 1024
datasets.MNIST("~/data", train=True, download=True)
parser = argparse.ArgumentParser(description="PyTorch MNIST Example")
parser.add_argument(
    "--cuda", action="store_true", default=False, help="Enables GPU training")
parser.add_argument(
    "--seed",
    type=int,
    default=1,
    metavar="S",
    help="random seed (default: 1)")
parser.add_argument(
    "--smoke-test", action="store_true", help="Finish quickly for testing")
parser.add_argument(
    "--ray-redis-address",
    help="Address of Ray cluster for seamless distributed execution.")


class Net(nn.Module):
    def __init__(self, config):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=3)
        self.fc = nn.Linear(640, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 640)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)


def train(model, optimizer, train_loader, use_cuda):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if batch_idx * len(data) > EPOCH_SIZE:
            return
        if use_cuda:
            data, target = data.cuda(), target.cuda()
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, data_loader, use_cuda):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(data_loader):
            if batch_idx * len(data) > TEST_SIZE:
                break
            if use_cuda:
                data, target = data.cuda(), target.cuda()
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct / total


def train_mnist(config):
    use_cuda = config.get("use_gpu") and torch.cuda.is_available()

    mnist_transforms = transforms.Compose([
        transforms.ToTensor(),
        # transforms.Normalize((0.1307, ), (0.3081, ))
    ])

    train_loader = torch.utils.data.DataLoader(
        datasets.MNIST(
            "~/data", train=True, download=False, transform=mnist_transforms),
        batch_size=64,
        shuffle=True)
    test_loader = torch.utils.data.DataLoader(
        datasets.MNIST("~/data", train=False, transform=mnist_transforms),
        batch_size=64,
        shuffle=True)

    model = Net(config)
    if use_cuda:
        model.cuda()

    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])

    while True:
        train(model, optimizer, train_loader, use_cuda)
        acc = test(model, test_loader, use_cuda)
        track.log(mean_accuracy=acc)


if __name__ == "__main__":
    args = parser.parse_args()
    if args.ray_redis_address:
        ray.init(redis_address=args.ray_redis_address)
    sched = AsyncHyperBandScheduler(
        time_attr="training_iteration", metric="mean_accuracy")
    tune.run(
        train_mnist,
        name="exp",
        scheduler=sched,
        stop={
            "mean_accuracy": 0.98,
            "training_iteration": 5 if args.smoke_test else 20
        },
        resources_per_trial={
            "cpu": 2,
            "gpu": int(args.cuda)
        },
        num_samples=1 if args.smoke_test else 10,
        config={
            "lr": tune.sample_from(lambda spec: 10**(-10 * np.random.rand())),
            "momentum": tune.uniform(0.1, 0.9),
            "use_gpu": int(args.cuda)
        })
