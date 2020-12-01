import argparse
import torch
import torch.nn as nn
import numpy as np
import torchvision
from torch.utils.data import DataLoader
from torchvision.datasets import CIFAR10

import torchvision.transforms as transforms

import ray
from ray import tune
from ray.tune.utils.mock import FailureInjectorCallback
from ray.tune.integration.horovod import DistributedTrainableCreator
from ray.util.sgd.torch.resnet import ResNet18
import time

parser = argparse.ArgumentParser()
parser.add_argument(
    "--mode", type=str, default="square", choices=["square", "cubic"])
parser.add_argument(
    "--learning_rate", type=float, default=0.1, dest="learning_rate")
parser.add_argument("--x_max", type=float, default=1., dest="x_max")
parser.add_argument("--gpu", action="store_true")
parser.add_argument(
    "--smoke-test", action="store_true", help=("Finish quickly for testing."))
args = parser.parse_args()

CIFAR10_STATS = {
    'mean': (0.4914, 0.4822, 0.4465),
    'std': (0.2023, 0.1994, 0.2010),
}

def train(config):
    import torch
    import horovod.torch as hvd
    hvd.init()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    net = ResNet18(None).to(device)
    optimizer = torch.optim.SGD(
        net.parameters(),
        lr=config["lr"],
    )
    criterion = nn.CrossEntropyLoss()
    optimizer = hvd.DistributedOptimizer(optimizer)
    num_steps = 5
    np.random.seed(1 + hvd.rank())
    torch.manual_seed(1234)
    # To ensure consistent initialization across slots,
    hvd.broadcast_parameters(net.state_dict(), root_rank=0)
    hvd.broadcast_optimizer_state(optimizer, root_rank=0)

    trainset = ray.get(config["data"])
    trainloader = torch.utils.data.DataLoader(
        trainset,
        batch_size=int(config["batch_size"]),
        shuffle=True,
        num_workers=4)

    for epoch in range(40):  # loop over the dataset multiple times
        running_loss = 0.0
        epoch_steps = 0
        for i, data in enumerate(trainloader, 0):
            # get the inputs; data is a list of [inputs, labels]
            inputs, labels = data
            inputs, labels = inputs.to(device), labels.to(device)

            # zero the parameter gradients
            optimizer.zero_grad()

            # forward + backward + optimize
            outputs = net(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            # print statistics
            running_loss += loss.item()
            epoch_steps += 1
            tune.report(loss=running_loss / epoch_steps)
            if i % 2000 == 1999:  # print every 2000 mini-batches
                print("[%d, %5d] loss: %.3f" % (epoch + 1, i + 1,
                                                running_loss / epoch_steps))


if __name__ == "__main__":
    if args.smoke_test:
        ray.init()
    else:
        ray.init(address="auto")  # assumes ray is started with ray up

    horovod_trainable = DistributedTrainableCreator(
        train,
        use_gpu=True,
        num_hosts=1 if args.smoke_test else 2,
        num_slots=2 if args.smoke_test else 2,
        replicate_pem=False)

    transform_train = transforms.Compose([
        transforms.RandomCrop(32, padding=4),
        transforms.RandomHorizontalFlip(),
        transforms.ToTensor(),
        transforms.Normalize(CIFAR10_STATS["mean"], CIFAR10_STATS["std"]),
    ])  # meanstd transformation

    dataset = torchvision.datasets.CIFAR10(
        root="/tmp/data_cifar",
        train=True,
        download=True,
        transform=transform_train)

    analysis = tune.run(
        horovod_trainable,
        metric="loss",
        mode="min",
        config={
            "lr": tune.grid_search([0.1 * i for i in range(1, 10)]),
            "batch_size": 64,
            "data": ray.put(dataset)
        },
        num_samples=1,
        fail_fast=True
        # max_retries=-1
    )
        # callbacks=[FailureInjectorCallback()])
    print("Best hyperparameters found were: ", analysis.best_config)
