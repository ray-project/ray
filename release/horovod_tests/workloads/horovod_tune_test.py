import torch
import torch.nn as nn
import os
import numpy as np
import torchvision
from torch.utils.data import DataLoader

import torchvision.transforms as transforms

import ray
from ray import tune
from ray.tune.schedulers import create_scheduler
from ray.tune.integration.horovod import (
    DistributedTrainableCreator,
    distributed_checkpoint_dir,
)
from ray.util.sgd.torch.resnet import ResNet18

from ray.tune.utils.release_test_util import ProgressCallback

CIFAR10_STATS = {
    "mean": (0.4914, 0.4822, 0.4465),
    "std": (0.2023, 0.1994, 0.2010),
}


def train(config, checkpoint_dir=None):
    import horovod.torch as hvd

    hvd.init()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    net = ResNet18(None).to(device)
    optimizer = torch.optim.SGD(
        net.parameters(),
        lr=config["lr"],
    )
    epoch = 0

    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
            model_state, optimizer_state, epoch = torch.load(f)

        net.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    criterion = nn.CrossEntropyLoss()
    optimizer = hvd.DistributedOptimizer(optimizer)
    np.random.seed(1 + hvd.rank())
    torch.manual_seed(1234)
    # To ensure consistent initialization across workers,
    hvd.broadcast_parameters(net.state_dict(), root_rank=0)
    hvd.broadcast_optimizer_state(optimizer, root_rank=0)

    trainset = ray.get(config["data"])
    trainloader = DataLoader(
        trainset, batch_size=int(config["batch_size"]), shuffle=True, num_workers=4
    )

    for epoch in range(epoch, 40):  # loop over the dataset multiple times
        running_loss = 0.0
        epoch_steps = 0
        for i, data in enumerate(trainloader):
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
                print(
                    "[%d, %5d] loss: %.3f"
                    % (epoch + 1, i + 1, running_loss / epoch_steps)
                )

        with distributed_checkpoint_dir(step=epoch) as checkpoint_dir:
            print("this checkpoint dir: ", checkpoint_dir)
            path = os.path.join(checkpoint_dir, "checkpoint")
            torch.save((net.state_dict(), optimizer.state_dict(), epoch), path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help=("Finish quickly for testing.")
    )
    args = parser.parse_args()

    if args.smoke_test:
        ray.init()
    else:
        ray.init(address="auto")  # assumes ray is started with ray up

    horovod_trainable = DistributedTrainableCreator(
        train,
        use_gpu=False if args.smoke_test else True,
        num_hosts=1 if args.smoke_test else 2,
        num_workers=2 if args.smoke_test else 2,
        replicate_pem=False,
        timeout_s=300,
    )

    transform_train = transforms.Compose(
        [
            transforms.RandomCrop(32, padding=4),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            transforms.Normalize(CIFAR10_STATS["mean"], CIFAR10_STATS["std"]),
        ]
    )  # meanstd transformation

    dataset = torchvision.datasets.CIFAR10(
        root="/tmp/data_cifar", train=True, download=True, transform=transform_train
    )

    # ensure that checkpointing works.
    pbt = create_scheduler(
        "pbt",
        perturbation_interval=2,
        hyperparam_mutations={
            "lr": tune.uniform(0.001, 0.1),
        },
    )

    analysis = tune.run(
        horovod_trainable,
        metric="loss",
        mode="min",
        keep_checkpoints_num=1,
        scheduler=pbt,
        config={
            "lr": 0.1
            if args.smoke_test
            else tune.grid_search([0.1 * i for i in range(1, 10)]),
            "batch_size": 64,
            "data": ray.put(dataset),
        },
        num_samples=1,
        stop={"training_iteration": 1} if args.smoke_test else None,
        callbacks=[ProgressCallback()],  # FailureInjectorCallback()
        fail_fast=True,
    )
    print("Best hyperparameters found were: ", analysis.best_config)
