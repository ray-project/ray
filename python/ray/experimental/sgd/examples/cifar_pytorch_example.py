from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import torch
import torch.nn as nn
import argparse
from ray import tune
import torch.utils.data
from torch import distributed
from torch.utils.data.distributed import DistributedSampler
import torchvision
import torchvision.transforms as transforms

import ray
from ray.experimental.sgd.pytorch import (PyTorchTrainer, PyTorchTrainable)
from ray.experimental.sgd.pytorch.resnet import ResNet18


def initialization_hook(runner):
    print("NCCL DEBUG SET")
    # Need this for avoiding a connection restart issue
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"
    os.environ["NCCL_DEBUG"] = "INFO"


def train(model, train_iterator, criterion, optimizer, config):
    model.train()
    train_loss, total_num, correct = 0, 0, 0
    for batch_idx, (data, target) in enumerate(train_iterator):
        if config.get("test_mode") and batch_idx > 0:
            break
        # get small model update
        if torch.cuda.is_available():
            data, target = data.cuda(), target.cuda()
        output = model(data)
        loss = criterion(output, target)
        loss.backward()
        train_loss += loss.item() * target.size(0)
        total_num += target.size(0)
        _, predicted = output.max(1)
        correct += predicted.eq(target).sum().item()
        optimizer.step()
        optimizer.zero_grad()
    stats = {
        "train_loss": train_loss / total_num,
        "train_acc": correct / total_num
    }
    return stats


def cifar_creator(batch_size, config):
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
        train_dataset = torchvision.datasets.CIFAR10(
            root="~/data",
            train=True,
            download=True,
            transform=transform_train)
    validation_dataset = torchvision.datasets.CIFAR10(
        root="~/data", train=False, download=False, transform=transform_test)

    if distributed.is_initialized():
        train_sampler = DistributedSampler(train_dataset)
    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=(train_sampler is None),
        num_workers=2,
        pin_memory=False,
        sampler=train_sampler)

    if distributed.is_initialized():
        validation_sampler = DistributedSampler(validation_dataset)
    validation_loader = torch.utils.data.DataLoader(
        validation_dataset,
        batch_size=batch_size,
        shuffle=(validation_sampler is None),
        num_workers=2,
        pin_memory=False,
        sampler=validation_sampler)

    return train_loader, validation_loader


def optimizer_creator(model, config):
    """Returns optimizer"""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 1e-4))


def train_example(num_replicas=1, use_gpu=False, test_mode=False):
    config = {"test_mode": test_mode}
    trainer1 = PyTorchTrainer(
        ResNet18,
        cifar_creator,
        optimizer_creator,
        lambda config: nn.CrossEntropyLoss(),
        initialization_hook=initialization_hook,
        train_function=train,
        num_replicas=num_replicas,
        config=config,
        use_gpu=use_gpu,
        batch_size=16 if test_mode else 512,
        backend="nccl")
    stats = trainer1.train()
    print(stats)
    print(trainer1.train())
    trainer1.shutdown()
    print("success!")


def tune_example(num_replicas=1, use_gpu=False, test_mode=False):
    config = {
        "model_creator": ResNet18,
        "data_creator": cifar_creator,
        "optimizer_creator": optimizer_creator,
        "loss_creator": lambda config: nn.CrossEntropyLoss(),
        "num_replicas": num_replicas,
        "initialization_hook": initialization_hook,
        "use_gpu": use_gpu,
        "batch_size": 16 if test_mode else 512,
        "config": {
            "lr": tune.choice([1e-4, 1e-3, 5e-3, 1e-2]),
            "test_mode": test_mode
        },
        "backend": "nccl"
    }

    analysis = tune.run(
        PyTorchTrainable,
        num_samples=2,
        config=config,
        stop={"training_iteration": 2},
        verbose=1)

    return analysis.get_best_config(metric="validation_loss", mode="min")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ray-redis-address",
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
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    ray.init(address=args.ray_redis_address, log_to_driver=False)

    if args.tune:
        tune_example(
            num_replicas=args.num_replicas,
            use_gpu=args.use_gpu,
            test_mode=args.smoke_test)
    else:
        train_example(
            num_replicas=args.num_replicas,
            use_gpu=args.use_gpu,
            test_mode=args.smoke_test)
