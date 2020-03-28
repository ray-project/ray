import os
import torch
import torch.nn as nn
import argparse
from ray import tune
from torch.utils.data import DataLoader, Subset
import torchvision
import torchvision.transforms as transforms

from tqdm import trange

import ray
from ray.util.sgd.torch import (TorchTrainer, TorchTrainable)
from ray.util.sgd.torch.resnet import ResNet18
from ray.util.sgd.utils import BATCH_SIZE


def initialization_hook():
    print("NCCL DEBUG SET")
    # Need this for avoiding a connection restart issue
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"
    os.environ["NCCL_DEBUG"] = "INFO"


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
    train_dataset = torchvision.datasets.CIFAR10(
        root="~/data", train=True, download=True, transform=transform_train)
    validation_dataset = torchvision.datasets.CIFAR10(
        root="~/data", train=False, download=False, transform=transform_test)

    if config.get("test_mode"):
        train_dataset = Subset(train_dataset, list(range(64)))
        validation_dataset = Subset(validation_dataset, list(range(64)))

    train_loader = DataLoader(
        train_dataset, batch_size=config[BATCH_SIZE], num_workers=2)
    validation_loader = DataLoader(
        validation_dataset, batch_size=config[BATCH_SIZE], num_workers=2)
    return train_loader, validation_loader


def optimizer_creator(model, config):
    """Returns optimizer"""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 0.1))


def scheduler_creator(optimizer, config):
    return torch.optim.lr_scheduler.MultiStepLR(
        optimizer, milestones=[150, 250, 350], gamma=0.1)


def train_example(num_workers=1,
                  num_epochs=5,
                  use_gpu=False,
                  use_fp16=False,
                  test_mode=False):
    trainer1 = TorchTrainer(
        model_creator=ResNet18,
        data_creator=cifar_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=nn.CrossEntropyLoss,
        scheduler_creator=scheduler_creator,
        initialization_hook=initialization_hook,
        num_workers=num_workers,
        config={
            "lr": 0.01,
            "test_mode": test_mode,
            BATCH_SIZE: 128,
        },
        use_gpu=use_gpu,
        backend="nccl" if use_gpu else "gloo",
        scheduler_step_freq="epoch",
        use_fp16=use_fp16,
        use_tqdm=True)
    pbar = trange(num_epochs, unit="epoch")
    for i in pbar:
        info = {"num_steps": 1} if test_mode else {}
        info["epoch_idx"] = i
        info["num_epochs"] = num_epochs
        # Increase `max_retries` to turn on fault tolerance.
        stats = trainer1.train(max_retries=1, info=info)
        pbar.set_postfix(dict(loss=stats["mean_train_loss"]))

    print(trainer1.validate())
    trainer1.shutdown()
    print("success!")


def tune_example(num_workers=1, use_gpu=False, test_mode=False):
    config = {
        "model_creator": ResNet18,
        "data_creator": cifar_creator,
        "optimizer_creator": optimizer_creator,
        "loss_creator": nn.CrossEntropyLoss,
        "num_workers": num_workers,
        "initialization_hook": initialization_hook,
        "use_gpu": use_gpu,
        "config": {
            "lr": tune.choice([1e-4, 1e-3]),
            BATCH_SIZE: 128,
            "test_mode": test_mode
        },
        "backend": "nccl" if use_gpu else "gloo"
    }

    analysis = tune.run(
        TorchTrainable,
        num_samples=2,
        config=config,
        stop={"training_iteration": 2},
        verbose=2)

    return analysis.get_best_config(metric="mean_accuracy", mode="max")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--num-epochs", type=int, default=5, help="Number of epochs to train.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--fp16",
        action="store_true",
        default=False,
        help="Enables FP16 training with apex. Requires `use-gpu`.")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")

    args, _ = parser.parse_known_args()

    ray.init(address=args.address, log_to_driver=True)

    if args.tune:
        tune_example(
            num_workers=args.num_workers,
            use_gpu=args.use_gpu,
            test_mode=args.smoke_test)
    else:
        train_example(
            num_workers=args.num_workers,
            num_epochs=args.num_epochs,
            use_gpu=args.use_gpu,
            use_fp16=args.fp16,
            test_mode=args.smoke_test)
