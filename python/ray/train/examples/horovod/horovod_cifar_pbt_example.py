import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
import torchvision
import torchvision.transforms as transforms
from torchvision.models import resnet18

import ray
from ray.air import RunConfig, session
from ray.air.config import ScalingConfig, FailureConfig, CheckpointConfig
from ray.air.checkpoint import Checkpoint
import ray.train.torch
from ray.train.horovod import HorovodTrainer
from ray import tune
from ray.tune.schedulers import create_scheduler
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from ray.tune.utils.release_test_util import ProgressCallback

# The long running version starts 4 trials while only 2 can be run at a time.
# Thus trials are paused and restored at all times so that every trial can make
# progress. The PBT scheduler also applies perturbation and mutation,
# which also involves pausing and restoring.
# The intention is to stress test the pausing and restoring of trials,
# especially that there should be no GPU memory leak.

# TODO(ml-team): This test is very low signal at the moment.
#  We should further trim it down.

CIFAR10_STATS = {
    "mean": (0.4914, 0.4822, 0.4465),
    "std": (0.2023, 0.1994, 0.2010),
}


def train_loop_per_worker(config):
    import horovod.torch as hvd

    hvd.init()
    device = ray.train.torch.get_device()
    net = resnet18().to(device)
    optimizer = torch.optim.SGD(
        net.parameters(),
        lr=config["lr"],
    )
    epoch = 0

    checkpoint = session.get_checkpoint()
    if checkpoint:
        checkpoint_dict = checkpoint.to_dict()
        model_state = checkpoint_dict["model_state"]
        optimizer_state = checkpoint_dict["optimizer_state"]
        epoch = checkpoint_dict["epoch"] + 1

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

    train_sampler = torch.utils.data.distributed.DistributedSampler(
        trainset, num_replicas=hvd.size(), rank=hvd.rank()
    )

    # Note, don't set `num_workers` in DataLoader (not even 1),
    # as that will separately start multiple processes (each corresponding to 1 worker)
    # to load the data. This is known to cause issues with Ray.
    trainloader = DataLoader(
        trainset, batch_size=int(config["batch_size"]), sampler=train_sampler
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

            if i % 2000 == 1999:  # print every 2000 mini-batches
                print(
                    "[%d, %5d] loss: %.3f"
                    % (epoch + 1, i + 1, running_loss / epoch_steps)
                )

            if config["smoke_test"]:
                break

        checkpoint = Checkpoint.from_dict(
            dict(
                model_state=net.state_dict(),
                optimizer_state=optimizer.state_dict(),
                epoch=epoch,
            )
        )
        session.report(dict(loss=running_loss / epoch_steps), checkpoint=checkpoint)


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

    horovod_trainer = HorovodTrainer(
        train_loop_per_worker=train_loop_per_worker,
        scaling_config=ScalingConfig(
            use_gpu=False if args.smoke_test else True,
            num_workers=2,
        ),
        train_loop_config={"batch_size": 64, "data": ray.put(dataset)},
    )

    # ensure that checkpointing works.
    pbt = create_scheduler(
        "pbt",
        perturbation_interval=1,  # To make perturb more often.
        hyperparam_mutations={
            "train_loop_config": {"lr": tune.uniform(0.001, 0.1)},
        },
    )

    tuner = Tuner(
        horovod_trainer,
        param_space={
            "train_loop_config": {
                "lr": 0.1
                if args.smoke_test
                else tune.grid_search([0.1 * i for i in range(1, 5)]),  # 4 trials
                "smoke_test": args.smoke_test,
            }
        },
        tune_config=TuneConfig(
            num_samples=2 if args.smoke_test else 1,
            metric="loss",
            mode="min",
            scheduler=pbt,
        ),
        run_config=RunConfig(
            stop={"training_iteration": 1} if args.smoke_test else None,
            failure_config=FailureConfig(fail_fast=False),
            checkpoint_config=CheckpointConfig(num_to_keep=1),
            callbacks=[ProgressCallback()],
        ),
    )

    result_grid = tuner.fit()

    # Make sure trials do not fail.
    for result in result_grid:
        assert not result.error

    print("Best hyperparameters found were: ", result_grid.get_best_result().config)
