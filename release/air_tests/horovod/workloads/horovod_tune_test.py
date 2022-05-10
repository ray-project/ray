import torch
import torch.nn as nn
import numpy as np
import torchvision
from ray.ml import RunConfig
from ray.ml.train.integrations.horovod import HorovodTrainer
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner
from torch.utils.data import DataLoader

import torchvision.transforms as transforms

import ray
from ray import tune
from ray import train
from ray.tune.schedulers import create_scheduler

from ray.util.ml_utils.resnet import ResNet18

from ray.tune.utils.release_test_util import ProgressCallback

CIFAR10_STATS = {
    "mean": (0.4914, 0.4822, 0.4465),
    "std": (0.2023, 0.1994, 0.2010),
}


def train_loop_per_worker(config):
    import horovod.torch as hvd

    hvd.init()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    net = ResNet18(None).to(device)
    optimizer = torch.optim.SGD(
        net.parameters(),
        lr=config["lr"],
    )
    epoch = 0

    checkpoint = train.load_checkpoint()
    if checkpoint:
        model_state = checkpoint["model_state"]
        optimizer_state = checkpoint["optimizer_state"]
        epoch = checkpoint["epoch"]

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
            train.report(loss=running_loss / epoch_steps)
            if i % 2000 == 1999:  # print every 2000 mini-batches
                print(
                    "[%d, %5d] loss: %.3f"
                    % (epoch + 1, i + 1, running_loss / epoch_steps)
                )

        train.save_checkpoint(
            model_state=net.state_dict(),
            optimizer_state=optimizer.state_dict(),
            epoch=epoch,
        )


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
        scaling_config={
            "use_gpu": False if args.smoke_test else True,
            "num_workers": 2 if args.smoke_test else 4,
        },
        train_loop_config={"batch_size": 64, "data": ray.put(dataset)},
    )

    # ensure that checkpointing works.
    pbt = create_scheduler(
        "pbt",
        perturbation_interval=2,
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
                else tune.grid_search([0.1 * i for i in range(1, 10)])
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
            callbacks=[ProgressCallback()],
        ),
        _tuner_kwargs={"fail_fast": False, "keep_checkpoints_num": 1},
    )

    result_grid = tuner.fit()

    # Make sure trials do not fail.
    for result in result_grid:
        assert not result.error

    print("Best hyperparameters found were: ", result_grid.get_best_result().config)
