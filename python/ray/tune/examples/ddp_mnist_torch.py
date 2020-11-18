# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
import argparse
import logging
import os
import torch
import torch.optim as optim
from torch.nn.parallel import DistributedDataParallel

import ray
from ray import tune
from ray.tune.examples.mnist_pytorch import (train, test, get_data_loaders,
                                             ConvNet)
from ray.tune.integration.torch import (DistributedTrainableCreator,
                                        distributed_checkpoint_dir)

logger = logging.getLogger(__name__)


def train_mnist(config, checkpoint_dir=False):
    use_cuda = torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    train_loader, test_loader = get_data_loaders()
    model = ConvNet().to(device)
    optimizer = optim.SGD(model.parameters(), lr=0.1)

    if checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "checkpoint")) as f:
            model_state, optimizer_state = torch.load(f)

        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    model = DistributedDataParallel(model)

    for epoch in range(40):
        train(model, optimizer, train_loader, device)
        acc = test(model, test_loader, device)

        if epoch % 3 == 0:
            with distributed_checkpoint_dir(step=epoch) as checkpoint_dir:
                path = os.path.join(checkpoint_dir, "checkpoint")
                torch.save((model.state_dict(), optimizer.state_dict()), path)
        tune.report(mean_accuracy=acc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--num-gpus-per-worker",
        action="store_true",
        default=False,
        help="enables CUDA training")
    parser.add_argument(
        "--cluster",
        action="store_true",
        default=False,
        help="enables multi-node tuning")
    parser.add_argument(
        "--workers-per-node",
        type=int,
        help="Forces workers to be colocated on machines if set.")

    args = parser.parse_args()

    if args.cluster:
        options = dict(address="auto")
    else:
        options = dict(num_cpus=2)
    ray.init(**options)
    trainable_cls = DistributedTrainableCreator(
        train_mnist,
        num_workers=args.num_workers,
        num_gpus_per_worker=args.num_gpus_per_worker,
        num_workers_per_host=args.workers_per_node)

    analysis = tune.run(
        trainable_cls,
        num_samples=4,
        stop={"training_iteration": 10},
        metric="mean_accuracy",
        mode="max")

    print("Best hyperparameters found were: ", analysis.best_config)
