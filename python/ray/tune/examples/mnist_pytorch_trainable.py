# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
from __future__ import print_function

import argparse
import os
import torch
import torch.optim as optim
from torchvision import datasets

from ray.tune import Trainable
from ray.tune.examples.mnist_pytorch import (
    train, test, get_data_loaders, ConvNet)

# Change these values if you want the training to run quicker or slower.
EPOCH_SIZE = 512
TEST_SIZE = 256

# Training settings
parser = argparse.ArgumentParser(description="PyTorch MNIST Example")
parser.add_argument(
    "--use-gpu",
    action="store_true",
    default=False,
    help="enables CUDA training")
parser.add_argument(
    "--redis-address",
    default=None,
    type=str,
    help="The Redis address of the cluster.")
parser.add_argument(
    "--seed",
    type=int,
    default=1,
    metavar="S",
    help="random seed (default: 1)")
parser.add_argument(
    "--smoke-test", action="store_true", help="Finish quickly for testing")


class TrainMNIST(Trainable):
    def _setup(self, config):
        torch.manual_seed(config.get("seed"))
        if config.get("use_gpu"):
            torch.cuda.manual_seed(config.get("seed"))

        use_cuda = config.get("use_gpu") and torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        self.train_loader, self.test_loader = get_data_loaders()
        self.model = ConvNet().to(self.device)
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=config.get("lr", 0.01),
            momentum=config.get("momentum", 0.9))

    def _train(self):
        train(
            self.model, self.optimizer, self.train_loader, device=self.device)
        acc = test(self.model, self.test_loader, self.device)
        return {"mean_accuracy": acc}

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))


if __name__ == "__main__":
    datasets.MNIST("~/data", train=True, download=True)
    args = parser.parse_args()

    import ray
    from ray import tune
    from ray.tune.schedulers import HyperBandScheduler

    ray.init(redis_address=args.redis_address)
    sched = HyperBandScheduler(
        time_attr="training_iteration", metric="mean_loss", mode="min")
    tune.run(
        TrainMNIST,
        scheduler=sched,
        **{
            "stop": {
                "mean_accuracy": 0.95,
                "training_iteration": 1 if args.smoke_test else 20,
            },
            "resources_per_trial": {
                "cpu": 3,
                "gpu": int(not args.no_cuda)
            },
            "num_samples": 1 if args.smoke_test else 20,
            "checkpoint_at_end": True,
            "config": {
                "args": args,
                "lr": tune.uniform(0.001, 0.1),
                "momentum": tune.uniform(0.1, 0.9),
            }
        })
