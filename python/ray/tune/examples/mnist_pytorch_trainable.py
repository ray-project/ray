# Original Code here:
# https://github.com/pytorch/examples/blob/master/mnist/main.py
from __future__ import print_function

import argparse
import os
import torch
import torch.optim as optim

import ray
from ray import train, tune
from ray.tune.schedulers import ASHAScheduler
from ray.tune.examples.mnist_pytorch import (
    train_func,
    test_func,
    get_data_loaders,
    ConvNet,
)

# Change these values if you want the training to run quicker or slower.
EPOCH_SIZE = 512
TEST_SIZE = 256

# Training settings
parser = argparse.ArgumentParser(description="PyTorch MNIST Example")
parser.add_argument(
    "--use-gpu", action="store_true", default=False, help="enables CUDA training"
)
parser.add_argument("--ray-address", type=str, help="The Redis address of the cluster.")
parser.add_argument(
    "--smoke-test", action="store_true", help="Finish quickly for testing"
)


# Below comments are for documentation purposes only.
# fmt: off
# __trainable_example_begin__
class TrainMNIST(tune.Trainable):
    def setup(self, config):
        use_cuda = config.get("use_gpu") and torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        self.train_loader, self.test_loader = get_data_loaders()
        self.model = ConvNet().to(self.device)
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=config.get("lr", 0.01),
            momentum=config.get("momentum", 0.9))

    def step(self):
        train_func(
            self.model, self.optimizer, self.train_loader, device=self.device)
        acc = test_func(self.model, self.test_loader, self.device)
        return {"mean_accuracy": acc}

    def save_checkpoint(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)

    def load_checkpoint(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        self.model.load_state_dict(torch.load(checkpoint_path))


# __trainable_example_end__
# fmt: on

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(address=args.ray_address, num_cpus=6 if args.smoke_test else None)
    sched = ASHAScheduler()

    tuner = tune.Tuner(
        tune.with_resources(TrainMNIST, resources={"cpu": 3, "gpu": int(args.use_gpu)}),
        run_config=train.RunConfig(
            stop={
                "mean_accuracy": 0.95,
                "training_iteration": 3 if args.smoke_test else 20,
            },
            checkpoint_config=train.CheckpointConfig(
                checkpoint_at_end=True, checkpoint_frequency=3
            ),
        ),
        tune_config=tune.TuneConfig(
            metric="mean_accuracy",
            mode="max",
            scheduler=sched,
            num_samples=1 if args.smoke_test else 20,
        ),
        param_space={
            "args": args,
            "lr": tune.uniform(0.001, 0.1),
            "momentum": tune.uniform(0.1, 0.9),
        },
    )
    results = tuner.fit()

    print("Best config is:", results.get_best_result().config)
