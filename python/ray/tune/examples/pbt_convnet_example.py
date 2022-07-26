#!/usr/bin/env python

# flake8: noqa
# fmt: off

# __tutorial_imports_begin__
import argparse
import os
import numpy as np
import torch
import torch.optim as optim
from torchvision import datasets
from ray.tune.examples.mnist_pytorch import train, test, ConvNet,\
    get_data_loaders

import ray
from ray import air, tune
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.utils import validate_save_restore
# __tutorial_imports_end__


# __trainable_begin__
class PytorchTrainable(tune.Trainable):
    """Train a Pytorch ConvNet with Trainable and PopulationBasedTraining
       scheduler. The example reuse some of the functions in mnist_pytorch,
       and is a good demo for how to add the tuning function without
       changing the original training code.
    """

    def setup(self, config):
        self.train_loader, self.test_loader = get_data_loaders()
        self.model = ConvNet()
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=config.get("lr", 0.01),
            momentum=config.get("momentum", 0.9))

    def step(self):
        train(self.model, self.optimizer, self.train_loader)
        acc = test(self.model, self.test_loader)
        return {"mean_accuracy": acc}

    def save_checkpoint(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def load_checkpoint(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))

    def reset_config(self, new_config):
        for param_group in self.optimizer.param_groups:
            if "lr" in new_config:
                param_group["lr"] = new_config["lr"]
            if "momentum" in new_config:
                param_group["momentum"] = new_config["momentum"]

        self.config = new_config
        return True
# __trainable_end__


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    ray.init(num_cpus=2)
    datasets.MNIST("~/data", train=True, download=True)

    # check if PytorchTrainble will save/restore correctly before execution
    validate_save_restore(PytorchTrainable)
    validate_save_restore(PytorchTrainable, use_object_store=True)

    # __pbt_begin__
    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        perturbation_interval=5,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: np.random.uniform(0.0001, 1),
            # allow perturbations within this set of categorical values
            "momentum": [0.8, 0.9, 0.99],
        })
    # __pbt_end__

    # __tune_begin__
    class CustomStopper(tune.Stopper):
        def __init__(self):
            self.should_stop = False

        def __call__(self, trial_id, result):
            max_iter = 5 if args.smoke_test else 100
            if not self.should_stop and result["mean_accuracy"] > 0.96:
                self.should_stop = True
            return self.should_stop or result["training_iteration"] >= max_iter

        def stop_all(self):
            return self.should_stop

    stopper = CustomStopper()

    tuner = tune.Tuner(
        PytorchTrainable,
        run_config=air.RunConfig(
            name="pbt_test",
            stop=stopper,
            verbose=1,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_score_attribute="mean_accuracy",
                checkpoint_frequency=5,
                num_to_keep=4,
            ),
        ),
        tune_config=tune.TuneConfig(
            scheduler=scheduler,
            metric="mean_accuracy",
            mode="max",
            num_samples=4,
            reuse_actors=True,
        ),
        param_space={
            "lr": tune.uniform(0.001, 1),
            "momentum": tune.uniform(0.001, 1),
        },
    )
    results = tuner.fit()
    # __tune_end__

    best_result = results.get_best_result()
    best_checkpoint = best_result.checkpoint

    restored_trainable = PytorchTrainable()
    restored_trainable.restore(best_checkpoint)
    best_model = restored_trainable.model
    # Note that test only runs on a small random set of the test data, thus the
    # accuracy may be different from metrics shown in tuning process.
    test_acc = test(best_model, get_data_loaders()[1])
    print("best model accuracy: ", test_acc)
