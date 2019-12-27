#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.util import validate_save_restore
from ray.tune.trial import ExportFormat

# __tutorial_imports_end__


# __trainable_begin__
class PytorchTrainble(tune.Trainable):
    """Train a Pytorch ConvNet with Trainable and PopulationBasedTraining
       scheduler. The example reuse some of the functions in mnist_pytorch,
       and is a good demo for how to add the tuning function without
       changing the original training code.
    """

    def _setup(self, config):
        self.train_loader, self.test_loader = get_data_loaders()
        self.model = ConvNet()
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=config.get("lr", 0.01),
            momentum=config.get("momentum", 0.9))

    def _train(self):
        train(self.model, self.optimizer, self.train_loader)
        acc = test(self.model, self.test_loader)
        return {"mean_accuracy": acc}

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))

    def _export_model(self, export_formats, export_dir):
        if export_formats == [ExportFormat.MODEL]:
            path = os.path.join(export_dir, "exported_convnet.pt")
            torch.save(self.model.state_dict(), path)
            return {export_formats[0]: path}
        else:
            raise ValueError("unexpected formats: " + str(export_formats))

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

    ray.init()
    datasets.MNIST("~/data", train=True, download=True)

    # check if PytorchTrainble will save/restore correctly before execution
    validate_save_restore(PytorchTrainble)
    validate_save_restore(PytorchTrainble, use_object_store=True)

    # __pbt_begin__
    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="mean_accuracy",
        mode="max",
        perturbation_interval=5,
        hyperparam_mutations={
            # distribution for resampling
            "lr": lambda: np.random.uniform(0.0001, 1),
            # allow perturbations within this set of categorical values
            "momentum": [0.8, 0.9, 0.99],
        })

    # __pbt_end__

    # __tune_begin__
    class Stopper:
        def __init__(self):
            self.should_stop = False

        def stop(self, trial_id, result):
            max_iter = 5 if args.smoke_test else 100
            if not self.should_stop and result["mean_accuracy"] > 0.96:
                self.should_stop = True
            return self.should_stop or result["training_iteration"] >= max_iter

    stopper = Stopper()

    analysis = tune.run(
        PytorchTrainble,
        name="pbt_test",
        scheduler=scheduler,
        reuse_actors=True,
        verbose=1,
        stop=stopper.stop,
        export_formats=[ExportFormat.MODEL],
        num_samples=4,
        config={
            "lr": tune.uniform(0.001, 1),
            "momentum": tune.uniform(0.001, 1),
        })
    # __tune_end__
