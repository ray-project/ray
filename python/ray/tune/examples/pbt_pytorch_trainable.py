#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# yapf: disable
# __tutorial_imports_begin__
import os
import numpy as np
import torch
import torch.optim as optim
from torchvision import datasets
from ray.tune.examples.mnist_pytorch import train, test, ConvNet, get_data_loaders

import ray
from ray import tune
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune.util import validate_save_restore
# __tutorial_imports_end__
# yapf: enable


# yapf: disable
# __trainable_begin__
class PytorchTrainble(tune.Trainable):
    """Train a Pytorch ConvNet with Trainable and PBT scheduler.

    """
    def _setup(self, config):
        self.device = torch.device("cpu")
        self.train_loader, self.test_loader = get_data_loaders()
        self.model = ConvNet().to(self.device)
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=config.get("lr", 0.01),
            momentum=config.get("momentum", 0.9))

    def _train(self):
        train(self.model, self.optimizer, self.train_loader, device=self.device)
        acc = test(self.model, self.test_loader, self.device)
        return {"mean_accuracy": acc}

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))

    def reset_config(self, new_config):
        del self.optimizer
        self.optimizer = optim.SGD(
            self.model.parameters(),
            lr=new_config.get("lr", 0.01),
            momentum=new_config.get("momentum", 0.9))
        return True
# __trainable_end__
# yapf: enable

ray.init()
datasets.MNIST("~/data", train=True, download=True)

validate_save_restore(PytorchTrainble)
validate_save_restore(PytorchTrainble, use_object_store=True)
print("Success!")

# yapf: disable
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
# yapf: disable
# __pbt_end__

analysis = tune.run(
    PytorchTrainble,
    name="pbt_test",
    scheduler=scheduler,
    reuse_actors=True,
    verbose=1,
    stop={
        "training_iteration": 100,
    },
    num_samples=4,
    # PBT starts by training many neural networks in parallel with random hyperparameters.
    config={
        "lr": tune.uniform(0.001, 1),
        "momentum": tune.uniform(0.001, 1),
    })
