"""
Simple Parallel Model Selection
===============================

In this example, we'll demonstrate how to quickly write a hyperparameter
tuning script that evaluates a set of hyperparameters in parallel.

This script will demonstrate how to use two important parts of the Ray API:
using ``ray.remote`` to define remote functions and ``ray.wait`` to wait for
their results to be ready.

.. image:: /ray-core/images/hyperparameter.png
    :align: center

.. tip:: For a production-grade implementation of distributed
    hyperparameter tuning, use `Tune`_, a scalable hyperparameter
    tuning library built using Ray's Actor API.

.. _`Tune`: https://docs.ray.io/en/master/tune.html

Setup: Dependencies
-------------------
First, import some dependencies and define functions to generate
random hyperparameters and retrieve data.
"""
import os
import numpy as np
from filelock import FileLock

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

import ray

ray.init()

# The number of sets of random hyperparameters to try.
num_evaluations = 10


# A function for generating random hyperparameters.
def generate_hyperparameters():
    return {
        "learning_rate": 10 ** np.random.uniform(-5, 1),
        "batch_size": np.random.randint(1, 100),
        "momentum": np.random.uniform(0, 1),
    }


def get_data_loaders(batch_size):
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
    )

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock(os.path.expanduser("~/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data", train=True, download=True, transform=mnist_transforms
            ),
            batch_size=batch_size,
            shuffle=True,
        )
    test_loader = torch.utils.data.DataLoader(
        datasets.MNIST("~/data", train=False, transform=mnist_transforms),
        batch_size=batch_size,
        shuffle=True,
    )
    return train_loader, test_loader


#######################################################################
# Setup: Defining the Neural Network
# ----------------------------------
#
# We define a small neural network to use in training. In addition,
# we created methods to train and test this neural network.


class ConvNet(nn.Module):
    """Simple two layer Convolutional Neural Network."""

    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)


def train(model, optimizer, train_loader, device=torch.device("cpu")):
    """Optimize the model with one pass over the data.

    Cuts off at 1024 samples to simplify training.
    """
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if batch_idx * len(data) > 1024:
            return
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, test_loader, device=torch.device("cpu")):
    """Checks the validation accuracy of the model.

    Cuts off at 512 samples for simplicity.
    """
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            if batch_idx * len(data) > 512:
                break
            data, target = data.to(device), target.to(device)
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct / total


#######################################################################
# Evaluating the Hyperparameters
# -------------------------------
#
# For a given configuration, the neural network created previously
# will be trained and return the accuracy of the model. These trained
# networks will then be tested for accuracy to find the best set of
# hyperparameters.
#
# The ``@ray.remote`` decorator defines a remote process.


@ray.remote
def evaluate_hyperparameters(config):
    model = ConvNet()
    train_loader, test_loader = get_data_loaders(config["batch_size"])
    optimizer = optim.SGD(
        model.parameters(), lr=config["learning_rate"], momentum=config["momentum"]
    )
    train(model, optimizer, train_loader)
    return test(model, test_loader)


#######################################################################
# Synchronous Evaluation of Randomly Generated Hyperparameters
# ------------------------------------------------------------
#
# We will create multiple sets of random hyperparameters for our neural
# network that will be evaluated in parallel.

# Keep track of the best hyperparameters and the best accuracy.
best_hyperparameters = None
best_accuracy = 0
# A list holding the object refs for all of the experiments that we have
# launched but have not yet been processed.
remaining_ids = []
# A dictionary mapping an experiment's object ref to its hyperparameters.
# hyerparameters used for that experiment.
hyperparameters_mapping = {}

###########################################################################
# Launch asynchronous parallel tasks for evaluating different
# hyperparameters. ``accuracy_id`` is an ObjectRef that acts as a handle to
# the remote task. It is used later to fetch the result of the task
# when the task finishes.

# Randomly generate sets of hyperparameters and launch a task to evaluate it.
for i in range(num_evaluations):
    hyperparameters = generate_hyperparameters()
    accuracy_id = evaluate_hyperparameters.remote(hyperparameters)
    remaining_ids.append(accuracy_id)
    hyperparameters_mapping[accuracy_id] = hyperparameters

###########################################################################
# Process each hyperparameter and corresponding accuracy in the order that
# they finish to store the hyperparameters with the best accuracy.

# Fetch and print the results of the tasks in the order that they complete.
while remaining_ids:
    # Use ray.wait to get the object ref of the first task that completes.
    done_ids, remaining_ids = ray.wait(remaining_ids)
    # There is only one return result by default.
    result_id = done_ids[0]

    hyperparameters = hyperparameters_mapping[result_id]
    accuracy = ray.get(result_id)
    print(
        """We achieve accuracy {:.3}% with
        learning_rate: {:.2}
        batch_size: {}
        momentum: {:.2}
      """.format(
            100 * accuracy,
            hyperparameters["learning_rate"],
            hyperparameters["batch_size"],
            hyperparameters["momentum"],
        )
    )
    if accuracy > best_accuracy:
        best_hyperparameters = hyperparameters
        best_accuracy = accuracy

# Record the best performing set of hyperparameters.
print(
    """Best accuracy over {} trials was {:.3} with
      learning_rate: {:.2}
      batch_size: {}
      momentum: {:.2}
      """.format(
        num_evaluations,
        100 * best_accuracy,
        best_hyperparameters["learning_rate"],
        best_hyperparameters["batch_size"],
        best_hyperparameters["momentum"],
    )
)
