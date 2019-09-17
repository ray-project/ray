"""
Parallel Asynchronous Hyperparameter Tuning
===========================================

In this example, we'll demonstrate how to quickly write a hyperparameter
tuning script that evaluates a set of hyperparameters in parallel.

This script will demonstrate how to use the ``ray.remote`` API for functions
along with ``ray.wait``.

.. important:: For a production-grade implementation of distributed
    hyperparameter tuning, use `Tune`_, a scalable hyperparameter
    tuning library built using Ray's Actor API.

.. _`Tune`: https://ray.readthedocs.io/en/latest/tune.html
"""
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
    return {"learning_rate": 10 ** np.random.uniform(-5, 5),
            "batch_size": np.random.randint(1, 100),
            "dropout": np.random.uniform(0, 1),
            "stddev": 10 ** np.random.uniform(-5, 5)}


def get_data_loaders():
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock("./data.lock"):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "./data",
                train=True,
                download=True,
                transform=mnist_transforms),
            batch_size=64,
            shuffle=True)
    test_loader = torch.utils.data.DataLoader(
        datasets.MNIST("./data", train=False, transform=mnist_transforms),
        batch_size=64,
        shuffle=True)
    return train_loader, test_loader


class ConvNet(nn.Module):
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
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if batch_idx * len(data) > EPOCH_SIZE:
            return
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, test_loader, device=torch.device("cpu")):
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            if batch_idx * len(data) > TEST_SIZE:
                break
            data, target = data.to(device), target.to(device)
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct / total

@ray.remote
def evaluate_hyperparameters(config):
    model = ConvNet()
    train_loader, test_loader = get_data_loaders()
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"])
    train(model, optimizer, train_loader)
    return test(model, test_loader)



# Keep track of the best hyperparameters and the best accuracy.
best_hyperparameters = None
best_accuracy = 0
# This list holds the object IDs for all of the experiments that we have
# launched and that have not yet been processed.
remaining_ids = []
# This is a dictionary mapping the object ID of an experiment to the
# hyerparameters used for that experiment.
hyperparameters_mapping = {}

# Randomly generate some hyperparameters, and launch a task for each set.
for i in range(trials):
    hyperparameters = generate_hyperparameters()
    accuracy_id = evaluate_hyperparameter.remote(hyperparameters)
    remaining_ids += [accuracy_id]
    # Keep track of which hyperparameters correspond to this experiment.
    hyperparameters_mapping[accuracy_id] = hyperparameters

# Fetch and print the results of the tasks in the order that they complete.
while remaining_ids:
    # Use ray.wait to get the object ID of the first task that completes.
    [result_id], remaining_ids = ray.wait(remaining_ids)
    # Process the output of this task.
    hyperparameters = hyperparameters_mapping[result_id]
    accuracy, _ = ray.get(result_id)
    print("""We achieve accuracy {:.3}% with
        learning_rate: {:.2}
        batch_size: {}
        dropout: {:.2}
        stddev: {:.2}
      """.format(100 * accuracy,
                 hyperparameters["learning_rate"],
                 hyperparameters["batch_size"],
                 hyperparameters["dropout"],
                 hyperparameters["stddev"]))
    if accuracy > best_accuracy:
        best_hyperparameters = hyperparameters
        best_accuracy = accuracy

# Record the best performing set of hyperparameters.
print("""Best accuracy over {} trials was {:.3} with
      learning_rate: {:.2}
      batch_size: {}
      dropout: {:.2}
      stddev: {:.2}
      """.format(trials, 100 * best_accuracy,
                 best_hyperparameters["learning_rate"],
                 best_hyperparameters["batch_size"],
                 best_hyperparameters["dropout"],
                 best_hyperparameters["stddev"]))
