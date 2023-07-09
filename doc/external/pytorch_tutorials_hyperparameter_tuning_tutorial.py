# -*- coding: utf-8 -*-
# flake8: noqa
"""
Hyperparameter tuning with Ray Tune
===================================

Hyperparameter tuning can make the difference between an average model and a highly
accurate one. Often simple things like choosing a different learning rate or changing
a network layer size can have a dramatic impact on your model performance.

Fortunately, there are tools that help with finding the best combination of parameters.
`Ray Tune <https://docs.ray.io/en/latest/tune.html>`_ is an industry standard tool for
distributed hyperparameter tuning. Ray Tune includes the latest hyperparameter search
algorithms, integrates with TensorBoard and other analysis libraries, and natively
supports distributed training through `Ray's distributed machine learning engine
<https://ray.io/>`_.

In this tutorial, we will show you how to integrate Ray Tune into your PyTorch
training workflow. We will extend `this tutorial from the PyTorch documentation
<https://pytorch.org/tutorials/beginner/blitz/cifar10_tutorial.html>`_ for training
a CIFAR10 image classifier.

As you will see, we only need to add some slight modifications. In particular, we
need to

1. wrap data loading and training in functions,
2. make some network parameters configurable,
3. add checkpointing (optional),
4. and define the search space for the model tuning

|

To run this tutorial, please make sure the following packages are
installed:

-  ``ray[tune]``: Distributed hyperparameter tuning library
-  ``torchvision``: For the data transformers

Setup / Imports
---------------
Let's start with the imports:
"""
from functools import partial
import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import random_split
import torchvision
import torchvision.transforms as transforms
from ray import tune
from ray.air import Checkpoint, session
from ray.tune.schedulers import ASHAScheduler

######################################################################
# Most of the imports are needed for building the PyTorch model. Only the last three
# imports are for Ray Tune.
#
# Data loaders
# ------------
# We wrap the data loaders in their own function and pass a global data directory.
# This way we can share a data directory between different trials.


def load_data(data_dir="./data"):
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )

    trainset = torchvision.datasets.CIFAR10(
        root=data_dir, train=True, download=True, transform=transform
    )

    testset = torchvision.datasets.CIFAR10(
        root=data_dir, train=False, download=True, transform=transform
    )

    return trainset, testset


######################################################################
# Configurable neural network
# ---------------------------
# We can only tune those parameters that are configurable.
# In this example, we can specify
# the layer sizes of the fully connected layers:


class Net(nn.Module):
    def __init__(self, l1=120, l2=84):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(3, 6, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, l1)
        self.fc2 = nn.Linear(l1, l2)
        self.fc3 = nn.Linear(l2, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = torch.flatten(x, 1)  # flatten all dimensions except batch
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


######################################################################
# The train function
# ------------------
# Now it gets interesting, because we introduce some changes to the example `from the PyTorch
# documentation <https://pytorch.org/tutorials/beginner/blitz/cifar10_tutorial.html>`_.
#
# We wrap the training script in a function ``train_cifar(config, data_dir=None)``.
# The ``config`` parameter will receive the hyperparameters we would like to
# train with. The ``data_dir`` specifies the directory where we load and store the data,
# so that multiple runs can share the same data source.
# We also load the model and optimizer state at the start of the run, if a checkpoint
# is provided. Further down in this tutorial you will find information on how
# to save the checkpoint and what it is used for.
#
# .. code-block:: python
#
#     net = Net(config["l1"], config["l2"])
#
#     checkpoint = session.get_checkpoint()
#
#     if checkpoint:
#         checkpoint_state = checkpoint.to_dict()
#         start_epoch = checkpoint_state["epoch"]
#         net.load_state_dict(checkpoint_state["net_state_dict"])
#         optimizer.load_state_dict(checkpoint_state["optimizer_state_dict"])
#     else:
#         start_epoch = 0
#
# The learning rate of the optimizer is made configurable, too:
#
# .. code-block:: python
#
#     optimizer = optim.SGD(net.parameters(), lr=config["lr"], momentum=0.9)
#
# We also split the training data into a training and validation subset. We thus train on
# 80% of the data and calculate the validation loss on the remaining 20%. The batch sizes
# with which we iterate through the training and test sets are configurable as well.
#
# Adding (multi) GPU support with DataParallel
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Image classification benefits largely from GPUs. Luckily, we can continue to use
# PyTorch's abstractions in Ray Tune. Thus, we can wrap our model in ``nn.DataParallel``
# to support data parallel training on multiple GPUs:
#
# .. code-block:: python
#
#     device = "cpu"
#     if torch.cuda.is_available():
#         device = "cuda:0"
#         if torch.cuda.device_count() > 1:
#             net = nn.DataParallel(net)
#     net.to(device)
#
# By using a ``device`` variable we make sure that training also works when we have
# no GPUs available. PyTorch requires us to send our data to the GPU memory explicitly,
# like this:
#
# .. code-block:: python
#
#     for i, data in enumerate(trainloader, 0):
#         inputs, labels = data
#         inputs, labels = inputs.to(device), labels.to(device)
#
# The code now supports training on CPUs, on a single GPU, and on multiple GPUs. Notably, Ray
# also supports `fractional GPUs <https://docs.ray.io/en/master/using-ray-with-gpus.html#fractional-gpus>`_
# so we can share GPUs among trials, as long as the model still fits on the GPU memory. We'll come back
# to that later.
#
# Communicating with Ray Tune
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# The most interesting part is the communication with Ray Tune:
#
# .. code-block:: python
#
#     checkpoint_data = {
#         "epoch": epoch,
#         "net_state_dict": net.state_dict(),
#         "optimizer_state_dict": optimizer.state_dict(),
#     }
#     checkpoint = Checkpoint.from_dict(checkpoint_data)
#
#     session.report(
#         {"loss": val_loss / val_steps, "accuracy": correct / total},
#         checkpoint=checkpoint,
#     )
#
# Here we first save a checkpoint and then report some metrics back to Ray Tune. Specifically,
# we send the validation loss and accuracy back to Ray Tune. Ray Tune can then use these metrics
# to decide which hyperparameter configuration lead to the best results. These metrics
# can also be used to stop bad performing trials early in order to avoid wasting
# resources on those trials.
#
# The checkpoint saving is optional, however, it is necessary if we wanted to use advanced
# schedulers like
# `Population Based Training <https://docs.ray.io/en/master/tune/tutorials/tune-advanced-tutorial.html>`_.
# Also, by saving the checkpoint we can later load the trained models and validate them
# on a test set. Lastly, saving checkpoints is useful for fault tolerance, and it allows
# us to interrupt training and continue training later.
#
# Full training function
# ~~~~~~~~~~~~~~~~~~~~~~
#
# The full code example looks like this:


def train_cifar(config, data_dir=None):
    net = Net(config["l1"], config["l2"])

    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda:0"
        if torch.cuda.device_count() > 1:
            net = nn.DataParallel(net)
    net.to(device)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(net.parameters(), lr=config["lr"], momentum=0.9)

    checkpoint = session.get_checkpoint()

    if checkpoint:
        checkpoint_state = checkpoint.to_dict()
        start_epoch = checkpoint_state["epoch"]
        net.load_state_dict(checkpoint_state["net_state_dict"])
        optimizer.load_state_dict(checkpoint_state["optimizer_state_dict"])
    else:
        start_epoch = 0

    trainset, testset = load_data(data_dir)

    test_abs = int(len(trainset) * 0.8)
    train_subset, val_subset = random_split(
        trainset, [test_abs, len(trainset) - test_abs]
    )

    trainloader = torch.utils.data.DataLoader(
        train_subset, batch_size=int(config["batch_size"]), shuffle=True, num_workers=8
    )
    valloader = torch.utils.data.DataLoader(
        val_subset, batch_size=int(config["batch_size"]), shuffle=True, num_workers=8
    )

    for epoch in range(start_epoch, 10):  # loop over the dataset multiple times
        running_loss = 0.0
        epoch_steps = 0
        for i, data in enumerate(trainloader, 0):
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
                running_loss = 0.0

        # Validation loss
        val_loss = 0.0
        val_steps = 0
        total = 0
        correct = 0
        for i, data in enumerate(valloader, 0):
            with torch.no_grad():
                inputs, labels = data
                inputs, labels = inputs.to(device), labels.to(device)

                outputs = net(inputs)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()

                loss = criterion(outputs, labels)
                val_loss += loss.cpu().numpy()
                val_steps += 1

        checkpoint_data = {
            "epoch": epoch,
            "net_state_dict": net.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
        }
        checkpoint = Checkpoint.from_dict(checkpoint_data)

        session.report(
            {"loss": val_loss / val_steps, "accuracy": correct / total},
            checkpoint=checkpoint,
        )
    print("Finished Training")


######################################################################
# As you can see, most of the code is adapted directly from the original example.
#
# Test set accuracy
# -----------------
# Commonly the performance of a machine learning model is tested on a hold-out test
# set with data that has not been used for training the model. We also wrap this in a
# function:


def test_accuracy(net, device="cpu"):
    trainset, testset = load_data()

    testloader = torch.utils.data.DataLoader(
        testset, batch_size=4, shuffle=False, num_workers=2
    )

    correct = 0
    total = 0
    with torch.no_grad():
        for data in testloader:
            images, labels = data
            images, labels = images.to(device), labels.to(device)
            outputs = net(images)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()

    return correct / total


######################################################################
# The function also expects a ``device`` parameter, so we can do the
# test set validation on a GPU.
#
# Configuring the search space
# ----------------------------
# Lastly, we need to define Ray Tune's search space. Here is an example:
#
# .. code-block:: python
#
#     config = {
#         "l1": tune.choice([2 ** i for i in range(9)]),
#         "l2": tune.choice([2 ** i for i in range(9)]),
#         "lr": tune.loguniform(1e-4, 1e-1),
#         "batch_size": tune.choice([2, 4, 8, 16])
#     }
#
# The ``tune.choice()`` accepts a list of values that are uniformly sampled from.
# In this example, the ``l1`` and ``l2`` parameters
# should be powers of 2 between 4 and 256, so either 4, 8, 16, 32, 64, 128, or 256.
# The ``lr`` (learning rate) should be uniformly sampled between 0.0001 and 0.1. Lastly,
# the batch size is a choice between 2, 4, 8, and 16.
#
# At each trial, Ray Tune will now randomly sample a combination of parameters from these
# search spaces. It will then train a number of models in parallel and find the best
# performing one among these. We also use the ``ASHAScheduler`` which will terminate bad
# performing trials early.
#
# We wrap the ``train_cifar`` function with ``functools.partial`` to set the constant
# ``data_dir`` parameter. We can also tell Ray Tune what resources should be
# available for each trial:
#
# .. code-block:: python
#
#     gpus_per_trial = 2
#     # ...
#     result = tune.run(
#         partial(train_cifar, data_dir=data_dir),
#         resources_per_trial={"cpu": 8, "gpu": gpus_per_trial},
#         config=config,
#         num_samples=num_samples,
#         scheduler=scheduler,
#         checkpoint_at_end=True)
#
# You can specify the number of CPUs, which are then available e.g.
# to increase the ``num_workers`` of the PyTorch ``DataLoader`` instances. The selected
# number of GPUs are made visible to PyTorch in each trial. Trials do not have access to
# GPUs that haven't been requested for them - so you don't have to care about two trials
# using the same set of resources.
#
# Here we can also specify fractional GPUs, so something like ``gpus_per_trial=0.5`` is
# completely valid. The trials will then share GPUs among each other.
# You just have to make sure that the models still fit in the GPU memory.
#
# After training the models, we will find the best performing one and load the trained
# network from the checkpoint file. We then obtain the test set accuracy and report
# everything by printing.
#
# The full main function looks like this:


def main(num_samples=10, max_num_epochs=10, gpus_per_trial=2):
    data_dir = os.path.abspath("./data")
    load_data(data_dir)
    config = {
        "l1": tune.choice([2**i for i in range(9)]),
        "l2": tune.choice([2**i for i in range(9)]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "batch_size": tune.choice([2, 4, 8, 16]),
    }
    scheduler = ASHAScheduler(
        metric="loss",
        mode="min",
        max_t=max_num_epochs,
        grace_period=1,
        reduction_factor=2,
    )
    result = tune.run(
        partial(train_cifar, data_dir=data_dir),
        resources_per_trial={"cpu": 2, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
    )

    best_trial = result.get_best_trial("loss", "min", "last")
    print(f"Best trial config: {best_trial.config}")
    print(f"Best trial final validation loss: {best_trial.last_result['loss']}")
    print(f"Best trial final validation accuracy: {best_trial.last_result['accuracy']}")

    best_trained_model = Net(best_trial.config["l1"], best_trial.config["l2"])
    device = "cpu"
    if torch.cuda.is_available():
        device = "cuda:0"
        if gpus_per_trial > 1:
            best_trained_model = nn.DataParallel(best_trained_model)
    elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
        device = torch.device('mps')
        if gpus_per_trial > 1:
            best_trained_model = nn.DataParallel(best_trained_model)
    best_trained_model.to(device)

    best_checkpoint = best_trial.checkpoint.to_air_checkpoint()
    best_checkpoint_data = best_checkpoint.to_dict()

    best_trained_model.load_state_dict(best_checkpoint_data["net_state_dict"])

    test_acc = test_accuracy(best_trained_model, device)
    print("Best trial test set accuracy: {}".format(test_acc))


if __name__ == "__main__":
    # sphinx_gallery_start_ignore
    # Fixes ``AttributeError: '_LoggingTee' object has no attribute 'fileno'``.
    # This is only needed to run with sphinx-build.
    import sys

    sys.stdout.fileno = lambda: False
    # sphinx_gallery_end_ignore
    # You can change the number of GPUs per trial here:
    main(num_samples=10, max_num_epochs=10, gpus_per_trial=0)


######################################################################
# If you run the code, an example output could look like this:
#
# ::
#
#     Number of trials: 10/10 (10 TERMINATED)
#     +-----+--------------+------+------+-------------+--------+---------+------------+
#     | ... |   batch_size |   l1 |   l2 |          lr |   iter |    loss |   accuracy |
#     |-----+--------------+------+------+-------------+--------+---------+------------|
#     | ... |            2 |    1 |  256 | 0.000668163 |      1 | 2.31479 |     0.0977 |
#     | ... |            4 |   64 |    8 | 0.0331514   |      1 | 2.31605 |     0.0983 |
#     | ... |            4 |    2 |    1 | 0.000150295 |      1 | 2.30755 |     0.1023 |
#     | ... |           16 |   32 |   32 | 0.0128248   |     10 | 1.66912 |     0.4391 |
#     | ... |            4 |    8 |  128 | 0.00464561  |      2 | 1.7316  |     0.3463 |
#     | ... |            8 |  256 |    8 | 0.00031556  |      1 | 2.19409 |     0.1736 |
#     | ... |            4 |   16 |  256 | 0.00574329  |      2 | 1.85679 |     0.3368 |
#     | ... |            8 |    2 |    2 | 0.00325652  |      1 | 2.30272 |     0.0984 |
#     | ... |            2 |    2 |    2 | 0.000342987 |      2 | 1.76044 |     0.292  |
#     | ... |            4 |   64 |   32 | 0.003734    |      8 | 1.53101 |     0.4761 |
#     +-----+--------------+------+------+-------------+--------+---------+------------+
#
#     Best trial config: {'l1': 64, 'l2': 32, 'lr': 0.0037339984519545164, 'batch_size': 4}
#     Best trial final validation loss: 1.5310075663924216
#     Best trial final validation accuracy: 0.4761
#     Best trial test set accuracy: 0.4737
#
# Most trials have been stopped early in order to avoid wasting resources.
# The best performing trial achieved a validation accuracy of about 47%, which could
# be confirmed on the test set.
#
# So that's it! You can now tune the parameters of your PyTorch models.
