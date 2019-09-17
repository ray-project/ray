"""
Parameter Server
================

Parameter servers are a core part of many machine learning applications.
Their role is to store the parameters of a machine learning model (e.g.,
the weights of a neural network) and to serve them to clients (
clients are often workers that process data and compute updates to the
parameters).

This document walks through how to implement simple synchronous and asynchronous
parameter servers using actors.

.. note:: This example is mainly a proof of concept and will not achieve
    extremely high throughput.

To run the application, first install some dependencies.

.. code-block:: bash

  pip install torch torchvision filelock


What is a Parameter Server?
---------------------------

A parameter server is a store used for training machine
learning models on a cluster. It holds the parameters of a
machine-learning model (e.g., a neural network).

.. image:: ../images/param_actor.png
    :align: center

Let's first define some helper functions and import some dependencies.

"""
import os
import torch
import torch.nn as nn
import torch.nn.functional as F
from torchvision import datasets, transforms
from filelock import FileLock
import numpy as np

import ray


def get_data_loader():
    """Safely downloads data. Returns training/validation set dataloader."""
    mnist_transforms = transforms.Compose(
        [transforms.ToTensor(),
         transforms.Normalize((0.1307, ), (0.3081, ))])

    # We add FileLock here because multiple workers will want to
    # download data, and this may cause overwrites since
    # DataLoader is not threadsafe.
    with FileLock(os.path.expanduser("~/data.lock")):
        train_loader = torch.utils.data.DataLoader(
            datasets.MNIST(
                "~/data",
                train=True,
                download=True,
                transform=mnist_transforms),
            batch_size=128,
            shuffle=True)
        test_loader = torch.utils.data.DataLoader(
            datasets.MNIST("~/data", train=False, transform=mnist_transforms),
            batch_size=128,
            shuffle=True)
    return train_loader, test_loader


def evaluate(model, test_loader):
    """Evaluates the validation dataset for validation accuracy."""
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(test_loader):
            # This is only set to finish evaluation faster.
            if batch_idx * len(data) > 1024:
                break
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()
    return 100. * correct / total


#######################################################################
# Defining the Neural Network
# ---------------------------
#
# We define a small neural network to use in training. We provide
# some helper functions for obtaining data, including getter/setter
# methods for gradients and weights.

class ConvNet(nn.Module):
    """Small ConvNet for MNIST."""
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)

    def get_weights(self):
        return {k: v.cpu() for k, v in self.state_dict().items()}

    def set_weights(self, weights):
        self.load_state_dict(weights)

    def get_gradients(self):
        grads = []
        for p in self.parameters():
            grad = None if p.grad is None else p.grad.data.cpu().numpy()
            grads.append(grad)
        return grads

    def set_gradients(self, gradients):
        for g, p in zip(gradients, self.parameters()):
            if g is not None:
                p.grad = torch.from_numpy(g)


###########################################################################
# Defining the Parameter Server
# -----------------------------
# The parameter server will hold a copy of the model.
# During training, it will
#   1. receive gradients and apply them to its model.
#   2. Send the updated model back to the workers.
#
# The @ray.remote decorator defines a remote process. It wraps the
# ParameterServer class and allows it to be instantiated as a
# remote process or actor.

@ray.remote
class ParameterServer(object):
    def __init__(self, lr):
        self.model = ConvNet()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)

    def apply_gradients(self, *gradients):
        summed_gradients = [
            np.stack(gradient_zip).sum(axis=0)
            for gradient_zip in zip(*gradients)
        ]
        self.optimizer.zero_grad()
        self.model.set_gradients(summed_gradients)
        self.optimizer.step()
        return self.model.get_weights()

    def get_weights(self):
        return self.model.get_weights()

###########################################################################
# Defining the Worker
# -------------------
# The worker will also hold a copy of the model. During training. it will
# continuously evaluate data and send gradients
# to the parameter server. The worker will update its model after sending
# gradients to the parameter server.


@ray.remote
class DataWorker(object):
    def __init__(self):
        self.model = ConvNet()
        self.data_iterator = iter(get_data_loader()[0])

    def compute_gradients(self, weights):
        self.model.set_weights(weights)
        try:
            data, target = next(self.data_iterator)
        except StopIteration:
            self.data_iterator = iter(get_data_loader()[0])
            data, target = next(self.data_iterator)
        self.model.zero_grad()
        output = self.model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        return self.model.get_gradients()

###########################################################################
# Synchronous Parameter Server Training
# -------------------------------------
# We'll now create a synchronous parameter server training scheme. We'll first
# instantiate a process for the parameter server, along with multiple
# workers.

iterations = 200
num_workers = 2

ray.init(ignore_reinit_error=True)
ps = ParameterServer.remote(1e-2)
workers = [DataWorker.remote() for i in range(num_workers)]

###########################################################################
# We'll also instantiate a model on the driver process to evaluate the test
# accuracy during training.

model = ConvNet()
test_loader = get_data_loader()[1]

###########################################################################
# Training alternates between computing the gradients given the current weights
# from the parameter server and updating the parameter server's weights with the
# resulting gradients.

print("Running Synchronous Parameter Server Training.")
current_weights = ps.get_weights.remote()
for i in range(iterations):
    gradients = [
        worker.compute_gradients.remote(current_weights)
        for worker in workers
    ]
    current_weights = ps.apply_gradients.remote(*gradients)

    if i % 10 == 0:
        # Evaluate the current model.
        model.set_weights(ray.get(current_weights))
        accuracy = evaluate(model, test_loader)
        print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))

print("Final accuracy is {:.1f}.".format(accuracy))
# We can
ray.shutdown()

###########################################################################
# Synchronous Parameter Server Training
# -------------------------------------
# We'll now create a synchronous parameter server training scheme. We'll first
# instantiate a process for the parameter server, along with multiple
# workers.


print("Running Asynchronous Parameter Server Training.")

ray.init(ignore_reinit_error=True)
ps = ParameterServer.remote(1e-2)
workers = [DataWorker.remote() for i in range(num_workers)]


###########################################################################
# Here, workers will asynchronously compute the gradients given its
# current weights, and send these gradients to the parameter server as
# soon as it is ready. When the Parameter server finishes applying the
# new gradient, it will send back a copy of the current weights to the
# worker. The worker will then update the weights and continue.

current_weights = ps.get_weights.remote()
gradients = {
    worker.compute_gradients.remote(current_weights): worker
    for worker in workers
}
for i in range(iterations * num_workers):
    [ready_gradient], _ = ray.wait(list(gradients))
    worker = gradients.pop(ready_gradient)

    # Compute and apply gradients.
    current_weights = ps.apply_gradients.remote(*[ready_gradient])
    gradients[worker.compute_gradients.remote(current_weights)] = worker

    if i % 10 == 0:
        # Evaluate the current model.
        model.set_weights(ray.get(current_weights))
        accuracy = evaluate(model, test_loader)
        print("Iter {}: \taccuracy is {:.1f}".format(i, accuracy))

print("Final accuracy is {:.1f}.".format(accuracy))

##############################################################################
# Final Thoughts
# --------------
#
# This approach is powerful because it enables users to implement a parameter
# server with a few lines of code as part of a Python application.
# As a result, this simplifies the deployment of applications that use
# parameter servers and to modify the behavior of the parameter server.
# For example, sharding the parameter server, changing the update rule,
# switch between asynchronous and synchronous updates, ignoring
# straggler workers, or any number of other customizations,
# will only require a few extra lines of code.
