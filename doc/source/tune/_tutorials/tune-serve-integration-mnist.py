# flake8: noqa
"""
Model selection and serving with Ray Tune and Ray Serve
=======================================================
This tutorial will show you an end-to-end example how to train a
model using Ray Tune on incrementally arriving data and deploy
the model using Ray Serve.

A machine learning workflow can be quite simple: You decide on
the objective you're trying to solve, collect and annotate the
data, and build a model to hopefully solve your problem. But
usually the work is not over yet. First, you would likely continue
to do some hyperparameter optimization to obtain the best possible
model (called *model selection*). Second, your trained model
somehow has to be moved to production - in other words, users
or services should be enabled to use your model to actually make
predictions. This part is called *model serving*.

Fortunately, Ray includes two libraries that help you with these
two steps: Ray Tune and Ray Serve. And even more, they compliment
each other nicely. Most notably, both are able to scale up your
workloads easily - so both your model training and serving benefit
from additional resources and can adapt to your environment. If you
need to train on more data or have more hyperparameters to tune,
Ray Tune can leverage your whole cluster for training. If you have
many users doing inference on your served models, Ray Serve can
automatically distribute the inference to multiple nodes.

This tutorial will show you an end-to-end example how to train a MNIST
image classifier on incrementally arriving data and automatically
serve an updated model on a HTTP endpoint.

By the end of this tutorial you will be able to

1. Do hyperparameter optimization on a simple MNIST classifier
2. Continue to train this classifier from an existing model with
   newly arriving data
3. Automatically create and serve data deployments with Ray Serve

Roadmap and desired functionality
---------------------------------
The general idea of this example is that we simulate newly arriving
data each day. So at day 0 we might have some initial data available
already, but at each day, new data arrives.

Our approach here is that we offer two ways to train: From scratch and
from an existing model. Maybe you would like to train and select models
from scratch each week with all data available until then, e.g. each
Sunday, like this:

.. code-block:: bash

    # Train with all data available at day 0
    python tune-serve-integration-mnist.py --from_scratch --day 0

During the other days you might want to improve your model, but
not train everything from scratch, saving some cluster resources.

.. code-block:: bash

    # Train with data arriving between day 0 and day 1
    python tune-serve-integration-mnist.py --from_existing --day 1
    # Train with incremental data on the other days, too
    python tune-serve-integration-mnist.py --from_existing --day 2
    python tune-serve-integration-mnist.py --from_existing --day 3
    python tune-serve-integration-mnist.py --from_existing --day 4
    python tune-serve-integration-mnist.py --from_existing --day 5
    python tune-serve-integration-mnist.py --from_existing --day 6
    # Retrain from scratch every 7th day:
    python tune-serve-integration-mnist.py --from_scratch --day 7

This example will support both modes. After each model selection run,
we will tell Ray Serve to serve an updated model. We also include a
small utility to query our served model to see if it works as it should.

.. code-block:: bash

    $ python tune-serve-integration-mnist.py --query 6
    Querying model with example #6. Label = 1, Response = 1, Correct = True

Imports
-------
Let's start with our dependencies. Most of these should be familiar
if you worked with PyTorch before. The most notable import for Ray
is the ``from ray import tune, serve`` import statement - which
includes almost all the things we need from the Ray side.
"""
import argparse
import json
import os
import shutil
import sys
from functools import partial
from math import ceil

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import ray
from ray import tune, serve
from ray.serve.exceptions import RayServeException
from ray.tune import CLIReporter
from ray.tune.schedulers import ASHAScheduler

from torch.utils.data import random_split, Subset
from torchvision.datasets import MNIST
from torchvision.transforms import transforms


#######################################################################
# Data interface
# --------------
# Let's start with a simulated data interface. This class acts as the
# interface between your training code and your database. We simulate
# that new data arrives each day with a ``day`` parameter. So, calling
# ``get_data(day=3)`` would return all data we received until day 3.
# We also implement an incremental data method, so calling
# ``get_incremental_data(day=3)`` would return all data collected
# between day 2 and day 3.
class MNISTDataInterface(object):
    """Data interface. Simulates that new data arrives every day."""

    def __init__(self, data_dir, max_days=10):
        self.data_dir = data_dir
        self.max_days = max_days

        transform = transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        )
        self.dataset = MNIST(
            self.data_dir, train=True, download=True, transform=transform
        )

    def _get_day_slice(self, day=0):
        if day < 0:
            return 0
        n = len(self.dataset)
        # Start with 30% of the data, get more data each day
        return min(n, ceil(n * (0.3 + 0.7 * day / self.max_days)))

    def get_data(self, day=0):
        """Get complete normalized train and validation data to date."""
        end = self._get_day_slice(day)

        available_data = Subset(self.dataset, list(range(end)))
        train_n = int(0.8 * end)  # 80% train data, 20% validation data

        return random_split(available_data, [train_n, end - train_n])

    def get_incremental_data(self, day=0):
        """Get next normalized train and validation data day slice."""
        start = self._get_day_slice(day - 1)
        end = self._get_day_slice(day)

        available_data = Subset(self.dataset, list(range(start, end)))
        train_n = int(0.8 * (end - start))  # 80% train data, 20% validation data

        return random_split(available_data, [train_n, end - start - train_n])


#######################################################################
# PyTorch neural network classifier
# ---------------------------------
# Next, we will introduce our PyTorch neural network model and the
# train and test function. These are adapted directly from
# our :doc:`PyTorch MNIST example </tune/examples/mnist_pytorch>`.
# We only introduced an additional neural network layer with a configurable
# layer size. This is not strictly needed for learning good performance on
# MNIST, but it is useful to demonstrate scenarios where your hyperparameter
# search space affects the model complexity.
class ConvNet(nn.Module):
    def __init__(self, layer_size=192):
        super(ConvNet, self).__init__()
        self.layer_size = layer_size
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, self.layer_size)
        self.out = nn.Linear(self.layer_size, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 3))
        x = x.view(-1, 192)
        x = self.fc(x)
        x = self.out(x)
        return F.log_softmax(x, dim=1)


def train(model, optimizer, train_loader, device=None):
    device = device or torch.device("cpu")
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()


def test(model, data_loader, device=None):
    device = device or torch.device("cpu")
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_idx, (data, target) in enumerate(data_loader):
            data, target = data.to(device), target.to(device)
            outputs = model(data)
            _, predicted = torch.max(outputs.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()

    return correct / total


#######################################################################
# Tune trainable for model selection
# ----------------------------------
# We'll now define our Tune trainable function. This function takes
# a ``config`` parameter containing the hyperparameters we should train
# the model on, and will start a full training run. This means it
# will take care of creating the model and optimizer and repeatedly
# call the ``train`` function to train the model. Also, this function
# will report the training progress back to Tune.
def train_mnist(
    config,
    start_model=None,
    checkpoint_dir=None,
    num_epochs=10,
    use_gpus=False,
    data_fn=None,
    day=0,
):
    # Create model
    use_cuda = use_gpus and torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")
    model = ConvNet(layer_size=config["layer_size"]).to(device)

    # Create optimizer
    optimizer = optim.SGD(
        model.parameters(), lr=config["lr"], momentum=config["momentum"]
    )

    # Load checkpoint, or load start model if no checkpoint has been
    # passed and a start model is specified
    load_dir = None
    if checkpoint_dir:
        load_dir = checkpoint_dir
    elif start_model:
        load_dir = start_model

    if load_dir:
        model_state, optimizer_state = torch.load(os.path.join(load_dir, "checkpoint"))
        model.load_state_dict(model_state)
        optimizer.load_state_dict(optimizer_state)

    # Get full training datasets
    train_dataset, validation_dataset = data_fn(day=day)

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=config["batch_size"], shuffle=True
    )

    validation_loader = torch.utils.data.DataLoader(
        validation_dataset, batch_size=config["batch_size"], shuffle=True
    )

    for i in range(num_epochs):
        train(model, optimizer, train_loader, device)
        acc = test(model, validation_loader, device)
        if i == num_epochs - 1:
            with tune.checkpoint_dir(step=i) as checkpoint_dir:
                torch.save(
                    (model.state_dict(), optimizer.state_dict()),
                    os.path.join(checkpoint_dir, "checkpoint"),
                )
            tune.report(mean_accuracy=acc, done=True)
        else:
            tune.report(mean_accuracy=acc)


#######################################################################
# Configuring the search space and starting Ray Tune
# --------------------------------------------------
# We would like to support two modes of training the model: Training
# a model from scratch, and continuing to train a model from an
# existing one.
#
# This is our function to train a number of models with different
# hyperparameters from scratch, i.e. from all data that is available
# until the given day. Our search space can thus also contain parameters
# that affect the model complexity (such as the layer size), since it
# does not have to be compatible to an existing model.
def tune_from_scratch(num_samples=10, num_epochs=10, gpus_per_trial=0.0, day=0):
    data_interface = MNISTDataInterface("~/data", max_days=10)
    num_examples = data_interface._get_day_slice(day)

    config = {
        "batch_size": tune.choice([16, 32, 64]),
        "layer_size": tune.choice([32, 64, 128, 192]),
        "lr": tune.loguniform(1e-4, 1e-1),
        "momentum": tune.uniform(0.1, 0.9),
    }

    scheduler = ASHAScheduler(
        metric="mean_accuracy",
        mode="max",
        max_t=num_epochs,
        grace_period=1,
        reduction_factor=2,
    )

    reporter = CLIReporter(
        parameter_columns=["layer_size", "lr", "momentum", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"],
    )

    analysis = tune.run(
        partial(
            train_mnist,
            start_model=None,
            data_fn=data_interface.get_data,
            num_epochs=num_epochs,
            use_gpus=True if gpus_per_trial > 0 else False,
            day=day,
        ),
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        verbose=0,
        name="tune_serve_mnist_fromscratch",
    )

    best_trial = analysis.get_best_trial("mean_accuracy", "max", "last")
    best_accuracy = best_trial.metric_analysis["mean_accuracy"]["last"]
    best_trial_config = best_trial.config
    best_checkpoint = best_trial.checkpoint.value

    return best_accuracy, best_trial_config, best_checkpoint, num_examples


#######################################################################
# To continue training from an existing model, we can use this function
# instead. It takes a starting model (a checkpoint) as a parameter and
# the old config.
#
# Note that this time the search space does _not_ contain the
# layer size parameter. Since we continue to train an existing model,
# we cannot change the layer size mid training, so we just continue
# to use the existing one.
def tune_from_existing(
    start_model, start_config, num_samples=10, num_epochs=10, gpus_per_trial=0.0, day=0
):
    data_interface = MNISTDataInterface("/tmp/mnist_data", max_days=10)
    num_examples = data_interface._get_day_slice(day) - data_interface._get_day_slice(
        day - 1
    )

    config = start_config.copy()
    config.update(
        {
            "batch_size": tune.choice([16, 32, 64]),
            "lr": tune.loguniform(1e-4, 1e-1),
            "momentum": tune.uniform(0.1, 0.9),
        }
    )

    scheduler = ASHAScheduler(
        metric="mean_accuracy",
        mode="max",
        max_t=num_epochs,
        grace_period=1,
        reduction_factor=2,
    )

    reporter = CLIReporter(
        parameter_columns=["lr", "momentum", "batch_size"],
        metric_columns=["mean_accuracy", "training_iteration"],
    )

    analysis = tune.run(
        partial(
            train_mnist,
            start_model=start_model,
            data_fn=data_interface.get_incremental_data,
            num_epochs=num_epochs,
            use_gpus=True if gpus_per_trial > 0 else False,
            day=day,
        ),
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        verbose=0,
        name="tune_serve_mnist_fromsexisting",
    )

    best_trial = analysis.get_best_trial("mean_accuracy", "max", "last")
    best_accuracy = best_trial.metric_analysis["mean_accuracy"]["last"]
    best_trial_config = best_trial.config
    best_checkpoint = best_trial.checkpoint.value

    return best_accuracy, best_trial_config, best_checkpoint, num_examples


#######################################################################
# Serving tuned models with Ray Serve
# -----------------------------------
# Let's now turn to the model serving part with Ray Serve. Serve allows
# you to deploy your models as multiple _deployments_. Broadly speaking,
# a deployment handles incoming requests and replies with a result. For
# instance, our MNIST deployment takes an image as input and outputs the
# digit it recognized from it. This deployment can be exposed over HTTP.
#
# First, we will define our deployment. This loads our PyTorch
# MNIST model from a checkpoint, takes an image as an input and
# outputs our digit prediction according to our trained model:
@serve.deployment(name="mnist", route_prefix="/mnist")
class MNISTDeployment:
    def __init__(self, checkpoint_dir, config, metrics, use_gpu=False):
        self.checkpoint_dir = checkpoint_dir
        self.config = config
        self.metrics = metrics

        use_cuda = use_gpu and torch.cuda.is_available()
        self.device = torch.device("cuda" if use_cuda else "cpu")
        model = ConvNet(layer_size=self.config["layer_size"]).to(self.device)

        model_state, optimizer_state = torch.load(
            os.path.join(self.checkpoint_dir, "checkpoint"), map_location=self.device
        )
        model.load_state_dict(model_state)

        self.model = model

    def __call__(self, flask_request):
        images = torch.tensor(flask_request.json["images"])
        images = images.to(self.device)
        outputs = self.model(images)
        predicted = torch.max(outputs.data, 1)[1]
        return {"result": predicted.numpy().tolist()}


#######################################################################
# We would like to have a fixed location where we store the currently
# active model. We call this directory ``model_dir``. Every time we
# would like to update our model, we copy the checkpoint of the new
# model to this directory. We then update the deployment to the new version.
def serve_new_model(model_dir, checkpoint, config, metrics, day, use_gpu=False):
    print("Serving checkpoint: {}".format(checkpoint))

    checkpoint_path = _move_checkpoint_to_model_dir(
        model_dir, checkpoint, config, metrics
    )

    serve.start(detached=True)
    MNISTDeployment.deploy(checkpoint_path, config, metrics, use_gpu)


def _move_checkpoint_to_model_dir(model_dir, checkpoint, config, metrics):
    """Move backend checkpoint to a central `model_dir` on the head node.
    If you would like to run Serve on multiple nodes, you might want to
    move the checkpoint to a shared storage, like Amazon S3, instead."""
    os.makedirs(model_dir, 0o755, exist_ok=True)

    checkpoint_path = os.path.join(model_dir, "checkpoint")
    meta_path = os.path.join(model_dir, "meta.json")

    if os.path.exists(checkpoint_path):
        shutil.rmtree(checkpoint_path)

    shutil.copytree(checkpoint, checkpoint_path)

    with open(meta_path, "wt") as fp:
        json.dump(dict(config=config, metrics=metrics), fp)

    return checkpoint_path


#######################################################################
# Since we would like to continue training from the current existing
# model, we introduce an utility function that fetches the currently
# served checkpoint as well as the hyperparameter config and achieved
# accuracy.
def get_current_model(model_dir):
    checkpoint_path = os.path.join(model_dir, "checkpoint")
    meta_path = os.path.join(model_dir, "meta.json")

    if not os.path.exists(checkpoint_path) or not os.path.exists(meta_path):
        return None, None, None

    with open(meta_path, "rt") as fp:
        meta = json.load(fp)

    return checkpoint_path, meta["config"], meta["metrics"]


#######################################################################
# Putting everything together
# ---------------------------
# Now we only need to glue this code together. This is the main
# entrypoint of the script, and we will define three methods:
#
# 1. Train new model from scratch with all data
# 2. Continue training from existing model with new data only
# 3. Query the model with test data
#
# Internally, this will just call the ``tune_from_scratch`` and
# ``tune_from_existing()`` functions.
# Both training functions will then call ``serve_new_model()`` to serve
# the newly trained or updated model.

# The query function will send a HTTP request to Serve with some
# test data obtained from the MNIST dataset.
if __name__ == "__main__":
    """
    This script offers training a new model from scratch with all
    available data, or continuing to train an existing model
    with newly available data.

    For instance, we might get new data every day. Every Sunday, we
    would like to train a new model from scratch.

    Naturally, we would like to use hyperparameter optimization to
    find the best model for out data.

    First, we might train a model with all data available at this day:

    .. code-block:: bash

        python tune-serve-integration-mnist.py --from_scratch --day 0

    On the coming days, we want to continue to train this model with
    newly available data:

    .. code-block:: bash

        python tune-serve-integration-mnist.py --from_existing --day 1
        python tune-serve-integration-mnist.py --from_existing --day 2
        python tune-serve-integration-mnist.py --from_existing --day 3
        python tune-serve-integration-mnist.py --from_existing --day 4
        python tune-serve-integration-mnist.py --from_existing --day 5
        python tune-serve-integration-mnist.py --from_existing --day 6
        # Retrain from scratch every 7th day:
        python tune-serve-integration-mnist.py --from_scratch --day 7

    We can also use this script to query our served model
    with some test data:

    .. code-block:: bash

        python tune-serve-integration-mnist.py --query 6
        Querying model with example #6. Label = 1, Response = 1, Correct = T
        python tune-serve-integration-mnist.py --query 28
        Querying model with example #28. Label = 2, Response = 7, Correct = F

    """
    parser = argparse.ArgumentParser(description="MNIST Tune/Serve example")
    parser.add_argument("--model_dir", type=str, default="~/mnist_tune_serve")

    parser.add_argument(
        "--from_scratch",
        action="store_true",
        help="Train and select best model from scratch",
        default=False,
    )

    parser.add_argument(
        "--from_existing",
        action="store_true",
        help="Train and select best model from existing model",
        default=False,
    )

    parser.add_argument(
        "--day",
        help="Indicate the day to simulate the amount of data available to us",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--query", help="Query endpoint with example", type=int, default=-1
    )

    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="Finish quickly for testing",
        default=False,
    )

    args = parser.parse_args()

    if args.smoke_test:
        ray.init(num_cpus=3, namespace="tune-serve-integration")
    else:
        ray.init(namespace="tune-serve-integration")

    model_dir = os.path.expanduser(args.model_dir)

    if args.query >= 0:
        import requests

        dataset = MNISTDataInterface("/tmp/mnist_data", max_days=0).dataset
        data = dataset[args.query]
        label = data[1]

        # Query our model
        response = requests.post(
            "http://localhost:8000/mnist", json={"images": [data[0].numpy().tolist()]}
        )

        try:
            pred = response.json()["result"][0]
        except:  # noqa: E722
            pred = -1

        print(
            "Querying model with example #{}. "
            "Label = {}, Response = {}, Correct = {}".format(
                args.query, label, pred, label == pred
            )
        )
        sys.exit(0)

    gpus_per_trial = 0.5 if not args.smoke_test else 0.0
    serve_gpu = True if gpus_per_trial > 0 else False
    num_samples = 8 if not args.smoke_test else 1
    num_epochs = 10 if not args.smoke_test else 1

    if args.from_scratch:  # train everyday from scratch
        print("Start training job from scratch on day {}.".format(args.day))
        acc, config, best_checkpoint, num_examples = tune_from_scratch(
            num_samples, num_epochs, gpus_per_trial, day=args.day
        )
        print(
            "Trained day {} from scratch on {} samples. "
            "Best accuracy: {:.4f}. Best config: {}".format(
                args.day, num_examples, acc, config
            )
        )
        serve_new_model(
            model_dir, best_checkpoint, config, acc, args.day, use_gpu=serve_gpu
        )

    if args.from_existing:
        old_checkpoint, old_config, old_acc = get_current_model(model_dir)
        if not old_checkpoint or not old_config or not old_acc:
            print("No existing model found. Train one with --from_scratch " "first.")
            sys.exit(1)
        acc, config, best_checkpoint, num_examples = tune_from_existing(
            old_checkpoint,
            old_config,
            num_samples,
            num_epochs,
            gpus_per_trial,
            day=args.day,
        )
        print(
            "Trained day {} from existing on {} samples. "
            "Best accuracy: {:.4f}. Best config: {}".format(
                args.day, num_examples, acc, config
            )
        )
        serve_new_model(
            model_dir, best_checkpoint, config, acc, args.day, use_gpu=serve_gpu
        )

#######################################################################
# That's it! We now have an end-to-end workflow to train and update a
# model every day with newly arrived data. Every week we might retrain
# the whole model. At every point in time we make sure to serve the
# model that achieved the best validation set accuracy.
#
# There are some ways we might extend this example. For instance, right
# now we only serve the latest trained model. We could  also choose to
# route only a certain percentage of users to the new model, maybe to
# see if the new model really does it's job right. These kind of
# deployments are called canary deployments.
# These kind of deployments would also require us to keep more than one
# model in our ``model_dir`` - which should be quite easy: We could just
# create subdirectories for each training day.
#
# Still, this example should show you how easy it is to integrate the
# Ray libraries Ray Tune and Ray Serve in your workflow. While both tools
# also work independently of each other, they complement each other
# nicely and support a large number of use cases.
