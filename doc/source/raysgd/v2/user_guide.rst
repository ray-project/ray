:orphan:

.. _sgd-user-guide:

RaySGD User Guide
=================

In this guide, we cover examples for the following use cases:

* How do I port my code to using RaySGD?
* How do I use RaySGD to train with a large dataset?
* How do I tune my RaySGD model?
* How do I run my training on pre-emptible instances (fault tolerance)?
* How do I monitor my training?



Quick Start
-----------

RaySGD abstracts away the complexity of setting up a distributed training system. Let's take this simple example function:

.. code-block:: python

    class Net(nn.Module):
        def __init__(self):
            super(Net, self).__init__()
            self.fc1 = nn.Linear(1, 128)
            self.fc2 = nn.Linear(128, 1)

        def forward(self, x):
            x = self.fc1(x)
            x = F.relu(x)
            x = self.fc2(x)
            return x

    def train_func():
        model = Net()
        for x in data:
            results = model(x)
        return results

To convert this to RaySGD, we add a `config` parameter to `train_func()`:

.. code-block:: diff

    -def train_func():
    +def train_func(config):

Then, we can construct the trainer function:

.. code-block:: python

    from ray.util.sgd import Trainer

    trainer = Trainer(num_workers=2)

Then, we can pass the function to the trainer. This will cause the trainer to start the necessary processes and execute the training function:

.. code-block:: python

    results = trainer.run(train_func, config=None)
    print(results)

Now, let's leverage Pytorch's Distributed Data Parallel. With RaySGD, you just pass in your distributed data parallel code as as you would normally run it with `torch.distributed.launch`:

.. code-block:: python

    import torch.nn as nn
    from torch.nn.parallel import DistributedDataParallel
    import torch.optim as optim

    def train_simple(config: Dict):

        # N is batch size; D_in is input dimension;
        # H is hidden dimension; D_out is output dimension.
        N, D_in, H, D_out = 8, 5, 5, 5

        # Create random Tensors to hold inputs and outputs
        x = torch.randn(N, D_in)
        y = torch.randn(N, D_out)
        loss_fn = nn.MSELoss()

        # Use the nn package to define our model and loss function.
        model = torch.nn.Sequential(
            torch.nn.Linear(D_in, H),
            torch.nn.ReLU(),
            torch.nn.Linear(H, D_out),
        )
        optimizer = optim.SGD(model.parameters(), lr=0.1)

        model = DistributedDataParallel(model)
        results = []

        for epoch in range(config.get("epochs", 10)):
            optimizer.zero_grad()
            output = model(x)
            loss = loss_fn(output, y)
            loss.backward()
            results.append(loss.item())
            optimizer.step()
        return results

Running this with RaySGD is as simple as the following:

.. code-block:: python

    all_results = trainer.run(train_simple)



Porting code to RaySGD
----------------------

.. tabs::

    .. group-tab:: pytorch

        TODO. Write about how to convert standard pytorch code to distributed.

    .. group-tab:: tensorflow

        TODO. Write about how to convert standard tf code to distributed.

    .. group-tab:: horovod

        TODO. Write about how to convert code to use horovod.



Training on a large dataset
---------------------------

SGD provides native support for :ref:`Ray Datasets <datasets>`. You can pass in a Dataset to RaySGD via ``Trainer.run``.
Underneath the hood, RaySGD will automatically shard the given dataset.


.. code-block:: python

    def train_func(config):
        batch_size = config["worker_batch_size"]
        data_shard = ray.sgd.get_data_shard()
        dataloader = data_shard.to_torch(batch_size=batch_size)

        for x, y in dataloader:
            output = model(x)
            ...

        return model

    trainer = Trainer(num_workers=8, backend="torch")
    dataset = ray.data.read_csv("...").filter().pipeline(length=50)

    result = trainer.run(
        train_func,
        config={"worker_batch_size": 64},
        dataset=dataset)


.. note:: This feature currently does not work with elastic training.


Monitoring training
-------------------

You may want to plug in your training code with your favorite experiment management framework.
RaySGD provides an interface to fetch intermediate results and callbacks to process/log your intermediate results.

You can plug all of these into RaySGD with the following interface:

.. code-block:: python

    def train_func(config):
        # do something
        for x, y in dataset:
            result = process(x)
            ray.sgd.report(**result)


    # TODO: Where do we pass in the logging folder?
    result = trainer.run(
        train_func,
        config={"worker_batch_size": 64},
        callbacks=[sgd.MlflowCallback()]
        dataset=dataset)

.. Here is a list of callbacks that is supported by RaySGD:

.. * WandbCallback
.. * MlflowCallback
.. * TensorboardCallback
.. * JsonCallback (Automatically logs given parameters)
.. * CSVCallback


.. note:: When using RayTune, these callbacks will not be used.

Checkpointing
-------------

RaySGD provides a way to save state during the training process. This will be useful for:

1. :ref:`Integration with Ray Tune <tune-sgd>` to use certain Ray Tune schedulers
2. Running a long-running training job on a cluster of pre-emptible machines/pods.


.. code-block:: python

    import ray

    def train_func(config):

        state = ray.sgd.load_checkpoint()
        # eventually, optional:
        for _ in config["num_epochs"]:
            train(...)
            ray.sgd.save_checkpoint((model, optimizer, etc))
        return model

    trainer = Trainer(backend="torch", num_workers=4)
    trainer.run(train_func)
    state = trainer.get_last_checkpoint()

.. Running on the cloud
.. --------------------

.. Use RaySGD with the Ray cluster launcher by changing the following:

.. .. code-block:: bash

..     ray up cluster.yaml

.. TODO.



.. Running on pre-emptible machines
.. --------------------------------

.. You may want to

.. TODO.


.. _tune-sgd:

Hyperparameter tuning
---------------------

Hyperparameter tuning with Ray Tune is natively supported with RaySGD. Specifically, you can take an existing training function and follow these steps:

1. Call ``trainer.to_tune_trainable``, which will produce an object ("Trainable") that will be passed to Ray Tune.
2. Call ``tune.run(trainable)`` instead of ``trainer.run``. This will invoke the hyperparameter tuning, starting multiple "trials" each with the resource amount specified by the Trainer.

A couple caveats:

* Tune won't handle the ``training_func`` return value correctly. To save your best trained model, you'll need to use the checkpointing API.
* You should **not** call ``tune.report`` or ``tune.checkpoint_dir`` in your training function.

.. code-block:: python

    import ray
    from ray import tune

    def training_func(config):
        dataloader = ray.sgd.get_dataset()\
            .get_shard(torch.rank())\
            .to_torch(batch_size=config["batch_size"])

        for i in config["epochs"]:
            ray.sgd.report(...)  # use same intermediate reporting API

    # Declare the specification for training.
    trainer = Trainer(backend="torch", num_workers=12, use_gpu=True)
    dataset = ray.dataset.pipeline()

    # Convert this to a trainable.
    trainable = trainer.to_tune_trainable(training_func, dataset=dataset)

    analysis = tune.run(trainable, config={
        "lr": tune.uniform(), "batch_size": tune.randint(1, 2, 3)}, num_samples=12)


Distributed metrics (for Pytorch)
---------------------------------

In real applications, you may want to calcluate optimization metrics besides accuracy and loss: recall, precision, Fbeta, etc.

RaySGD natively supports `TorchMetrics <https://torchmetrics.readthedocs.io/en/latest/>`_, which provides a collection of machine learning metrics for distributed, scalable Pytorch models.

Here is an example:

.. code-block:: python

    import torch
    import torchmetrics
    import ray

    def train_func(config):
        preds = torch.randn(10, 5).softmax(dim=-1)
        target = torch.randint(5, (10,))

        acc = torchmetrics.functional.accuracy(preds, target)
        ray.sgd.report(accuracy=acc)

    trainer = Trainer(num_workers=2)
    trainer.run(train_func, config=None)
