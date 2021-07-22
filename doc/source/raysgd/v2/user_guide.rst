.. _sgd-user-guide:

RaySGD User Guide
=================


Outline of use cases
--------------------

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



Porting code over to using RaySGD
---------------------------------

.. tabs::

    .. pytorch::

        TODO. Write about how to convert standard pytorch code to distributed.

    .. tensorflow::

        TODO. Write about how to convert standard pytorch code to distributed.

    .. horovod::

        TODO. Write about how to convert code to use horovod.



RaySGD Training on a large dataset
----------------------------------

SGD provides native support for Ray Datasets. You can pass in a Dataset to RaySGD via `Trainer.run`.
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


Monitoring training with intermediate results
---------------------------------------------

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

Here is a list of callbacks that is supported by RaySGD:

* WandbCallback
* MlflowCallback
* TensorboardCallback
* JsonCallback (Automatically logs given parameters)
* CSVCallback


.. note:: When using RayTune, these callbacks will not be used.

Running on the cloud
--------------------

TODO.

Implementing fault tolerance
----------------------------

TODO.


Hyperparameter tuning with RaySGD
---------------------------------




Distributed metrics
-------------------









A