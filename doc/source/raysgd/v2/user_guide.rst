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

.. TODO: make this runnable :)

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

We can simply construct the trainer function and specify the backend we want (torch, tensorflow, or horovod) and the number of workers to use for training:

.. code-block:: python

    from ray.util.sgd.v2 import Trainer

    trainer = Trainer(backend = "torch", num_workers=2)

Then, we can pass the function to the trainer. This will cause the trainer to start the necessary processes and execute the training function:

.. code-block:: python


    trainer.start()
    results = trainer.run(train_func)
    print(results)

Now, let's leverage Pytorch's Distributed Data Parallel. With RaySGD, you
just pass in your distributed data parallel code as as you would normally run
it with ``torch.distributed.launch``:

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


Backends
--------

RaySGD provides a thin API around different backend frameworks for
distributed deep learning. At the moment, RaySGD allows you to perform
training with:

* **PyTorch:** RaySGD initializes your distributed process group, allowing
  you to run your `DistributedDataParallel` training script. See `PyTorch
  Distributed Overview <https://torchmetrics.readthedocs.io/en/latest/>`_ for
  more information.
* **TensorFlow:**  RaySGD configures `TF_CONFIG` for you, allowing you to run
  your `MultiWorkerMirroredStrategy` training script. See `Distributed training
  with TensorFlow <https://www.tensorflow.org/guide/distributed_training>`_ for
  more information.
* **Horovod:** RaySGD configures the Horovod environment and Rendezvous
  server for you, allowing you to run your `DistributedOptimizer` training
  script. See `Horovod documentation <https://horovod.readthedocs.io/en/stable/index.html>`_
  for more information.


Porting code to RaySGD
----------------------

.. tabs::

    .. group-tab:: pytorch

        TODO. Write about how to convert standard pytorch code to distributed.

    .. group-tab:: tensorflow

        TODO. Write about how to convert standard tf code to distributed.

    .. group-tab:: horovod

        TODO. Write about how to convert code to use horovod.

.. TODO add BackendConfigs

.. To make existing code from the previous SGD API, see :ref:`Backwards Compatibility <sgd-backwards-compatibility>`.

Configurations
--------------

With RaySGD, you can execute a training function (``train_func``) in a
distributed manner by calling ``trainer.run(train_func)``. To pass arguments
into the training function, you can expose a single ``config`` parameter:

.. code-block:: diff

    -def train_func():
    +def train_func(config):

Then, you can pass in the config dictionary as an argument to ``Trainer.run``:

.. code-block:: diff

    -trainer.run(train_func)
    +config = {} # This should be populated.
    +trainer.run(train_func, config=config)

Putting this all together, you can run your training function with different
configurations. As an example:

.. code-block:: python

    from ray.util.sgd.v2 import Trainer

    def train_func(config):
        results = []
        for i in range(config["num_epochs"]):
            results.append(i)
        return results

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    print(trainer.run(train_func, config={"num_epochs": 2}))
    # [[0, 1], [0, 1]]
    print(trainer.run(train_func, config={"num_epochs": 5}))
    # [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]]
    trainer.shutdown()

A primary use-case for ``config`` is to try different hyperparameters. To
perform hyperparameter tuning with RaySGD, please refer to the
:ref:`Ray Tune integration <tune-sgd>`.

.. TODO add support for with_parameters

Log Directory Structure
-----------------------

Each ``Trainer`` will have a local directory created for logs, and each call
to ``Trainer.run`` will create its own sub-directory of logs.

By default, the ``logdir`` will be created at
``~/ray_results/sgd_<datestring>``.
This can be overridden in the ``Trainer`` constructor to an absolute path or
a path relative to ``~/ray_results``.

Log directories are exposed through the following attributes:

+------------------------+---------------------------------------------------+
| Attribute              | Example                                           |
+========================+===================================================+
| trainer.logdir         | /home/ray_results/sgd_2021-09-01_12-00-00         |
+------------------------+---------------------------------------------------+
| trainer.latest_run_dir | /home/ray_results/sgd_2021-09-01_12-00-00/run_001 |
+------------------------+---------------------------------------------------+

Logs will be written by:

1. :ref:`Logging Callbacks <sgd-logging-callbacks>`
2. :ref:`Checkpoints <sgd-checkpointing>`

.. TODO link to Training Run Iterator API as a 3rd option for logging.

.. _sgd-logging:

Logging, Monitoring, and Callbacks
----------------------------------

Reporting intermediate results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RaySGD provides an ``sgd.report(**kwargs)`` API for reporting intermediate
results from the training function up to the ``Trainer``.

Using ``Trainer.run``, these results can be processed  through :ref:`Callbacks
<sgd-callbacks>` with a ``handle_result`` method defined.

For custom handling, the lower-level ``Trainer.run_iterator`` API produces an
``SGDIterator`` which will iterate over the reported results.

The primary use-case for reporting is for metrics (accuracy, loss, etc.)

Distributed metrics (for Pytorch)
+++++++++++++++++++++++++++++++++

In real applications, you may want to calculate optimization metrics besides
accuracy and loss: recall, precision, Fbeta, etc.

RaySGD natively supports `TorchMetrics <https://torchmetrics.readthedocs.io/en/latest/>`_, which provides a collection of machine learning metrics for distributed, scalable Pytorch models.

Here is an example:

.. code-block:: python

    from ray.util.sgd import v2 as sgd
    from ray.util.sgd.v2 import SGDCallback, Trainer
    from typing import List, Dict

    import torch
    import torchmetrics

    class PrintingCallback(SGDCallback):
        def handle_result(self, results: List[Dict], **info):
            print(results)

    def train_func(config):
        preds = torch.randn(10, 5).softmax(dim=-1)
        target = torch.randint(5, (10,))
        accuracy = torchmetrics.functional.accuracy(preds, target).item()
        sgd.report(accuracy=accuracy)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    result = trainer.run(
        train_func,
        callbacks=[PrintingCallback()]
    )
    # [{'accuracy': 0.20000000298023224, '_timestamp': 1630716913, '_time_this_iter_s': 0.0039408206939697266, '_training_iteration': 1},
    #  {'accuracy': 0.10000000149011612, '_timestamp': 1630716913, '_time_this_iter_s': 0.0030548572540283203, '_training_iteration': 1}]
    trainer.shutdown()

Autofilled metrics
++++++++++++++++++

In addition to user defined metrics, a few fields are automatically populated:

* ``_timestamp``
* ``_time_this_iter_s``
* ``_training_iteration``

For debugging purposes, a more extensive set of metrics can be included in
any run by setting the ``SGD_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS`` environment
variable to ``1``.

* ``_date``
* ``_hostname``
* ``_node_ip``
* ``_pid``
* ``_time_total_s``

.. _sgd-callbacks:

Callbacks
~~~~~~~~~

You may want to plug in your training code with your favorite experiment management framework.
RaySGD provides an interface to fetch intermediate results and callbacks to process/log your intermediate results.

You can plug all of these into RaySGD with the following interface:

.. code-block:: python

    from ray.util.sgd import v2 as sgd
    from ray.util.sgd.v2 import SGDCallback, Trainer
    from typing import List, Dict

    class PrintingCallback(SGDCallback):
        def handle_result(self, results: List[Dict], **info):
            print(results)

    def train_func():
        for i in range(3):
            sgd.report(epoch=i)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    result = trainer.run(
        train_func,
        callbacks=[PrintingCallback()]
    )
    # [{'epoch': 0, '_timestamp': 1630471763, '_time_this_iter_s': 0.0020279884338378906, '_training_iteration': 1}, {'epoch': 0, '_timestamp': 1630471763, '_time_this_iter_s': 0.0014922618865966797, '_training_iteration': 1}]
    # [{'epoch': 1, '_timestamp': 1630471763, '_time_this_iter_s': 0.0008401870727539062, '_training_iteration': 2}, {'epoch': 1, '_timestamp': 1630471763, '_time_this_iter_s': 0.0007486343383789062, '_training_iteration': 2}]
    # [{'epoch': 2, '_timestamp': 1630471763, '_time_this_iter_s': 0.0014500617980957031, '_training_iteration': 3}, {'epoch': 2, '_timestamp': 1630471763, '_time_this_iter_s': 0.0015292167663574219, '_training_iteration': 3}]
    trainer.shutdown()

.. Here is a list of callbacks that are supported by RaySGD:

.. * JsonLoggerCallback
.. * TBXLoggerCallback
.. * WandbCallback
.. * MlflowCallback
.. * CSVCallback


.. note:: When using RayTune, these callbacks will not be used.

.. _sgd-logging-callbacks:

Logging Callbacks
+++++++++++++++++

The following ``SGDCallback``\s are available and will write to a file within the
:ref:`log directory <sgd-logging>` of each training run.

1. ``JsonLoggerCallback``
2. ``TBXLoggerCallback``

Custom Callbacks
++++++++++++++++

If the provided callbacks do not cover your desired integrations or use-cases,
you may always implement a custom callback by subclassing ``SGDCallback``. If
the callback is general enough, please feel welcome to add it to the ``ray``
repository.

A simple example for creating a callback that will print out results:

.. code-block:: python

    from ray.util.sgd.v2 import SGDCallback

    class PrintingCallback(SGDCallback):
        def handle_result(self, results: List[Dict], **info):
            print(results)


..
    Advanced Customization
    ~~~~~~~~~~~~~~~~~~~~~~

    TODO add link to Run Iterator API and describe how to use it specifically
    for custom integrations.

.. _sgd-checkpointing:

Checkpointing
-------------

RaySGD provides a way to save state during the training process. This will be useful for:

1. :ref:`Integration with Ray Tune <tune-sgd>` to use certain Ray Tune schedulers
2. Running a long-running training job on a cluster of pre-emptible machines/pods.


Saving checkpoints
~~~~~~~~~~~~~~~~~~

Checkpoints can be saved by calling ``sgd.save_checkpoint(**kwargs)`` in the
training function. This will propagate the data from only the rank 0 worker.

The latest saved checkpoint can be accessed through the ``Trainer``'s
``latest_checkpoint`` attribute.

.. code-block:: python

    from ray.util.sgd import v2 as sgd
    from ray.util.sgd.v2 import Trainer

    def train_func(config):
        model = 0 # This should be replaced with a real model.
        for epoch in range(config["num_epochs"]):
            model += epoch
            sgd.save_checkpoint(epoch=epoch, model=model)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, config={"num_epochs": 5})
    trainer.shutdown()

    print(trainer.latest_checkpoint)
    # {'epoch': 4, 'model': 10}

By default, checkpoints will be persisted to local disk in the :ref:`log
directory <sgd-logging>` of each run.

.. code-block:: python

    print(trainer.latest_checkpoint_dir)
    # /home/ray_results/sgd_2021-09-01_12-00-00/run_001/checkpoints
    print(trainer.latest_checkpoint_path)
    # /home/ray_results/sgd_2021-09-01_12-00-00/run_001/checkpoints/checkpoint_000005


.. note:: Persisting checkpoints to durable storage (e.g. S3) is not yet supported.

Configuring checkpoints
+++++++++++++++++++++++

For more configurability of checkpointing behavior (specifically saving
checkpoints to disk), a ``CheckpointStrategy`` can be passed into
``Trainer.run``.

.. note:: Currently ``CheckpointStrategy`` only enables or disables disk
   persistence altogether. Additional functionality coming soon!


Loading checkpoints
~~~~~~~~~~~~~~~~~~~

Checkpoints can be loaded into the training function in 2 steps:

1. From the training function, ``sgd.load_checkpoint()`` can be used to access
``trainer.latest_checkpoint``.
2. ``trainer.latest_checkpoint`` can be bootstrapped by passing in the
``checkpoint`` argument of ``trainer.run()``.

.. code-block:: python

    from ray.util.sgd import v2 as sgd
    from ray.util.sgd.v2 import Trainer

    def train_func(config):
        checkpoint = sgd.load_checkpoint() or {}
        # This should be replaced with a real model.
        model = checkpoint.get("model", 0)
        start_epoch = checkpoint.get("epoch", -1) + 1
        for epoch in range(start_epoch, config["num_epochs"]):
            model += epoch
            sgd.save_checkpoint(epoch=epoch, model=model)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, config={"num_epochs": 5},
                checkpoint={"epoch": 2, "model": 3})
    trainer.shutdown()

    print(trainer.latest_checkpoint)
    # {'epoch': 4, 'model': 10}


Checkpoints can be loaded to support resuming from a previously saved
checkpoint and providing fault tolerance.

.. Running on the cloud
.. --------------------

.. Use RaySGD with the Ray cluster launcher by changing the following:

.. .. code-block:: bash

..     ray up cluster.yaml

.. TODO.

Fault Tolerance & Elastic Training
----------------------------------

RaySGD has built-in fault tolerance to recover from worker failures (i.e.
``RayActorError``\s). When a failure is detected, the workers will be shut
down and new workers will be added in. The training function will be
restarted, but progress from the previous execution can be resumed through
checkpointing.

.. note:: In order to retain progress when recovery, your training function
   **must** implement logic for both saving *and* loading :ref:`checkpoints
   <sgd-checkpointing>`.

The number of retries is configurable through the ``max_retries`` argument of
the ``Trainer`` constructor.

.. note:: Elastic Training is not yet supported.

.. Running on pre-emptible machines
.. --------------------------------

.. You may want to

.. TODO.


Training on a large dataset
---------------------------

.. note:: This feature is coming soon!

SGD provides native support for :ref:`Ray Datasets <datasets>`. You can pass in a Dataset to RaySGD via ``Trainer.run``\.
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


.. _tune-sgd:

Hyperparameter tuning (Ray Tune)
--------------------------------

Hyperparameter tuning with Ray Tune is natively supported with RaySGD. Specifically, you can take an existing training function and follow these steps:

1. Call ``trainer.to_tune_trainable``, which will produce an object ("Trainable") that will be passed to Ray Tune.
2. Call ``tune.run(trainable)`` instead of ``trainer.run``. This will invoke the hyperparameter tuning, starting multiple "trials" each with the resource amount specified by the Trainer.

A couple caveats:

* Tune will ignore the return value of ``training_func``. To save your best
  trained model, you will need to use the ``sgd.save_checkpoint`` API.
* You should **not** call ``tune.report`` or ``tune.checkpoint_dir`` in your
  training function. Functional parity is achieved through ``sgd.report``,
  ``sgd.save_checkpoint``, and ``sgd.load_checkpoint``. This allows you to go
  from RaySGD to RaySGD+RayTune without changing any code in the training
  function.


.. code-block:: python

    from ray import tune
    from ray.util.sgd import v2 as sgd
    from ray.util.sgd.v2 import Trainer

    def training_func(config):
        # In this example, nothing is expected to change over epochs,
        # and the output metric is equivalent to the input value.
        for _ in range(config["num_epochs"]):
            sgd.report(output=config["input"])

    trainer = Trainer(backend="torch", num_workers=2)
    trainable = trainer.to_tune_trainable(training_func)
    analysis = tune.run(trainable, config={
        "num_epochs": 2,
        "input": tune.grid_search([1, 2, 3])
    })
    print(analysis.get_best_config(metric="output", mode="max"))
    # {'num_epochs': 2, 'input': 3}

.. note:: RaySGD+RayTune+RayDatasets integration is not yet supported.

..
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
..
    Advanced APIs
    -------------

    TODO

    Training Run Iterator API
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    TODO

    Stateful Class API
    ~~~~~~~~~~~~~~~~~~

    TODO

.. _sgd-backwards-compatibility:

..
    Backwards Compatibility
    -------------

    TODO
