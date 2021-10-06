.. _sgd-user-guide:

RaySGD User Guide
=================

.. tip:: Get in touch with us if you're using or considering using `RaySGD <https://forms.gle/PXFcJmHwszCwQhqX7>`_!

In this guide, we cover examples for the following use cases:

* How do I :ref:`port my code <sgd-porting-code>` to using RaySGD?
* How do I :ref:`monitor <sgd-monitoring>` my training?
* How do I run my training on pre-emptible instances
  (:ref:`fault tolerance <sgd-fault-tolerance>`)?
* How do I use RaySGD to :ref:`train with a large dataset <sgd-datasets>`?
* How do I :ref:`tune <sgd-tune>` my RaySGD model?

.. _sgd-backends:

Backends
--------

RaySGD provides a thin API around different backend frameworks for
distributed deep learning. At the moment, RaySGD allows you to perform
training with:

* **PyTorch:** RaySGD initializes your distributed process group, allowing
  you to run your ``DistributedDataParallel`` training script. See `PyTorch
  Distributed Overview <https://pytorch.org/tutorials/beginner/dist_overview.html>`_
  for more information.
* **TensorFlow:**  RaySGD configures ``TF_CONFIG`` for you, allowing you to run
  your ``MultiWorkerMirroredStrategy`` training script. See `Distributed
  training with TensorFlow <https://www.tensorflow.org/guide/distributed_training>`_
  for more information.
* **Horovod:** RaySGD configures the Horovod environment and Rendezvous
  server for you, allowing you to run your ``DistributedOptimizer`` training
  script. See `Horovod documentation <https://horovod.readthedocs.io/en/stable/index.html>`_
  for more information.

.. _sgd-porting-code:

Porting code to RaySGD
----------------------

The following instructions assume you have a training function
that can already be run on a single worker for one of the supported
:ref:`backend <sgd-backends>` frameworks.

Update training function
~~~~~~~~~~~~~~~~~~~~~~~~

First, you'll want to update your training function to support distributed
training.

.. tabs::

  .. group-tab:: PyTorch

    RaySGD will set up your distributed process group for you. You simply
    need to add in the proper PyTorch hooks in your training function to
    utilize it.

    **Step 1:** Wrap your model in ``DistributedDataParallel``.

    The `DistributedDataParallel <https://pytorch.org/docs/master/generated/torch.nn.parallel.DistributedDataParallel.html>`_
    container will parallelize the input ``Module`` across the worker processes.

    .. code-block:: python

        from torch.nn.parallel import DistributedDataParallel

        model = DistributedDataParallel(model)

    **Step 2:** Update your ``DataLoader`` to use a ``DistributedSampler``.

    The `DistributedSampler <https://pytorch.org/docs/master/data.html#torch.utils.data.distributed.DistributedSampler>`_
    will split the data across the workers, so each process will train on
    only a subset of the data.

    .. code-block:: python

        from torch.utils.data import DataLoader, DistributedSampler

        data_loader = DataLoader(dataset,
                                 batch_size=batch_size,
                                 sampler=DistributedSampler(dataset))


    **Step 3:** Set the proper CUDA device if you are using GPUs.

    If you are using GPUs, you need to make sure to the CUDA devices are properly setup inside your training function.

    This involves 3 steps:

    1. Use the local rank to set the default CUDA device for the worker.
    2. Move the model to the default CUDA device (or a specific CUDA device).
    3. Specify ``device_ids`` when wrapping in ``DistributedDataParallel``.

    .. code-block:: python

        def train_func():
            device = torch.device(f"cuda:{sgd.local_rank()}" if
                          torch.cuda.is_available() else "cpu")
            torch.cuda.set_device(device)

            # Create model.
            model = NeuralNetwork()
            model = model.to(device)
            model = DistributedDataParallel(
                model,
                device_ids=[sgd.local_rank()] if torch.cuda.is_available() else None)


  .. group-tab:: TensorFlow

    .. note::
       The current TensorFlow implementation supports
       ``MultiWorkerMirroredStrategy`` (and ``MirroredStrategy``). If there are
       other strategies you wish to see supported by RaySGD, please let us know
       by submitting a `feature request on GitHub`_.

    These instructions closely follow TensorFlow's `Multi-worker training
    with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_
    tutorial. One key difference is that RaySGD will handle the environment
    variable set up for you.

    **Step 1:** Wrap your model in ``MultiWorkerMirroredStrategy``.

    The `MultiWorkerMirroredStrategy <https://www.tensorflow.org/api_docs/python/tf/distribute/experimental/MultiWorkerMirroredStrategy>`_
    enables synchronous distributed training. The ``Model`` *must* be built and
    compiled within the scope of the strategy.

    .. code-block:: python

        with tf.distribute.MultiWorkerMirroredStrategy().scope():
            model = ... # build model
            model.compile()

    **Step 2:** Update your ``Dataset`` batch size to the *global* batch
    size.

    The `batch <https://www.tensorflow.org/api_docs/python/tf/data/Dataset#batch>`_
    will be split evenly across worker processes, so ``batch_size`` should be
    set appropriately.

    .. code-block:: diff

        -batch_size = worker_batch_size
        +batch_size = worker_batch_size * num_workers

  .. group-tab:: Horovod

    If you have a training function that already runs with the `Horovod Ray
    Executor <https://horovod.readthedocs.io/en/stable/ray_include.html#horovod-ray-executor>`_,
    you should not need to make any additional changes!

    To onboard onto Horovod, please visit the `Horovod guide
    <https://horovod.readthedocs.io/en/stable/index.html#get-started>`_.

Create RaySGD Trainer
~~~~~~~~~~~~~~~~~~~~~

The ``Trainer`` is the primary RaySGD class that is used to manage state and
execute training. You can create a simple ``Trainer`` for the backend of choice
with one of the following:

.. code-block:: python

    torch_trainer = Trainer(backend="torch", num_workers=2)

    tensorflow_trainer = Trainer(backend="tensorflow", num_workers=2)

    horovod_trainer = Trainer(backend="horovod", num_workers=2)

For more configurability, please reference the :ref:`sgd-api-trainer` API.
To customize the ``backend`` setup, you can replace the string argument with a
:ref:`sgd-api-backend-config` object.

Run training function
~~~~~~~~~~~~~~~~~~~~~

With a distributed training function and a RaySGD ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.start() # set up resources
    trainer.run(train_func)
    trainer.shutdown() # clean up resources

.. To make existing code from the previous SGD API, see :ref:`Backwards Compatibility <sgd-backwards-compatibility>`.

.. _`feature request on GitHub`: https://github.com/ray-project/ray/issues

Configuring Training
--------------------

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

    from ray.sgd import Trainer

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
:ref:`Ray Tune integration <sgd-tune>`.

.. TODO add support for with_parameters


.. _sgd-log-dir:

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

.. _sgd-monitoring:

Logging, Monitoring, and Callbacks
----------------------------------

Reporting intermediate results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RaySGD provides an ``sgd.report(**kwargs)`` API for reporting intermediate
results from the training function up to the ``Trainer``.

Using ``Trainer.run``, these results can be processed through :ref:`Callbacks
<sgd-callbacks>` with a ``handle_result`` method defined.

For custom handling, the lower-level ``Trainer.run_iterator`` API produces an
:ref:`sgd-api-iterator` which will iterate over the reported results.

The primary use-case for reporting is for metrics (accuracy, loss, etc.).

.. code-block:: python

    def train_func():
        ...
        for i in range(num_epochs):
            results = model.train(...)
            sgd.report(results)
        return model

Autofilled metrics
++++++++++++++++++

In addition to user defined metrics, a few fields are automatically populated:

.. code-block:: python

    # Unix epoch time in seconds when the data is reported.
    _timestamp
    # Time in seconds between iterations.
    _time_this_iter_s
    # The iteration ID, where each iteration is defined by one call to sgd.report().
    # This is a 1-indexed incrementing integer ID.
    _training_iteration

For debugging purposes, a more extensive set of metrics can be included in
any run by setting the ``SGD_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS`` environment
variable to ``1``.


.. code-block:: python

    # The local date string when the data is reported.
    _date
    # The worker hostname (platform.node()).
    _hostname
    # The worker IP address.
    _node_ip
    # The worker process ID (os.getpid()).
    _pid
    # The cumulative training time of all iterations so far.
    _time_total_s


.. _sgd-callbacks:

Callbacks
~~~~~~~~~

You may want to plug in your training code with your favorite experiment management framework.
RaySGD provides an interface to fetch intermediate results and callbacks to process/log your intermediate results.

You can plug all of these into RaySGD with the following interface:

.. code-block:: python

    from ray import sgd
    from ray.sgd Trainer
    from ray.sgd.callbacks import SGDCallback
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

.. _sgd-logging-callbacks:

Logging Callbacks
+++++++++++++++++

The following ``SGDCallback``\s are available and will write to a file within the
:ref:`log directory <sgd-log-dir>` of each training run.

1. :ref:`sgd-api-json-logger-callback`
2. :ref:`sgd-api-tbx-logger-callback`

Custom Callbacks
++++++++++++++++

If the provided callbacks do not cover your desired integrations or use-cases,
you may always implement a custom callback by subclassing ``SGDCallback``. If
the callback is general enough, please feel welcome to `add it <https://docs
.ray.io/en/master/getting-involved.html>`_ to the ``ray``
`repository <https://github.com/ray-project/ray>`_.

A simple example for creating a callback that will print out results:

.. code-block:: python

    from ray.sgd.callbacks import SGDCallback

    class PrintingCallback(SGDCallback):
        def handle_result(self, results: List[Dict], **info):
            print(results)


..
    Advanced Customization
    ~~~~~~~~~~~~~~~~~~~~~~

    TODO add link to Run Iterator API and describe how to use it specifically
    for custom integrations.

Example: PyTorch Distributed metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


In real applications, you may want to calculate optimization metrics besides
accuracy and loss: recall, precision, Fbeta, etc.

RaySGD natively supports `TorchMetrics <https://torchmetrics.readthedocs.io/en/latest/>`_, which provides a collection of machine learning metrics for distributed, scalable Pytorch models.

Here is an example:

.. code-block:: python

    from ray import sgd
    from ray.sgd import SGDCallback, Trainer
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

.. _sgd-checkpointing:

Checkpointing
-------------

RaySGD provides a way to save state during the training process. This is
useful for:

1. :ref:`Integration with Ray Tune <sgd-tune>` to use certain Ray Tune
   schedulers.
2. Running a long-running training job on a cluster of pre-emptible machines/pods.
3. Persisting trained model state to later use for serving/inference.
4. In general, storing any model artifacts.

Saving checkpoints
~~~~~~~~~~~~~~~~~~

Checkpoints can be saved by calling ``sgd.save_checkpoint(**kwargs)`` in the
training function.

.. note:: This must be called by all workers, but only data from the rank 0
          worker will be saved by the ``Trainer``.

The latest saved checkpoint can be accessed through the ``Trainer``'s
``latest_checkpoint`` attribute.

.. code-block:: python

    from ray import sgd
    from ray.sgd import Trainer

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
directory <sgd-log-dir>` of each run.

.. code-block:: python

    print(trainer.latest_checkpoint_dir)
    # /home/ray_results/sgd_2021-09-01_12-00-00/run_001/checkpoints
    print(trainer.latest_checkpoint_path)
    # /home/ray_results/sgd_2021-09-01_12-00-00/run_001/checkpoints/checkpoint_000005


.. note:: Persisting checkpoints to durable storage (e.g. S3) is not yet supported.

Configuring checkpoints
+++++++++++++++++++++++

For more configurability of checkpointing behavior (specifically saving
checkpoints to disk), a :ref:`sgd-api-checkpoint-strategy` can be passed into
``Trainer.run``.

As an example, to disable writing checkpoints to disk:

.. code-block:: python
    :emphasize-lines: 8,12

    from ray import sgd
    from ray.sgd import CheckpointStrategy, Trainer

    def train_func():
        for epoch in range(3):
            sgd.save_checkpoint(epoch=epoch)

    checkpoint_strategy = CheckpointStrategy(num_to_keep=0)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, checkpoint_strategy=checkpoint_strategy)
    trainer.shutdown()

.. note:: Currently ``CheckpointStrategy`` only enables or disables disk
   persistence altogether. Additional functionality coming soon!


Loading checkpoints
~~~~~~~~~~~~~~~~~~~

Checkpoints can be loaded into the training function in 2 steps:

1. From the training function, ``sgd.load_checkpoint()`` can be used to access
   the most recently saved checkpoint. This is useful to continue training even
   if there's a worker failure.
2. The checkpoint to start training with can be bootstrapped by passing in a
   ``checkpoint`` to ``trainer.run()``.

.. code-block:: python

    from ray import sgd
    from ray.sgd import Trainer

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

.. Running on the cloud
.. --------------------

.. Use RaySGD with the Ray cluster launcher by changing the following:

.. .. code-block:: bash

..     ray up cluster.yaml

.. TODO.

.. _sgd-fault-tolerance:

Fault Tolerance & Elastic Training
----------------------------------

RaySGD has built-in fault tolerance to recover from worker failures (i.e.
``RayActorError``\s). When a failure is detected, the workers will be shut
down and new workers will be added in. The training function will be
restarted, but progress from the previous execution can be resumed through
checkpointing.

.. warning:: In order to retain progress when recovery, your training function
   **must** implement logic for both saving *and* loading :ref:`checkpoints
   <sgd-checkpointing>`.

Each instance of recovery from a worker failure is considered a retry. The
number of retries is configurable through the ``max_retries`` argument of the
``Trainer`` constructor.

.. note:: Elastic Training is not yet supported.

.. Running on pre-emptible machines
.. --------------------------------

.. You may want to

.. TODO.

.. _sgd-datasets:

Training on a large dataset (Ray Datasets)
------------------------------------------

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
    dataset = ray.data.read_csv("...").filter().window(blocks_per_window=50)

    result = trainer.run(
        train_func,
        config={"worker_batch_size": 64},
        dataset=dataset)


.. note:: This feature currently does not work with elastic training.


.. _sgd-tune:

Hyperparameter tuning (Ray Tune)
--------------------------------

Hyperparameter tuning with :ref:`Ray Tune <tune-main>` is natively supported
with RaySGD. Specifically, you can take an existing training function and
follow these steps:

**Step 1: Convert to Tune Trainable**

Instantiate your Trainer and call ``trainer.to_tune_trainable``, which will
produce an object ("Trainable") that will be passed to Ray Tune.

.. code-block:: python

    from ray import sgd
    from ray.sgd import Trainer

    def train_func(config):
        # In this example, nothing is expected to change over epochs,
        # and the output metric is equivalent to the input value.
        for _ in range(config["num_epochs"]):
            sgd.report(output=config["input"])

    trainer = Trainer(backend="torch", num_workers=2)
    trainable = trainer.to_tune_trainable(train_func)

**Step 2: Call tune.run**

Call ``tune.run`` on the created ``Trainable`` to start multiple ``Tune``
"trials", each running a RaySGD job and each with a unique hyperparameter
configuration.

.. code-block:: python

    from ray import tune
    analysis = tune.run(trainable, config={
        "num_epochs": 2,
        "input": tune.grid_search([1, 2, 3])
    })
    print(analysis.get_best_config(metric="output", mode="max"))
    # {'num_epochs': 2, 'input': 3}

A couple caveats:

* Tune will ignore the return value of ``train_func``. To save your best
  trained model, you will need to use the ``sgd.save_checkpoint`` API.
* You should **not** call ``tune.report`` or ``tune.checkpoint_dir`` in your
  training function. Functional parity is achieved through ``sgd.report``,
  ``sgd.save_checkpoint``, and ``sgd.load_checkpoint``. This allows you to go
  from RaySGD to RaySGD+RayTune without changing any code in the training
  function.


.. code-block:: python

    from ray import tune
    from ray import sgd
    from ray.sgd import Trainer

    def train_func(config):
        # In this example, nothing is expected to change over epochs,
        # and the output metric is equivalent to the input value.
        for _ in range(config["num_epochs"]):
            sgd.report(output=config["input"])

    trainer = Trainer(backend="torch", num_workers=2)
    trainable = trainer.to_tune_trainable(train_func)
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
    dataset = ray.dataset.window()

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
