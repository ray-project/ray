.. _train-user-guide:

Ray Train User Guide
====================

.. tip:: Get in touch with us if you're using or considering using `Ray Train <https://forms.gle/PXFcJmHwszCwQhqX7>`_!

Ray Train provides solutions for training machine learning models in a distributed manner on Ray.
As of Ray 1.8, support for Deep Learning is available in ``ray.train`` (formerly :ref:`Ray SGD <sgd-index>`).
For other model types, distributed training support is available through other libraries:

* **Reinforcement Learning:** :ref:`rllib-index`
* **XGBoost:** :ref:`xgboost-ray`
* **LightGBM:** :ref:`lightgbm-ray`
* **Pytorch Lightning:** :ref:`ray-lightning`

In this guide, we cover examples for the following use cases:

* How do I :ref:`port my code <train-porting-code>` to using Ray Train?
* How do I :ref:`monitor <train-monitoring>` my training?
* How do I run my training on pre-emptible instances
  (:ref:`fault tolerance <train-fault-tolerance>`)?
* How do I use Ray Train to :ref:`train with a large dataset <train-datasets>`?
* How do I :ref:`tune <train-tune>` my Ray Train model?

.. _train-backends:

Backends
--------

Ray Train provides a thin API around different backend frameworks for
distributed deep learning. At the moment, Ray Train allows you to perform
training with:

* **PyTorch:** Ray Train initializes your distributed process group, allowing
  you to run your ``DistributedDataParallel`` training script. See `PyTorch
  Distributed Overview <https://pytorch.org/tutorials/beginner/dist_overview.html>`_
  for more information.
* **TensorFlow:**  Ray Train configures ``TF_CONFIG`` for you, allowing you to run
  your ``MultiWorkerMirroredStrategy`` training script. See `Distributed
  training with TensorFlow <https://www.tensorflow.org/guide/distributed_training>`_
  for more information.
* **Horovod:** Ray Train configures the Horovod environment and Rendezvous
  server for you, allowing you to run your ``DistributedOptimizer`` training
  script. See `Horovod documentation <https://horovod.readthedocs.io/en/stable/index.html>`_
  for more information.

.. _train-porting-code:

Porting code to Ray Train
-------------------------

The following instructions assume you have a training function
that can already be run on a single worker for one of the supported
:ref:`backend <train-backends>` frameworks.

Update training function
~~~~~~~~~~~~~~~~~~~~~~~~

First, you'll want to update your training function to support distributed
training.

.. tabs::

  .. group-tab:: PyTorch

    Ray Train will set up your distributed process group for you. You simply
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
            device = torch.device(f"cuda:{train.local_rank()}" if
                          torch.cuda.is_available() else "cpu")
            torch.cuda.set_device(device)

            # Create model.
            model = NeuralNetwork()
            model = model.to(device)
            model = DistributedDataParallel(
                model,
                device_ids=[train.local_rank()] if torch.cuda.is_available() else None)


  .. group-tab:: TensorFlow

    .. note::
       The current TensorFlow implementation supports
       ``MultiWorkerMirroredStrategy`` (and ``MirroredStrategy``). If there are
       other strategies you wish to see supported by Ray Train, please let us know
       by submitting a `feature request on GitHub`_.

    These instructions closely follow TensorFlow's `Multi-worker training
    with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_
    tutorial. One key difference is that Ray Train will handle the environment
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

Create Ray Train Trainer
~~~~~~~~~~~~~~~~~~~~~~~~

The ``Trainer`` is the primary Ray Train class that is used to manage state and
execute training. You can create a simple ``Trainer`` for the backend of choice
with one of the following:

.. code-block:: python

    torch_trainer = Trainer(backend="torch", num_workers=2)

    tensorflow_trainer = Trainer(backend="tensorflow", num_workers=2)

    horovod_trainer = Trainer(backend="horovod", num_workers=2)

For more configurability, please reference the :ref:`train-api-trainer` API.
To customize the ``backend`` setup, you can replace the string argument with a
:ref:`train-api-backend-config` object.

Run training function
~~~~~~~~~~~~~~~~~~~~~

With a distributed training function and a Ray Train ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.start() # set up resources
    trainer.run(train_func)
    trainer.shutdown() # clean up resources

.. To make existing code from the previous SGD API, see :ref:`Backwards Compatibility <train-backwards-compatibility>`.

.. _`feature request on GitHub`: https://github.com/ray-project/ray/issues

Configuring Training
--------------------

With Ray Train, you can execute a training function (``train_func``) in a
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

    from ray.train import Trainer

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
perform hyperparameter tuning with Ray Train, please refer to the
:ref:`Ray Tune integration <train-tune>`.

.. TODO add support for with_parameters


.. _train-log-dir:

Log Directory Structure
-----------------------

Each ``Trainer`` will have a local directory created for logs, and each call
to ``Trainer.run`` will create its own sub-directory of logs.

By default, the ``logdir`` will be created at
``~/ray_results/train_<datestring>``.
This can be overridden in the ``Trainer`` constructor to an absolute path or
a path relative to ``~/ray_results``.

Log directories are exposed through the following attributes:

+------------------------+-----------------------------------------------------+
| Attribute              | Example                                             |
+========================+=====================================================+
| trainer.logdir         | /home/ray_results/train_2021-09-01_12-00-00         |
+------------------------+-----------------------------------------------------+
| trainer.latest_run_dir | /home/ray_results/train_2021-09-01_12-00-00/run_001 |
+------------------------+-----------------------------------------------------+

Logs will be written by:

1. :ref:`Logging Callbacks <train-logging-callbacks>`
2. :ref:`Checkpoints <train-checkpointing>`

.. TODO link to Training Run Iterator API as a 3rd option for logging.

.. _train-monitoring:

Logging, Monitoring, and Callbacks
----------------------------------

Reporting intermediate results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Train provides a ``train.report(**kwargs)`` API for reporting intermediate
results from the training function up to the ``Trainer``.

Using ``Trainer.run``, these results can be processed through :ref:`Callbacks
<train-callbacks>` with a ``handle_result`` method defined.

For custom handling, the lower-level ``Trainer.run_iterator`` API produces an
:ref:`train-api-iterator` which will iterate over the reported results.

The primary use-case for reporting is for metrics (accuracy, loss, etc.).

.. code-block:: python

    def train_func():
        ...
        for i in range(num_epochs):
            results = model.train(...)
            train.report(results)
        return model

Autofilled metrics
++++++++++++++++++

In addition to user defined metrics, a few fields are automatically populated:

.. code-block:: python

    # Unix epoch time in seconds when the data is reported.
    _timestamp
    # Time in seconds between iterations.
    _time_this_iter_s
    # The iteration ID, where each iteration is defined by one call to train.report().
    # This is a 1-indexed incrementing integer ID.
    _training_iteration

For debugging purposes, a more extensive set of metrics can be included in
any run by setting the ``TRAIN_RESULT_ENABLE_DETAILED_AUTOFILLED_METRICS`` environment
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


.. _train-callbacks:

Callbacks
~~~~~~~~~

You may want to plug in your training code with your favorite experiment management framework.
Ray Train provides an interface to fetch intermediate results and callbacks to process/log your intermediate results.

You can plug all of these into Ray Train with the following interface:

.. code-block:: python

    from ray import train
    from ray.train import Trainer, TrainingCallback
    from typing import List, Dict

    class PrintingCallback(TrainingCallback):
        def handle_result(self, results: List[Dict], **info):
            print(results)

    def train_func():
        for i in range(3):
            train.report(epoch=i)

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

.. Here is a list of callbacks that are supported by Ray Train:

.. * JsonLoggerCallback
.. * TBXLoggerCallback
.. * WandbCallback
.. * MlflowCallback
.. * CSVCallback

.. _train-logging-callbacks:

Logging Callbacks
+++++++++++++++++

The following ``TrainingCallback``\s are available and will write to a file within the
:ref:`log directory <train-log-dir>` of each training run.

1. :ref:`train-api-json-logger-callback`
2. :ref:`train-api-tbx-logger-callback`

Custom Callbacks
++++++++++++++++

If the provided callbacks do not cover your desired integrations or use-cases,
you may always implement a custom callback by subclassing ``TrainingCallback``. If
the callback is general enough, please feel welcome to `add it <https://docs
.ray.io/en/master/getting-involved.html>`_ to the ``ray``
`repository <https://github.com/ray-project/ray>`_.

A simple example for creating a callback that will print out results:

.. code-block:: python

    from ray.train import TrainingCallback

    class PrintingCallback(TrainingCallback):
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

Ray Train natively supports `TorchMetrics <https://torchmetrics.readthedocs.io/en/latest/>`_, which provides a collection of machine learning metrics for distributed, scalable Pytorch models.

Here is an example:

.. code-block:: python

    from ray import train
    from train.train import Trainer, TrainingCallback
    from typing import List, Dict

    import torch
    import torchmetrics

    class PrintingCallback(TrainingCallback):
        def handle_result(self, results: List[Dict], **info):
            print(results)

    def train_func(config):
        preds = torch.randn(10, 5).softmax(dim=-1)
        target = torch.randint(5, (10,))
        accuracy = torchmetrics.functional.accuracy(preds, target).item()
        train.report(accuracy=accuracy)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    result = trainer.run(
        train_func,
        callbacks=[PrintingCallback()]
    )
    # [{'accuracy': 0.20000000298023224, '_timestamp': 1630716913, '_time_this_iter_s': 0.0039408206939697266, '_training_iteration': 1},
    #  {'accuracy': 0.10000000149011612, '_timestamp': 1630716913, '_time_this_iter_s': 0.0030548572540283203, '_training_iteration': 1}]
    trainer.shutdown()

.. _train-checkpointing:

Checkpointing
-------------

Ray Train provides a way to save state during the training process. This is
useful for:

1. :ref:`Integration with Ray Tune <train-tune>` to use certain Ray Tune
   schedulers.
2. Running a long-running training job on a cluster of pre-emptible machines/pods.
3. Persisting trained model state to later use for serving/inference.
4. In general, storing any model artifacts.

Saving checkpoints
~~~~~~~~~~~~~~~~~~

Checkpoints can be saved by calling ``train.save_checkpoint(**kwargs)`` in the
training function.

.. note:: This must be called by all workers, but only data from the rank 0
          worker will be saved by the ``Trainer``.

The latest saved checkpoint can be accessed through the ``Trainer``'s
``latest_checkpoint`` attribute.

.. code-block:: python

    from ray import train
    from ray.train import Trainer

    def train_func(config):
        model = 0 # This should be replaced with a real model.
        for epoch in range(config["num_epochs"]):
            model += epoch
            train.save_checkpoint(epoch=epoch, model=model)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, config={"num_epochs": 5})
    trainer.shutdown()

    print(trainer.latest_checkpoint)
    # {'epoch': 4, 'model': 10}

By default, checkpoints will be persisted to local disk in the :ref:`log
directory <train-log-dir>` of each run.

.. code-block:: python

    print(trainer.latest_checkpoint_dir)
    # /home/ray_results/train_2021-09-01_12-00-00/run_001/checkpoints

    # By default, the "best" checkpoint path will refer to the most recent one.
    # This can be configured by defining a CheckpointStrategy.
    print(trainer.best_checkpoint_path)
    # /home/ray_results/train_2021-09-01_12-00-00/run_001/checkpoints/checkpoint_000005


.. note:: Persisting checkpoints to durable storage (e.g. S3) is not yet supported.

Configuring checkpoints
+++++++++++++++++++++++

For more configurability of checkpointing behavior (specifically saving
checkpoints to disk), a :ref:`train-api-checkpoint-strategy` can be passed into
``Trainer.run``.

As an example, to completely disable writing checkpoints to disk:

.. code-block:: python
    :emphasize-lines: 8,12

    from ray import train
    from ray.train import CheckpointStrategy, Trainer

    def train_func():
        for epoch in range(3):
            train.save_checkpoint(epoch=epoch)

    checkpoint_strategy = CheckpointStrategy(num_to_keep=0)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, checkpoint_strategy=checkpoint_strategy)
    trainer.shutdown()


You may also config ``CheckpointStrategy`` to keep the "N best" checkpoints persisted to disk. The following example shows how you could keep the 2 checkpoints with the lowest "loss" value:

.. code-block:: python

    from ray import train
    from ray.train import CheckpointStrategy, Trainer


    def train_func():
        # first checkpoint
        train.save_checkpoint(loss=2)
        # second checkpoint
        train.save_checkpoint(loss=4)
        # third checkpoint
        train.save_checkpoint(loss=1)
        # fourth checkpoint
        train.save_checkpoint(loss=3)

    # Keep the 2 checkpoints with the smallest "loss" value.
    checkpoint_strategy = CheckpointStrategy(num_to_keep=2,
                                             checkpoint_score_attribute="loss",
                                             checkpoint_score_order="min")

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, checkpoint_strategy=checkpoint_strategy)
    print(trainer.best_checkpoint_path)
    # /home/ray_results/train_2021-09-01_12-00-00/run_001/checkpoints/checkpoint_000003
    print(trainer.latest_checkpoint_dir)
    # /home/ray_results/train_2021-09-01_12-00-00/run_001/checkpoints
    print([checkpoint_path for checkpoint_path in trainer.latest_checkpoint_dir.iterdir()])
    # [PosixPath('/home/ray_results/train_2021-09-01_12-00-00/run_001/checkpoints/checkpoint_000003'),
    # PosixPath('/home/ray_results/train_2021-09-01_12-00-00/run_001/checkpoints/checkpoint_000001')]
    trainer.shutdown()

Loading checkpoints
~~~~~~~~~~~~~~~~~~~

Checkpoints can be loaded into the training function in 2 steps:

1. From the training function, ``train.load_checkpoint()`` can be used to access
   the most recently saved checkpoint. This is useful to continue training even
   if there's a worker failure.
2. The checkpoint to start training with can be bootstrapped by passing in a
   ``checkpoint`` to ``trainer.run()``.

.. code-block:: python

    from ray import train
    from ray.train import Trainer

    def train_func(config):
        checkpoint = train.load_checkpoint() or {}
        # This should be replaced with a real model.
        model = checkpoint.get("model", 0)
        start_epoch = checkpoint.get("epoch", -1) + 1
        for epoch in range(start_epoch, config["num_epochs"]):
            model += epoch
            train.save_checkpoint(epoch=epoch, model=model)

    trainer = Trainer(backend="torch", num_workers=2)
    trainer.start()
    trainer.run(train_func, config={"num_epochs": 5},
                checkpoint={"epoch": 2, "model": 3})
    trainer.shutdown()

    print(trainer.latest_checkpoint)
    # {'epoch': 4, 'model': 10}

.. Running on the cloud
.. --------------------

.. Use Ray Train with the Ray cluster launcher by changing the following:

.. .. code-block:: bash

..     ray up cluster.yaml

.. TODO.

.. _train-fault-tolerance:

Fault Tolerance & Elastic Training
----------------------------------

Ray Train has built-in fault tolerance to recover from worker failures (i.e.
``RayActorError``\s). When a failure is detected, the workers will be shut
down and new workers will be added in. The training function will be
restarted, but progress from the previous execution can be resumed through
checkpointing.

.. warning:: In order to retain progress when recovery, your training function
   **must** implement logic for both saving *and* loading :ref:`checkpoints
   <train-checkpointing>`.

Each instance of recovery from a worker failure is considered a retry. The
number of retries is configurable through the ``max_retries`` argument of the
``Trainer`` constructor.

.. note:: Elastic Training is not yet supported.

.. Running on pre-emptible machines
.. --------------------------------

.. You may want to

.. TODO.

.. _train-datasets:

Distributed Data Ingest (Ray Datasets)
--------------------------------------

Ray Train provides native support for :ref:`Ray Datasets <datasets>` to support the following use cases:

1. **Large Datasets**: With Ray Datasets, you can easily work with datasets that are too big to fit on a single node.
   Ray Datasets will distribute the dataset across the Ray Cluster and allow you to perform dataset operations (map, filter, etc.)
   on the distributed dataset.
2. **Automatic locality-aware sharding**: If provided a Ray Dataset, Ray Train will automatically shard the dataset and assign each shard
   to a training worker while minimizing cross-node data transfer. Unlike with standard Torch or Tensorflow datasets, each training
   worker will only load its assigned shard into memory rather than the entire ``Dataset``.
3. **Pipelined Execution**: Ray Datasets also supports pipelining, meaning that data processing operations
   can be run concurrently with training. Training is no longer blocked on expensive data processing operations (such as global shuffling)
   and this minimizes the amount of time your GPUs are idle. See :ref:`dataset-pipeline` for more information.

To get started, pass in a Ray Dataset (or multiple) into ``Trainer.run``. Underneath the hood, Ray Train will automatically shard the given dataset.

.. warning::

    If you are doing distributed training with Tensorflow, you will need to
    disable Tensorflow's built-in autosharding as the data on each worker is
    already sharded.

    .. code-block:: python

        def train_func():
            ...
            tf_dataset = ray.train.get_dataset_shard().to_tf()
            options = tf.data.Options()
            options.experimental_distribute.auto_shard_policy = \
                tf.data.experimental.AutoShardPolicy.OFF
            tf_dataset = tf_dataset.with_options(options)


**Simple Dataset Example**

.. code-block:: python

    def train_func(config):
        # Create your model here.
        model = NeuralNetwork()

        batch_size = config["worker_batch_size"]

        train_data_shard = ray.train.get_dataset_shard("train")
        train_torch_dataset = train_data_shard.to_torch(label_column="label",
                                                  batch_size=batch_size)

        validation_data_shard = ray.train.get_dataset_shard("validation")
        validation_torch_dataset = validation_data_shard.to_torch(label_column="label",
                                                                  batch_size=batch_size)

        for epoch in config["num_epochs"]:
            for X, y in train_torch_dataset:
                model.train()
                output = model(X)
                # Train on one batch.
            for X, y in validation_torch_dataset:
                model.eval()
                output = model(X)
                # Validate one batch.
        return model

    trainer = Trainer(num_workers=8, backend="torch")
    dataset = ray.data.read_csv("...")

    # Random split dataset into 80% training data and 20% validation data.
    split_index = int(dataset.count() * 0.8)
    train_dataset, validation_dataset = \
        dataset.random_shuffle().split_at_indices([split_index])

    result = trainer.run(
        train_func,
        config={"worker_batch_size": 64, "num_epochs": 2},
        dataset={
            "train": train_dataset,
            "validation": validation_dataset
        })

.. _train-dataset-pipeline:

Pipelined Execution
~~~~~~~~~~~~~~~~~~~
For pipelined execution, you just need to convert your :ref:`Dataset <datasets>` into a :ref:`DatasetPipeline <dataset-pipeline>`.
All operations after this conversion will be executed in a pipelined fashion.

See :ref:`dataset-pipeline` for more semantics on pipelining.

Example: Per-Epoch Shuffle Pipeline
+++++++++++++++++++++++++++++++++++
A common use case is to have a training pipeline that globally shuffles the dataset before every epoch.

This is very simple to do with Ray Datasets + Ray Train.

.. code-block:: python

    def train_func():
        # This is a dummy train function just iterating over the dataset.
        # You should replace this with your training logic.
        dataset_pipeline_shard = ray.train.get_dataset_shard()
        # Infinitely long iterator of randomly shuffled dataset shards.
        dataset_iterator = train_dataset_pipeline_shard.iter_datasets()
        for _ in range(config["num_epochs"]):
            # Single randomly shuffled dataset shard.
            train_dataset = next(dataset_iterator)
            # Convert shard to native Torch Dataset.
            train_torch_dataset = train_dataset.to_torch(label_column="label",
                                                         batch_size=batch_size)
            # Train on your Torch Dataset here!

    # Create a pipeline that loops over its source dataset indefinitely,
    # with each repeat of the dataset randomly shuffled.
    dataset_pipeline: DatasetPipeline = ray.data \
        .read_parquet(...) \
        .repeat() \
        .random_shuffle_each_window()

    # Pass in the pipeline to the Trainer.
    # The Trainer will automatically split the DatasetPipeline for you.
    trainer = Trainer(num_workers=8, backend="torch")
    result = trainer.run(
        train_func,
        config={"worker_batch_size": 64, "num_epochs": 2},
        dataset=dataset_pipeline)


You can easily set the working set size for the global shuffle by specifying the window size of the ``DatasetPipeline``.

.. code-block:: python

    # Create a pipeline that loops over its source dataset indefinitely.
    pipe: DatasetPipeline = ray.data \
        .read_parquet(...) \
        .window(blocks_per_window=10) \
        .repeat() \
        .random_shuffle_each_window()


See :ref:`dataset-pipeline-per-epoch-shuffle` for more info.


.. _train-tune:

Hyperparameter tuning (Ray Tune)
--------------------------------

Hyperparameter tuning with :ref:`Ray Tune <tune-main>` is natively supported
with Ray Train. Specifically, you can take an existing training function and
follow these steps:

**Step 1: Convert to Tune Trainable**

Instantiate your Trainer and call ``trainer.to_tune_trainable``, which will
produce an object ("Trainable") that will be passed to Ray Tune.

.. code-block:: python

    from ray import train
    from ray.train import Trainer

    def train_func(config):
        # In this example, nothing is expected to change over epochs,
        # and the output metric is equivalent to the input value.
        for _ in range(config["num_epochs"]):
            train.report(output=config["input"])

    trainer = Trainer(backend="torch", num_workers=2)
    trainable = trainer.to_tune_trainable(train_func)

**Step 2: Call tune.run**

Call ``tune.run`` on the created ``Trainable`` to start multiple ``Tune``
"trials", each running a Ray Train job and each with a unique hyperparameter
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
  trained model, you will need to use the ``train.save_checkpoint`` API.
* You should **not** call ``tune.report`` or ``tune.checkpoint_dir`` in your
  training function. Functional parity is achieved through ``train.report``,
  ``train.save_checkpoint``, and ``train.load_checkpoint``. This allows you to go
  from Ray Train to Ray Train+RayTune without changing any code in the training
  function.


.. code-block:: python

    from ray import train, tune
    from ray.train import Trainer

    def train_func(config):
        # In this example, nothing is expected to change over epochs,
        # and the output metric is equivalent to the input value.
        for _ in range(config["num_epochs"]):
            train.report(output=config["input"])

    trainer = Trainer(backend="torch", num_workers=2)
    trainable = trainer.to_tune_trainable(train_func)
    analysis = tune.run(trainable, config={
        "num_epochs": 2,
        "input": tune.grid_search([1, 2, 3])
    })
    print(analysis.get_best_config(metric="output", mode="max"))
    # {'num_epochs': 2, 'input': 3}


..
    import ray
    from ray import tune

    def training_func(config):
        dataloader = ray.train.get_dataset()\
            .get_shard(torch.rank())\
            .to_torch(batch_size=config["batch_size"])

        for i in config["epochs"]:
            ray.train.report(...)  # use same intermediate reporting API

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

.. _train-backwards-compatibility:

..
    Backwards Compatibility
    -------------

    TODO
