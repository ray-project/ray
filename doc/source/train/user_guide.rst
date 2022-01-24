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

    Ray Train will set up your distributed process group for you and also provides utility methods
    to automatically prepare your model and data for distributed training.

    .. note::
       Ray Train will still work even if you don't use the ``prepare_model`` and ``prepare_data_loader`` utilities below,
       and instead handle the logic directly inside your training function.

    First, use the ``prepare_model`` function to automatically move your model to the right device and wrap it in
    ``DistributedDataParallel``

    .. code-block:: diff

        import torch
        from torch.nn.parallel import DistributedDataParallel
        +from ray import train


        def train_func():
        -   device = torch.device(f"cuda:{train.local_rank()}" if
        -         torch.cuda.is_available() else "cpu")
        -   torch.cuda.set_device(device)

            # Create model.
            model = NeuralNetwork()

        -   model = model.to(device)
        -   model = DistributedDataParallel(model,
        -       device_ids=[train.local_rank()] if torch.cuda.is_available() else None)

        +   model = train.torch.prepare_model(model)

            ...


    Then, use the ``prepare_data_loader`` function to automatically add a ``DistributedSampler`` to your ``DataLoader``
    and move the batches to the right device.

    .. code-block:: diff

        import torch
        from torch.utils.data import DataLoader, DistributedSampler
        +from ray import train


        def train_func():
        -   device = torch.device(f"cuda:{train.local_rank()}" if
        -          torch.cuda.is_available() else "cpu")
        -   torch.cuda.set_device(device)

            ...

        -   data_loader = DataLoader(my_dataset, batch_size=worker_batch_size, sampler=DistributedSampler(dataset))

        +   data_loader = DataLoader(my_dataset, batch_size=worker_batch_size)
        +   data_loader = train.torch.prepare_data_loader(data_loader)

            for X, y in data_loader:
        -       X = X.to_device(device)
        -       y = y.to_device(device)

    .. tip::
       Keep in mind that ``DataLoader`` takes in a ``batch_size`` which is the batch size for each worker.
       The global batch size can be calculated from the worker batch size (and vice-versa) with the following equation:

       .. code-block::

            global_batch_size = worker_batch_size * train.world_size()

  .. group-tab:: TensorFlow

    .. note::
       The current TensorFlow implementation supports
       ``MultiWorkerMirroredStrategy`` (and ``MirroredStrategy``). If there are
       other strategies you wish to see supported by Ray Train, please let us know
       by submitting a `feature request on GitHub <https://github.com/ray-project/ray/issues>`_.

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
        +batch_size = worker_batch_size * train.world_size()

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

.. tabs::

  .. group-tab:: PyTorch

    .. code-block:: python

        from ray.train import Trainer
        trainer = Trainer(backend="torch", num_workers=2)

        # For GPU Training, set `use_gpu` to True.
        # trainer = Trainer(backend="torch", num_workers=2, use_gpu=True)


  .. group-tab:: TensorFlow

    .. code-block:: python

        from ray.train import Trainer
        trainer = Trainer(backend="tensorflow", num_workers=2)

        # For GPU Training, set `use_gpu` to True.
        # trainer = Trainer(backend="tensorflow", num_workers=2, use_gpu=True)

  .. group-tab:: Horovod

    .. code-block:: python

        from ray.train import Trainer
        trainer = Trainer(backend="horovod", num_workers=2)

        # For GPU Training, set `use_gpu` to True.
        # trainer = Trainer(backend="horovod", num_workers=2, use_gpu=True)

To customize the ``backend`` setup, you can replace the string argument with a
:ref:`train-api-backend-config` object.

.. tabs::

  .. group-tab:: PyTorch

    .. code-block:: python

        from ray.train import Trainer
        from ray.train.torch import TorchConfig

        trainer = Trainer(backend=TorchConfig(...), num_workers=2)


  .. group-tab:: TensorFlow

    .. code-block:: python

        from ray.train import Trainer
        from ray.train.tensorflow import TensorflowConfig

        trainer = Trainer(backend=TensorflowConfig(...), num_workers=2)

  .. group-tab:: Horovod

    .. code-block:: python

        from ray.train import Trainer
        from ray.train.horovod import HorovodConfig

        trainer = Trainer(backend=HorovodConfig(...), num_workers=2)

For more configurability, please reference the :ref:`train-api-trainer` API.

Run training function
~~~~~~~~~~~~~~~~~~~~~

With a distributed training function and a Ray Train ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.start() # set up resources
    trainer.run(train_func)
    trainer.shutdown() # clean up resources

Configuring Training
--------------------

With Ray Train, you can execute a training function (``train_func``) in a
distributed manner by calling ``trainer.run(train_func)``. To pass arguments
into the training function, you can expose a single ``config`` dictionary parameter:

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

1. :ref:`Callbacks <train-callbacks>`
2. :ref:`Checkpoints <train-checkpointing>`

.. TODO link to Training Run Iterator API as a 3rd option for logging.

.. _train-monitoring:

Logging, Monitoring, and Callbacks
----------------------------------

Ray Train has mechanisms to easily collect intermediate results from the training workers during the training run
and also has a :ref:`Callback interface <train-callbacks>` to perform actions on these intermediate results (such as logging, aggregations, printing, etc.).
You can use either the :ref:`built-in callbacks <train-builtin-callbacks>` that Ray Train provides,
or implement a :ref:`custom callback <train-custom-callbacks>` for your use case.

Reporting intermediate results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Train provides a ``train.report(**kwargs)`` API for reporting intermediate
results from the training function (run on distributed workers) up to the
``Trainer`` (where your python script is executed).

Using ``Trainer.run``, these results can be processed through :ref:`Callbacks
<train-callbacks>` with a ``handle_result`` method defined.

The primary use-case for reporting is for metrics (accuracy, loss, etc.) at
the end of each training epoch.

.. code-block:: python

    def train_func():
        ...
        for i in range(num_epochs):
            results = model.train(...)
            train.report(results)
        return model


For custom handling, the lower-level ``Trainer.run_iterator`` API produces a
:ref:`train-api-iterator` which will iterate over the reported results.

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
Ray Train provides an interface to fetch intermediate results and callbacks to process/log your intermediate results
(the values passed into ``train.report(...)``).

Ray Train contains built-in callbacks for popular tracking frameworks, or you can implement your own callback via the ``TrainingCallback`` interface.

.. _train-builtin-callbacks:

Built-in Callbacks
++++++++++++++++++

The following ``TrainingCallback``\s are available and will log the intermediate results of the training run.

1. :ref:`train-api-print-callback`
2. :ref:`train-api-json-logger-callback`
3. :ref:`train-api-tbx-logger-callback`
4. :ref:`train-api-mlflow-logger-callback`

Example: Logging to MLflow and Tensorboard
++++++++++++++++++++++++++++++++++++++++++

**Step 1: Install the necessary packages**

.. code-block:: bash

    $ pip install mlflow
    $ pip install tensorboardX

**Step 2: Run the following training script**

.. literalinclude:: /../../python/ray/train/examples/mlflow_simple_example.py
   :language: python

**Step 3: Visualize the logs**

.. code-block:: bash

    # Navigate to the run directory of the trainer.
    # For example `cd /home/ray_results/train_2021-09-01_12-00-00/run_001`
    $ cd <TRAINER_RUN_DIR>

    # View the MLflow UI.
    $ mlflow ui

    # View the tensorboard UI.
    $ tensorboard --logdir .


.. _train-custom-callbacks:

Custom Callbacks
++++++++++++++++

If the provided callbacks do not cover your desired integrations or use-cases,
you may always implement a custom callback by subclassing ``TrainingCallback``. If
the callback is general enough, please feel welcome to `add it <https://docs
.ray.io/en/master/getting-involved.html>`_ to the ``ray``
`repository <https://github.com/ray-project/ray>`_.

A simple example for creating a callback that will print out results:

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
training function. This will cause the checkpoint state from the distributed
workers to be saved on the ``Trainer`` (where your python script is executed).

The latest saved checkpoint can be accessed through the ``Trainer``'s
``latest_checkpoint`` attribute.

Concrete examples are provided to demonstrate how checkpoints (model weights but not models) are saved
appropriately in distributed training.

.. tabs::

  .. group-tab:: PyTorch

    .. code-block:: python
        :emphasize-lines: 37, 38, 39

        import ray.train.torch
        from ray import train
        from ray.train import Trainer

        import torch
        import torch.nn as nn
        from torch.nn.modules.utils import consume_prefix_in_state_dict_if_present
        from torch.optim import Adam
        import numpy as np


        def train_func(config):
            n = 100
            # create a toy dataset
            # data   : X - dim = (n, 4)
            # target : Y - dim = (n, 1)
            X = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
            Y = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))
            # toy neural network : 1-layer
            # wrap the model in DDP
            model = ray.train.torch.prepare_model(nn.Linear(4, 1))
            criterion = nn.MSELoss()

            optimizer = Adam(model.parameters(), lr=3e-4)
            for epoch in range(config["num_epochs"]):
                y = model.forward(X)
                # compute loss
                loss = criterion(y, Y)
                # back-propagate loss
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                # To fetch non-DDP state_dict
                # w/o DDP: model.state_dict()
                # w/  DDP: model.module.state_dict()
                # See: https://github.com/ray-project/ray/issues/20915
                state_dict = model.state_dict()
                consume_prefix_in_state_dict_if_present(state_dict, "module.")
                train.save_checkpoint(epoch=epoch, model_weights=state_dict)


        trainer = Trainer(backend="torch", num_workers=2)
        trainer.start()
        trainer.run(train_func, config={"num_epochs": 5})
        trainer.shutdown()

        print(trainer.latest_checkpoint)
        # {'epoch': 4, 'model_weights': OrderedDict([('bias', tensor([0.1533])), ('weight', tensor([[0.4529, 0.4618, 0.2730, 0.0190]]))]), '_timestamp': 1639117274}


  .. group-tab:: TensorFlow

    .. code-block:: python
        :emphasize-lines: 24

        from ray import train
        from ray.train import Trainer

        import numpy as np


        def train_func(config):
            import tensorflow as tf
            n = 100
            # create a toy dataset
            # data   : X - dim = (n, 4)
            # target : Y - dim = (n, 1)
            X = np.random.normal(0, 1, size=(n, 4))
            Y = np.random.uniform(0, 1, size=(n, 1))

            strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
            with strategy.scope():
                # toy neural network : 1-layer
                model = tf.keras.Sequential([tf.keras.layers.Dense(1, activation="linear", input_shape=(4,))])
                model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

            for epoch in range(config["num_epochs"]):
                model.fit(X, Y, batch_size=20)
                train.save_checkpoint(epoch=epoch, model_weights=model.get_weights())


        trainer = Trainer(backend="tensorflow", num_workers=2)
        trainer.start()
        trainer.run(train_func, config={"num_epochs": 5})
        trainer.shutdown()

        print(trainer.latest_checkpoint)
        # {'epoch': 4, 'model_weights': [array([[-0.03075046], [-0.8020745 ], [-0.13172336], [ 0.6760253 ]], dtype=float32), array([0.02125629], dtype=float32)], '_timestamp': 1639117674}


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

.. tabs::

  .. group-tab:: PyTorch

    .. code-block:: python
        :emphasize-lines: 24, 26, 27, 30, 31, 35

        import ray.train.torch
        from ray import train
        from ray.train import Trainer

        import torch
        import torch.nn as nn
        from torch.nn.modules.utils import consume_prefix_in_state_dict_if_present
        from torch.optim import Adam
        import numpy as np


        def train_func(config):
            n = 100
            # create a toy dataset
            # data   : X - dim = (n, 4)
            # target : Y - dim = (n, 1)
            X = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
            Y = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))

            # toy neural network : 1-layer
            model = nn.Linear(4, 1)
            criterion = nn.MSELoss()
            optimizer = Adam(model.parameters(), lr=3e-4)
            start_epoch = 0

            checkpoint = train.load_checkpoint()
            if checkpoint:
                # assume that we have run the train.save_checkpoint() example
                # and successfully save some model weights
                model.load_state_dict(checkpoint.get("model_weights"))
                start_epoch = checkpoint.get("epoch", -1) + 1

            # wrap the model in DDP
            model = ray.train.torch.prepare_model(model)
            for epoch in range(start_epoch, config["num_epochs"]):
                y = model.forward(X)
                # compute loss
                loss = criterion(y, Y)
                # back-propagate loss
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                state_dict = model.state_dict()
                consume_prefix_in_state_dict_if_present(state_dict, "module.")
                train.save_checkpoint(epoch=epoch, model_weights=state_dict)


        trainer = Trainer(backend="torch", num_workers=2)
        trainer.start()
        # save a checkpoint
        trainer.run(train_func, config={"num_epochs": 2})
        # load a checkpoint
        trainer.run(train_func, config={"num_epochs": 4},
                    checkpoint=trainer.latest_checkpoint)

        trainer.shutdown()

        print(trainer.latest_checkpoint)
        # {'epoch': 3, 'model_weights': OrderedDict([('bias', tensor([-0.3304])), ('weight', tensor([[-0.0197, -0.3704,  0.2944,  0.3117]]))]), '_timestamp': 1639117865}

  .. group-tab:: TensorFlow

    .. code-block:: python
        :emphasize-lines: 16, 22, 23, 26, 27, 30

        from ray import train
        from ray.train import Trainer

        import numpy as np


        def train_func(config):
            import tensorflow as tf
            n = 100
            # create a toy dataset
            # data   : X - dim = (n, 4)
            # target : Y - dim = (n, 1)
            X = np.random.normal(0, 1, size=(n, 4))
            Y = np.random.uniform(0, 1, size=(n, 1))

            start_epoch = 0
            strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()

            with strategy.scope():
                # toy neural network : 1-layer
                model = tf.keras.Sequential([tf.keras.layers.Dense(1, activation="linear", input_shape=(4,))])
                checkpoint = train.load_checkpoint()
                if checkpoint:
                    # assume that we have run the train.save_checkpoint() example
                    # and successfully save some model weights
                    model.set_weights(checkpoint.get("model_weights"))
                    start_epoch = checkpoint.get("epoch", -1) + 1
                model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

            for epoch in range(start_epoch, config["num_epochs"]):
                model.fit(X, Y, batch_size=20)
                train.save_checkpoint(epoch=epoch, model_weights=model.get_weights())


        trainer = Trainer(backend="tensorflow", num_workers=2)
        trainer.start()
        # save a checkpoint
        trainer.run(train_func, config={"num_epochs": 2})
        trainer.shutdown()

        # restart the trainer for the loading checkpoint example
        # TensorFlow ops need to be created after a MultiWorkerMirroredStrategy instance is created.
        # See: https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras#train_the_model_with_multiworkermirroredstrategy
        trainer.start()
        # load a checkpoint
        trainer.run(train_func, config={"num_epochs": 5},
                    checkpoint=trainer.latest_checkpoint)
        trainer.shutdown()

        print(trainer.latest_checkpoint)
        # {'epoch': 4, 'model_weights': [array([[ 0.06892418], [-0.73326826], [ 0.76637405], [ 0.06124062]], dtype=float32), array([0.05737507], dtype=float32)], '_timestamp': 1639117991}



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
   to a training worker while minimizing cross-node data transfer. Unlike with standard Torch or TensorFlow datasets, each training
   worker will only load its assigned shard into memory rather than the entire ``Dataset``.
3. **Pipelined Execution**: Ray Datasets also supports pipelining, meaning that data processing operations
   can be run concurrently with training. Training is no longer blocked on expensive data processing operations (such as global shuffling)
   and this minimizes the amount of time your GPUs are idle. See :ref:`dataset-pipeline` for more information.

To get started, pass in a Ray Dataset (or multiple) into ``Trainer.run``. Underneath the hood, Ray Train will automatically shard the given dataset.

.. warning::

    If you are doing distributed training with TensorFlow, you will need to
    disable TensorFlow's built-in autosharding as the data on each worker is
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
        dataset_iterator = train_dataset_pipeline_shard.iter_epochs()
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


Backwards Compatibility with Ray SGD
------------------------------------

If you are currently using :ref:`RaySGD <sgd-index>`, you can migrate to Ray Train by following: :ref:`sgd-migration`.
