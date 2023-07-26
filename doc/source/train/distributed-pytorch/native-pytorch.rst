Native PyTorch training
=======================

Quickstart
----------

This example shows how you can use Ray Train with PyTorch.

First, set up your dataset and model.

.. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
    :language: python
    :start-after: __torch_setup_begin__
    :end-before: __torch_setup_end__


Now define your single-worker PyTorch training function.

.. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
    :language: python
    :start-after: __torch_single_begin__
    :end-before: __torch_single_end__

This training function can be executed with:

.. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
    :language: python
    :start-after: __torch_single_run_begin__
    :end-before: __torch_single_run_end__
    :dedent:

Now let's convert this to a distributed multi-worker training function!

All you have to do is use the ``ray.train.torch.prepare_model`` and
``ray.train.torch.prepare_data_loader`` utility functions to
easily setup your model & data for distributed training.
This will automatically wrap your model with ``DistributedDataParallel``
and place it on the right device, and add ``DistributedSampler`` to your DataLoaders.

.. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
    :language: python
    :start-after: __torch_distributed_begin__
    :end-before: __torch_distributed_end__

Then, instantiate a ``TorchTrainer``
with 4 workers, and use it to run the new training function!

.. literalinclude:: /../../python/ray/train/examples/pytorch/torch_quick_start.py
    :language: python
    :start-after: __torch_trainer_begin__
    :end-before: __torch_trainer_end__
    :dedent:


.. _train-porting-code-pytorch:

Porting code from an existing training loop
-------------------------------------------

The following instructions assume you have a training function
that can already be run on a single worker.

Updating your training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you'll want to update your training function to support distributed
training.

Ray Train will set up your distributed process group for you and also provides utility methods
to automatically prepare your model and data for distributed training.

.. note::
   Ray Train will still work even if you don't use the :func:`ray.train.torch.prepare_model`
   and :func:`ray.train.torch.prepare_data_loader` utilities below,
   and instead handle the logic directly inside your training function.

First, use the :func:`~ray.train.torch.prepare_model` function to automatically move your model to the right device and wrap it in
``DistributedDataParallel``:

.. code-block:: diff

     import torch
     from torch.nn.parallel import DistributedDataParallel
    +from ray.air import session
    +from ray import train
    +import ray.train.torch


     def train_func():
    -    device = torch.device(f"cuda:{session.get_local_rank()}" if
    -        torch.cuda.is_available() else "cpu")
    -    torch.cuda.set_device(device)

         # Create model.
         model = NeuralNetwork()

    -    model = model.to(device)
    -    model = DistributedDataParallel(model,
    -        device_ids=[session.get_local_rank()] if torch.cuda.is_available() else None)

    +    model = train.torch.prepare_model(model)

         ...


Then, use the ``prepare_data_loader`` function to automatically add a ``DistributedSampler`` to your ``DataLoader``
and move the batches to the right device. This step is not necessary if you are passing in Ray Data to your Trainer
(see :ref:`train-datasets`):

.. code-block:: diff

     import torch
     from torch.utils.data import DataLoader, DistributedSampler
    +from ray.air import session
    +from ray import train
    +import ray.train.torch


     def train_func():
    -    device = torch.device(f"cuda:{session.get_local_rank()}" if
    -        torch.cuda.is_available() else "cpu")
    -    torch.cuda.set_device(device)

         ...

    -    data_loader = DataLoader(my_dataset, batch_size=worker_batch_size, sampler=DistributedSampler(dataset))

    +    data_loader = DataLoader(my_dataset, batch_size=worker_batch_size)
    +    data_loader = train.torch.prepare_data_loader(data_loader)

         for X, y in data_loader:
    -        X = X.to_device(device)
    -        y = y.to_device(device)

.. tip::
    Keep in mind that ``DataLoader`` takes in a ``batch_size`` which is the batch size for each worker.
    The global batch size can be calculated from the worker batch size (and vice-versa) with the following equation:

    .. code-block:: python

        global_batch_size = worker_batch_size * session.get_world_size()


Creating a TorchTrainer
~~~~~~~~~~~~~~~~~~~~~~~

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. You can create a ``TorchTrainer`` like this:


.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )


To customize the backend setup, you can specify a :class:`TorchConfig <ray.train.torch.TorchConfig>`:

.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer, TorchConfig

    trainer = TorchTrainer(
        train_func,
        torch_backend=TorchConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )


Running your training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With a distributed training function and the ``TorchTrainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()


Data loading and preprocessing
------------------------------


Using GPUs
----------

Configuring scale
-----------------

Reporting results
-----------------

Saving and loading checkpoints
------------------------------
:ref:`Checkpoints <checkpoint-api-ref>` can be saved by calling ``session.report(metrics, checkpoint=Checkpoint(...))`` in the
training function. This will cause the checkpoint state from the distributed
workers to be saved on the ``Trainer`` (where your python script is executed).

The latest saved checkpoint can be accessed through the ``checkpoint`` attribute of
the :py:class:`~ray.air.result.Result`, and the best saved checkpoints can be accessed by the ``best_checkpoints``
attribute.

Saving checkpoints
~~~~~~~~~~~~~~~~~~

To save a checkpoint, you create a :class:`Checkpoint <ray.air.checkpoint.Checkpoint>`
object and report it to Ray Train:

.. code-block:: python
    :emphasize-lines: 36, 37, 38, 39, 40, 41

    import ray.train.torch
    from ray.air import session, Checkpoint, ScalingConfig
    from ray.train.torch import TorchTrainer

    import torch
    import torch.nn as nn
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
            state_dict = model.state_dict()
            checkpoint = Checkpoint.from_dict(
                dict(epoch=epoch, model_weights=state_dict)
            )
            session.report({}, checkpoint=checkpoint)

    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 5},
        scaling_config=ScalingConfig(num_workers=2),
    )
    result = trainer.fit()

    print(result.checkpoint.to_dict())
    # {'epoch': 4, 'model_weights': OrderedDict([('bias', tensor([-0.1215])), ('weight', tensor([[0.3253, 0.1979, 0.4525, 0.2850]]))]), '_timestamp': 1656107095, '_preprocessor': None, '_current_checkpoint_id': 4}

The checkpoints are saved to the configured :ref:`storage_path <train-persistent-storage>`.


Loading checkpoints
~~~~~~~~~~~~~~~~~~~

Checkpoints can be loaded into the training function in 2 steps:

1. From the training function, :func:`ray.air.session.get_checkpoint` can be used to access
   the most recently saved :py:class:`~ray.air.checkpoint.Checkpoint`. This is useful to continue training even
   if there's a worker failure.
2. The checkpoint to start training with can be bootstrapped by passing in a
   :py:class:`~ray.air.checkpoint.Checkpoint` to ``Trainer`` as the ``resume_from_checkpoint`` argument.


.. code-block:: python
    :emphasize-lines: 23, 25, 26, 29, 30, 31, 35

    import ray.train.torch
    from ray.air import session, Checkpoint, ScalingConfig
    from ray.train.torch import TorchTrainer

    import torch
    import torch.nn as nn
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

        checkpoint = session.get_checkpoint()
        if checkpoint:
            # assume that we have run the session.report() example
            # and successfully save some model weights
            checkpoint_dict = checkpoint.to_dict()
            model.load_state_dict(checkpoint_dict.get("model_weights"))
            start_epoch = checkpoint_dict.get("epoch", -1) + 1

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
            checkpoint = Checkpoint.from_dict(
                dict(epoch=epoch, model_weights=state_dict)
            )
            session.report({}, checkpoint=checkpoint)

    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 2},
        scaling_config=ScalingConfig(num_workers=2),
    )
    # save a checkpoint
    result = trainer.fit()

    # load checkpoint
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"num_epochs": 4},
        scaling_config=ScalingConfig(num_workers=2),
        resume_from_checkpoint=result.checkpoint,
    )
    result = trainer.fit()

    print(result.checkpoint.to_dict())
    # {'epoch': 3, 'model_weights': OrderedDict([('bias', tensor([0.0902])), ('weight', tensor([[-0.1549, -0.0861,  0.4353, -0.4116]]))]), '_timestamp': 1656108265, '_preprocessor': None, '_current_checkpoint_id': 2}




Experiment tracking
-------------------

Inspecting results
------------------



Aggregating results across workers
----------------------------------
Per default, Ray Train only reports results from the rank 0 worker.
This can be changed by using third-party libraries to aggregate results.

Ray Train natively supports `TorchMetrics <https://torchmetrics.readthedocs.io/en/latest/>`_, which provides a collection of machine learning metrics for distributed, scalable PyTorch models.

Here is an example of reporting both the aggregated R2 score and mean train and validation loss from all workers.

.. literalinclude:: ../doc_code/torchmetrics_example.py
    :language: python
    :start-after: __start__



.. _torch-amp:

Automatic Mixed Precision
-------------------------

Automatic mixed precision (AMP) lets you train your models faster by using a lower
precision datatype for operations like linear layers and convolutions.

.. tab-set::

    .. tab-item:: PyTorch

        You can train your Torch model with AMP by:

        1. Adding :func:`ray.train.torch.accelerate` with ``amp=True`` to the top of your training function.
        2. Wrapping your optimizer with :func:`ray.train.torch.prepare_optimizer`.
        3. Replacing your backward call with :func:`ray.train.torch.backward`.

        .. code-block:: diff

             def train_func():
            +    train.torch.accelerate(amp=True)

                 model = NeuralNetwork()
                 model = train.torch.prepare_model(model)

                 data_loader = DataLoader(my_dataset, batch_size=worker_batch_size)
                 data_loader = train.torch.prepare_data_loader(data_loader)

                 optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
            +    optimizer = train.torch.prepare_optimizer(optimizer)

                 model.train()
                 for epoch in range(90):
                     for images, targets in dataloader:
                         optimizer.zero_grad()

                         outputs = model(images)
                         loss = torch.nn.functional.cross_entropy(outputs, targets)

            -            loss.backward()
            +            train.torch.backward(loss)
                         optimizer.step()
                ...


.. note:: The performance of AMP varies based on GPU architecture, model type,
        and data shape. For certain workflows, AMP may perform worse than
        full-precision training.


.. _train-reproducibility:

Reproducibility
---------------

To limit sources of nondeterministic behavior, add
:func:`ray.train.torch.enable_reproducibility` to the top of your training
function.

.. code-block:: diff

     def train_func():
    +    train.torch.enable_reproducibility()

         model = NeuralNetwork()
         model = train.torch.prepare_model(model)

         ...

.. warning:: :func:`ray.train.torch.enable_reproducibility` can't guarantee
    completely reproducible results across executions. To learn more, read
    the `PyTorch notes on randomness <https://pytorch.org/docs/stable/notes/randomness.html>`_.

