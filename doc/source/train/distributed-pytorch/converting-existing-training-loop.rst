Converting an existing training loop
====================================

The following instructions assume you have a training function
that can already be run on a single worker.


Updating your training function
-------------------------------

First, you'll want to update your training function to support distributed
training.


.. tab-set::

    .. tab-item:: PyTorch

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

Creating a :class:`~ray.train.torch.TorchTrainer`
-------------------------------------------------

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. For distributed PyTorch, we use a :class:`~ray.train.torch.TorchTrainer`
that you can setup like this:


.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )



To customize the backend setup, you can pass a
:class:`~ray.train.torch.TorchConfig`:

.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.torch import TorchTrainer, TorchConfig

    trainer = TorchTrainer(
        train_func,
        torch_backend=TorchConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )

For more configurability, please reference the :py:class:`~ray.train.data_parallel_trainer.DataParallelTrainer` API.

Running your training function
------------------------------

With a distributed training function and a Ray Train ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()
