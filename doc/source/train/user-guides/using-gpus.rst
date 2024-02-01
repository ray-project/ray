.. _train_scaling_config:

Configuring Scale and GPUs
==========================
Increasing the scale of a Ray Train training run is simple and can be done in a few lines of code.
The main interface for this is the :class:`~ray.train.ScalingConfig`, 
which configures the number of workers and the resources they should use.

In this guide, a *worker* refers to a Ray Train distributed training worker,
which is a :ref:`Ray Actor <actor-key-concept>` that runs your training function.

Increasing the number of workers
--------------------------------
The main interface to control parallelism in your training code is to set the
number of workers. This can be done by passing the ``num_workers`` attribute to
the :class:`~ray.train.ScalingConfig`:

.. testcode::

    from ray.train import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8
    )


Using GPUs
----------
To use GPUs, pass ``use_gpu=True`` to the :class:`~ray.train.ScalingConfig`.
This will request one GPU per training worker. In the example below, training will
run on 8 GPUs (8 workers, each using one GPU).

.. testcode::

    from ray.train import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        use_gpu=True
    )


Using GPUs in the training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
When ``use_gpu=True`` is set, Ray Train will automatically set up environment variables
in your training function so that the GPUs can be detected and used
(e.g. ``CUDA_VISIBLE_DEVICES``).

You can get the associated devices with :meth:`ray.train.torch.get_device`.

.. testcode::

    import torch
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer, get_device


    def train_func(config):
        assert torch.cuda.is_available()

        device = get_device()
        assert device == torch.device("cuda:0")

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=True
        )
    )
    trainer.fit()

Assigning multiple GPUs to a worker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sometimes you might want to allocate multiple GPUs for a worker. For example, 
you can specify `resources_per_worker={"GPU": 2}` in the `ScalingConfig` if you want to 
assign 2 GPUs for each worker.

You can get a list of associated devices with :meth:`ray.train.torch.get_devices`.

.. testcode::

    import torch
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer, get_device, get_devices


    def train_func(config):
        assert torch.cuda.is_available()

        device = get_device()
        devices = get_devices()
        assert device == torch.device("cuda:0")
        assert devices == [torch.device("cuda:0"), torch.device("cuda:1")]

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=True,
            resources_per_worker={"GPU": 2}
        )
    )
    trainer.fit()


Setting the resources per worker
--------------------------------
If you want to allocate more than one CPU or GPU per training worker, or if you
defined :ref:`custom cluster resources <cluster-resources>`, set
the ``resources_per_worker`` attribute:

.. testcode::

    from ray.train import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        resources_per_worker={
            "CPU": 4,
            "GPU": 2,
        },
        use_gpu=True,
    )


.. note::
    If you specify GPUs in ``resources_per_worker``, you also need to set
    ``use_gpu=True``.

You can also instruct Ray Train to use fractional GPUs. In that case, multiple workers
will be assigned the same CUDA device.

.. testcode::

    from ray.train import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        resources_per_worker={
            "CPU": 4,
            "GPU": 0.5,
        },
        use_gpu=True,
    )


Setting the communication backend (PyTorch)
-------------------------------------------

.. note::

    This is an advanced setting. In most cases, you don't have to change this setting.

You can set the PyTorch distributed communication backend (e.g. GLOO or NCCL) by passing a
:class:`~ray.train.torch.TorchConfig` to the :class:`~ray.train.torch.TorchTrainer`.

See the `PyTorch API reference <https://pytorch.org/docs/stable/distributed.html#torch.distributed.init_process_group>`__
for valid options.

.. testcode::
    :hide:

    num_training_workers = 1

.. testcode::

    from ray.train.torch import TorchConfig, TorchTrainer

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(
            num_workers=num_training_workers,
            use_gpu=True,
        ),
        torch_config=TorchConfig(backend="gloo"),
    )


.. _train_trainer_resources:

Trainer resources
-----------------
So far we've configured resources for each training worker. Technically, each
training worker is a :ref:`Ray Actor <actor-guide>`. Ray Train also schedules
an actor for the :class:`Trainer <ray.train.trainer.BaseTrainer>` object when
you call :meth:`Trainer.fit() <ray.train.trainer.BaseTrainer.fit>`.

This object often only manages lightweight communication between the training workers.
You can still specify its resources, which can be useful if you implemented your own
Trainer that does heavier processing.

.. testcode::

    from ray.train import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        trainer_resources={
            "CPU": 4,
            "GPU": 1,
        }
    )

Per default, a trainer uses 1 CPU. If you have a cluster with 8 CPUs and want
to start 4 training workers a 2 CPUs, this will not work, as the total number
of required CPUs will be 9 (4 * 2 + 1). In that case, you can specify the trainer
resources to use 0 CPUs:

.. testcode::

    from ray.train import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=4,
        resources_per_worker={
            "CPU": 2,
        },
        trainer_resources={
            "CPU": 0,
        }
    )
