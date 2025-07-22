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


    def train_func():
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


    def train_func():
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


Setting the GPU type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Train allows you to specify the accelerator type for each worker.
This is useful if you want to use a specific accelerator type for model training.
In a heterogeneous Ray cluster, this means that your training workers will be forced to run on the specified GPU type,
rather than on any arbitrary GPU node. You can get a list of supported `accelerator_type` from
:ref:`the available accelerator types <accelerator_types>`.

For example, you can specify `accelerator_type="A100"` in the :class:`~ray.train.ScalingConfig` if you want to
assign each worker a NVIDIA A100 GPU.

.. tip::
    Ensure that your cluster has instances with the specified accelerator type
    or is able to autoscale to fulfill the request.

.. testcode::

    ScalingConfig(
        num_workers=1,
        use_gpu=True,
        accelerator_type="A100"
    )


(PyTorch) Setting the communication backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PyTorch Distributed supports multiple `backends <https://pytorch.org/docs/stable/distributed.html#backends>`__
for communicating tensors across workers. By default Ray Train will use NCCL when ``use_gpu=True`` and Gloo otherwise.

If you explicitly want to override this setting, you can configure a :class:`~ray.train.torch.TorchConfig`
and pass it into the :class:`~ray.train.torch.TorchTrainer`.

.. testcode::
    :hide:

    num_training_workers = 1

.. testcode::

    from ray.train.torch import TorchConfig, TorchTrainer

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(
            num_workers=num_training_workers,
            use_gpu=True, # Defaults to NCCL
        ),
        torch_config=TorchConfig(backend="gloo"),
    )

(NCCL) Setting the communication network interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using NCCL for distributed training, you can configure the network interface cards
that are used for communicating between GPUs by setting the
`NCCL_SOCKET_IFNAME <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/env.html#nccl-socket-ifname>`__
environment variable.

To ensure that the environment variable is set for all training workers, you can pass it
in a :ref:`Ray runtime environment <runtime-environments>`:

.. testcode::
    :skipif: True

    import ray

    runtime_env = {"env_vars": {"NCCL_SOCKET_IFNAME": "ens5"}}
    ray.init(runtime_env=runtime_env)

    trainer = TorchTrainer(...)

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



(Deprecated) Trainer resources
------------------------------

.. important::
    This API is deprecated. See `this migration guide <https://github.com/ray-project/ray/issues/49454>`_ for more details.


So far we've configured resources for each training worker. Technically, each
training worker is a :ref:`Ray Actor <actor-guide>`. Ray Train also schedules
an actor for the trainer object when you call ``trainer.fit()``.

This object often only manages lightweight communication between the training workers.
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
