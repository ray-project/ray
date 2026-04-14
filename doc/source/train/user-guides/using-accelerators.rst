.. _train_scaling_config:

Configuring scale and accelerators
==================================
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

Using accelerators
------------------

.. tab-set::

    .. tab-item:: GPU

        To use GPUs, pass ``use_gpu=True`` to the :class:`~ray.train.ScalingConfig`.
        This requests one GPU per training worker. In the following example, training
        runs on 8 GPUs (8 workers, each using one GPU).

        .. testcode::

            from ray.train import ScalingConfig

            scaling_config = ScalingConfig(
                num_workers=8,
                use_gpu=True
            )

    .. tab-item:: TPU

        To use TPUs, pass ``use_tpu=True`` to the :class:`~ray.train.ScalingConfig`.
        You also need to specify ``topology`` and ``accelerator_type``.

        Set ``num_workers`` to the number of TPU VM hosts in the slice, or a
        multiple of it for multi-slice training. For example, a ``v6e`` TPU
        slice with a ``4x4`` topology has 4 VMs, so you'd set ``num_workers=4``
        for a single slice, or ``num_workers=8`` for two slices.

        For details on how TPU topologies map to the number of VMs, see
        `Plan TPUs in GKE <https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus>`_.

        .. testcode::
            :skipif: True

            from ray.train import ScalingConfig

            # Single slice: 4 v6e VMs in a 4x4 topology
            scaling_config = ScalingConfig(
                num_workers=4,
                use_tpu=True,
                topology="4x4",
                accelerator_type="TPU-V6E",
            )

            # Multi-slice: 2 v6e slices, 8 VMs total
            scaling_config = ScalingConfig(
                num_workers=8,
                use_tpu=True,
                topology="4x4",
                accelerator_type="TPU-V6E",
            )


Using accelerators in the training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: GPU

        When ``use_gpu=True`` is set, Ray Train automatically sets up environment variables
        in your training function so that the GPUs can be detected and used
        (such as ``CUDA_VISIBLE_DEVICES``).

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

    .. tab-item:: TPU

        When ``use_tpu=True`` is set, Ray Train configures the distributed
        environment for TPU execution on each worker. The specific initialization
        depends on the trainer you use (such as :class:`~ray.train.v2.jax.JaxTrainer`).

        .. note::

            If you're using JAX, import ``jax`` inside your ``train_loop_per_worker``
            function rather than at the module level. This avoids driver-side TPU
            lock issues.

        The following example shows a basic TPU training setup with
        :class:`~ray.train.v2.jax.JaxTrainer`:

        .. testcode::
            :skipif: True

            import ray.train
            from ray.train import ScalingConfig
            from ray.train.v2.jax import JaxTrainer


            def train_func():
                import jax
                devices = jax.devices()
                ray.train.report({"num_devices": len(devices)})

            trainer = JaxTrainer(
                train_func,
                scaling_config=ScalingConfig(
                    num_workers=4,
                    use_tpu=True,
                    topology="4x4",
                    accelerator_type="TPU-V6E",
                )
            )
            trainer.fit()


Assigning multiple accelerators to a worker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: GPU

        Sometimes you might want to allocate multiple GPUs for a worker. For example,
        you can specify ``resources_per_worker={"GPU": 2}`` in the ``ScalingConfig`` if you want to
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

    .. tab-item:: TPU

        Each TPU VM host has multiple TPU chips. You can allocate all chips on a
        host to a single worker by specifying the number of chips in
        ``resources_per_worker``. For example, a ``ct6e-standard-4t`` machine has
        4 chips per VM.

        .. testcode::
            :skipif: True

            from ray.train import ScalingConfig

            scaling_config = ScalingConfig(
                num_workers=4,
                use_tpu=True,
                topology="4x4",
                accelerator_type="TPU-V6E",
                resources_per_worker={"TPU": 4},
            )

        When ``topology`` and ``accelerator_type`` are specified, Ray Train
        can auto-detect the correct ``resources_per_worker`` for the given
        TPU slice configuration.


Setting the accelerator type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Train allows you to specify the accelerator type for each worker.
This is useful if you want to use a specific accelerator type for model training.
In a heterogeneous Ray cluster, this means that your training workers are forced to run on the specified accelerator type,
rather than on any arbitrary accelerator node. You can get a list of supported ``accelerator_type`` from
:ref:`the available accelerator types <accelerator_types>`.

.. tab-set::

    .. tab-item:: GPU

        The following example specifies ``accelerator_type="A100"`` to assign each worker
        a NVIDIA A100 GPU.

        .. tip::
            Ensure that your cluster has instances with the specified accelerator type
            or is able to autoscale to fulfill the request.

        .. testcode::

            ScalingConfig(
                num_workers=1,
                use_gpu=True,
                accelerator_type="A100"
            )

    .. tab-item:: TPU

        For TPUs, ``accelerator_type`` specifies the TPU generation. The
        supported values are: ``"TPU-V2"``, ``"TPU-V3"``, ``"TPU-V4"``,
        ``"TPU-V5P"``, ``"TPU-V5LITEPOD"``, ``"TPU-V6E"``, and ``"TPU-V7X"``.

        .. testcode::
            :skipif: True

            ScalingConfig(
                num_workers=4,
                use_tpu=True,
                topology="2x2x4",
                accelerator_type="TPU-V4",
            )


(PyTorch) Setting the communication backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PyTorch Distributed supports multiple `backends <https://pytorch.org/docs/stable/distributed.html#backends>`__
for communicating tensors across workers. By default Ray Train uses NCCL when ``use_gpu=True`` and Gloo otherwise.

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
If you want to allocate more than one CPU or accelerator per training worker, or if you
defined :ref:`custom cluster resources <cluster-resources>`, set
the ``resources_per_worker`` attribute:

.. tab-set::

    .. tab-item:: GPU

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
        are assigned the same CUDA device.

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

    .. tab-item:: TPU

        For TPU workers, specify the number of TPU chips per worker in
        ``resources_per_worker``. Each TPU VM host has a fixed number of chips
        depending on the machine type (such as 4 chips for ``ct6e-standard-4t``).
        Supported chip counts are 1, 2, 4, and 8.

        .. testcode::
            :skipif: True

            from ray.train import ScalingConfig

            # Allocate all 4 chips per host to each worker
            scaling_config = ScalingConfig(
                num_workers=4,
                resources_per_worker={
                    "CPU": 4,
                    "TPU": 4,
                },
                use_tpu=True,
                topology="4x4",
                accelerator_type="TPU-V6E",
            )

            # Allocate 2 of 4 chips per host to each worker
            scaling_config = ScalingConfig(
                num_workers=4,
                resources_per_worker={
                    "CPU": 4,
                    "TPU": 2,
                },
                use_tpu=True,
                topology="4x4",
                accelerator_type="TPU-V6E",
            )

        .. note::
            If you specify TPUs in ``resources_per_worker``, you also need to set
            ``use_tpu=True``.

        When ``topology`` and ``accelerator_type`` are provided, Ray Train
        can auto-detect the correct ``resources_per_worker`` for the TPU slice,
        so you can omit it:

        .. testcode::
            :skipif: True

            from ray.train import ScalingConfig

            scaling_config = ScalingConfig(
                num_workers=4,
                use_tpu=True,
                topology="4x4",
                accelerator_type="TPU-V6E",
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
to start 4 training workers a 2 CPUs, this won't work, as the total number
of required CPUs is 9 (4 * 2 + 1). In that case, you can specify the trainer
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
