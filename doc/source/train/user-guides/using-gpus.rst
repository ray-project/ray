.. _train_scaling_config:

Configuring Scale and GPUs
==========================
Increasing the scale of a Ray Train training run is simple and can often
be done in a few lines of code.

The main interface for configuring scale and resources
is the :class:`~ray.air.config.ScalingConfig`.

Scaling Configurations in Train (``ScalingConfig``)
---------------------------------------------------

The scaling configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the scaling configuration are :ref:`tunable <tune-search-space-tutorial>`.

.. literalinclude:: ../doc_code/key_concepts.py
    :language: python
    :start-after: __scaling_config_start__
    :end-before: __scaling_config_end__

.. seealso::

    See the :class:`~ray.air.ScalingConfig` API reference.


Increasing the number of workers
--------------------------------
The main interface to control parallelism in your training code is to set the
number of workers. This can be done by passing the ``num_workers`` attribute to
the :class:`~ray.air.config.ScalingConfig`:

.. code-block:: python

    from ray.air.config import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8
    )


Using GPUs
----------
To use GPUs, pass ``use_gpu=True`` to the :class:`~ray.air.config.ScalingConfig`.
This will request one GPU per training worker. In the example below, training will
run on 8 GPUs (8 workers, each using one GPU).

.. code-block:: python

    from ray.air.config import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        use_gpu=True
    )


More resources
--------------
If you want to allocate more than one CPU or GPU per training worker, or if you
defined :ref:`custom cluster resources <cluster-resources>`, set
the ``resources_per_worker`` attribute:

.. code-block:: python

    from ray.air.config import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        resources_per_worker={
            "CPU": 4,
            "GPU": 2,
        }
        use_gpu=True,
    )

Note that if you specify GPUs in ``resources_per_worker``, you also need to keep
``use_gpu=True``.

You can also instruct Ray Train to use fractional GPUs. In that case, multiple workers
will be assigned the same CUDA device.

.. code-block:: python

    from ray.air.config import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=8,
        resources_per_worker={
            "CPU": 4,
            "GPU": 0.5,
        }
        use_gpu=True,
    )

Using GPUs in training code
~~~~~~~~~~~~~~~~~~~~~~~~~~~
When ``use_gpu=True`` is set, Ray Train will automatically set up environment variables
in your training loop so that the GPUs can be detected and used
(e.g. ``CUDA_VISIBLE_DEVICES``).

You can get the associated devices with :meth:`ray.train.torch.get_device`.

.. code-block:: python

    import torch
    from ray.air.config import ScalingConfig
    from ray.train.torch import TorchTrainer, get_device


    def train_loop(config):
        assert torch.cuda.is_available()

        device = get_device()
        assert device == torch.device("cuda:0")


    trainer = TorchTrainer(
        train_loop,
        scaling_config=ScalingConfig(
            num_workers=1,
            use_gpu=True
        )
    )
    trainer.fit()


Trainer resources
-----------------
So far we've configured resources for each training worker. Technically, each
training worker is a :ref:`Ray Actor <actor-guide>`. Ray Train also schedules
an actor for the :class:`Trainer <ray.train.trainer.BaseTrainer>` object.

This object often only manages lightweight communication between the training workers.
You can still specify its resources, which can be useful if you implemented your own
Trainer that does heavier processing.

.. code-block:: python

    from ray.air.config import ScalingConfig

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

.. code-block:: python

    from ray.air.config import ScalingConfig

    scaling_config = ScalingConfig(
        num_workers=4,
        resources_per_worker={
            "CPU": 2,
        },
        trainer_resources={
            "CPU": 0,
        }
    )
