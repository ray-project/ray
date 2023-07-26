Horovod
=======

Ray integrates with Horovod as a distributed communication backend.

Ray Train configures the Horovod environment and Rendezvous server for you,
allowing you to run your ``DistributedOptimizer`` training script.

.. _train-porting-code-horovod:

Porting code from an existing training loop
-------------------------------------------

The following instructions assume you have a training function
that can already be run on a single worker.

Updating your training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you'll want to update your training function to support distributed
training.


If you have a training function that already runs with the `Horovod Ray
Executor <https://horovod.readthedocs.io/en/stable/ray_include.html#horovod-ray-executor>`_,
you should not need to make any additional changes!

To onboard onto Horovod, please visit the `Horovod guide
<https://horovod.readthedocs.io/en/stable/index.html#get-started>`_.

Creating a HorovodTrainer
~~~~~~~~~~~~~~~~~~~~~~~~~

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. You can create a ``HorovodTrainer`` like this:


.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.horovod import HorovodTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = HorovodTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )

To customize the backend setup, you can specify a :class:`HorovodConfig <ray.train.horovod.HorovodConfig>`:


.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.horovod import HorovodTrainer, HorovodConfig

    trainer = HorovodTrainer(
        train_func,
        tensorflow_backend=HorovodConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )

Running your training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With a distributed training function and the ``HorovodTrainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()
