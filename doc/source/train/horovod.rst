Horovod
=======

Ray Train configures the Horovod environment and Rendezvous
server for you, allowing you to run your ``DistributedOptimizer`` training
script. See `Horovod documentation <https://horovod.readthedocs.io/en/stable/index.html>`_
for more information.

Quickstart
-----------
.. literalinclude:: /ray-air/doc_code/hvd_trainer.py
  :language: python



Updating your training function
-------------------------------

First, you'll want to update your training function to support distributed
training.

If you have a training function that already runs with the `Horovod Ray
Executor <https://horovod.readthedocs.io/en/stable/ray_include.html#horovod-ray-executor>`_,
you should not need to make any additional changes.

To onboard onto Horovod, please visit the `Horovod guide
<https://horovod.readthedocs.io/en/stable/index.html#get-started>`_.


Creating a :class:`~ray.train.horovod.HorovodTrainer`
-----------------------------------------------------

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. For Horovod, we use a :class:`~ray.train.horovod.HorovodTrainer`
that you can setup like this:

.. code-block:: python

    from ray.train import ScalingConfig
    from ray.train.horovod import HorovodTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = HorovodTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )

When training with Horovod, we will always use a HorovodTrainer,
irrespective of the training framework (e.g. PyTorch or Tensorflow).

To customize the backend setup, you can pass a
:class:`~ray.train.horovod.HorovodConfig`:

.. code-block:: python

    from ray.train import ScalingConfig
    from ray.train.horovod import HorovodTrainer, HorovodConfig

    trainer = HorovodTrainer(
        train_func,
        tensorflow_backend=HorovodConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )

For more configurability, please reference the :py:class:`~ray.train.data_parallel_trainer.DataParallelTrainer` API.

Running your training function
------------------------------

With a distributed training function and a Ray Train ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()


Further reading
---------------
Ray Train's :class:`~ray.train.horovod.HorovodTrainer` replaces the distributed
communication backend of the native libraries with its own implementation.
Thus, the remaining integration points remain the same. If you're using Horovod
with :ref:`PyTorch <train-pytorch>` or :ref:`Tensorflow <train-tensorflow-overview>`,
refer to the respective guides for further configuration
and information.

If you are implementing your own Horovod-based training routine without using any of
the training libraries, we still encourage you to read through the
:ref:`User Guides <train-user-guides>`, as many of the contents are applicable
to generic use cases and can be easily adapted.
