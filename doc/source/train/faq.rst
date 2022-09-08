.. _train-faq:

Ray Train FAQ
=============

How fast is Ray Train compared to PyTorch, TensorFlow, etc.?
------------------------------------------------------------

At its core, training speed should be the same - while Ray Train launches distributed training workers via Ray Actors,
communication during training (e.g. gradient synchronization) is handled by the backend training framework itself.

For example, when running Ray Train with the ``TorchTrainer``,
distributed training communication is done with Torch's ``DistributedDataParallel``.

Take a look at the :ref:`Pytorch <pytorch-training-parity>` and :ref:`Tensorflow <tf-training-parity>` benchmarks to check performance parity.

How do I set resources?
-----------------------

By default, each worker will reserve 1 CPU resource, and an additional 1 GPU resource if ``use_gpu=True``.

To override these resource requests or request additional custom resources,
you can initialize the ``Trainer`` with ``resources_per_worker`` specified in ``scaling_config``.

.. note::
   Some GPU utility functions (e.g. :func:`ray.train.torch.get_device`, :func:`ray.train.torch.prepare_model`)
   currently assume each worker is allocated exactly 1 GPU. The partial GPU and multi GPU use-cases
   can still be run with Ray Train today without these functions.
