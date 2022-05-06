.. _train-faq:

Ray Train FAQ
=============

How fast is Ray Train compared to PyTorch, TensorFlow, etc.?
------------------------------------------------------------

At its core, training speed should be the same - while Ray Train launches distributed training workers via Ray Actors,
communication during training (e.g. gradient synchronization) is handled by the backend training framework itself.

For example, when running Ray Train with the ``"torch"`` backend,
distributed training communication is done with Torch's ``DistributedDataParallel``.

How do I set resources?
-----------------------

By default, each worker will reserve 1 CPU resource, and an additional 1 GPU resource if ``use_gpu=True``.

To override these resource requests or request additional custom resources,
you can initialize the ``Trainer`` with ``resources_per_worker``.

.. note::
   Some GPU utility functions (e.g. :ref:`train-api-torch-get-device`, :ref:`train-api-torch-prepare-model`)
   currently assume each worker is allocated exactly 1 GPU. The partial GPU and multi GPU use-cases
   can still be run with Ray Train today without these functions.

How can I use Matplotlib with Ray Train?
-----------------------------------------

If you try to create a Matplotlib plot in the training function, you may encounter an error:

.. code-block::

    UserWarning: Starting a Matplotlib GUI outside of the main thread will likely fail.

To handle this, consider the following approaches:

1. If there is no dependency on any code in your training function, simply move the Matplotlib logic out and execute it before or after ``trainer.run``.
2. If you are plotting metrics, you can pass the metrics via ``train.report()`` and create a :ref:`custom callback <train-custom-callbacks>` to plot the results.
