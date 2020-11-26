Pytorch Lightning with RaySGD
==============================
.. image:: /images/sgd_ptl.png
  :align: center
  :scale: 50 %


RaySGD includes an integration with Pytorch Lightning's `LightningModule <https://pytorch-lightning.readthedocs.io/en/latest/lightning_module.html>`_.
Easily take your existing ``LightningModule``, and use it with Ray SGD's ``TorchTrainer`` to take advantage of all of Ray SGD's distributed training features with minimal code changes.

.. tip:: This LightningModule integration is currently under active development. If you encounter any bugs, please raise an issue on `Github <https://github.com/ray-project/ray/issues>`_!

.. note:: Not all Pytorch Lightning features are supported. A full list of unsupported model hooks is listed down :ref:`below <ptl-unsupported-features>`. Please post any feature requests on `Github <https://github.com/ray-project/ray/issues>`_ and we will get to it shortly!

.. contents::
    :local:

Quick Start
-----------
Step 1: Define your ``LightningModule`` just like how you would with Pytorch Lightning.

.. code-block:: python

    from pytorch_lightning.core.lightning import LightningModule

    class MyLightningModule(LightningModule):
        ...

Step 2: Use the ``TrainingOperator.from_ptl`` method to convert the ``LightningModule`` to a Ray SGD compatible ``LightningOperator``.

.. code-block:: python

    from ray.util.sgd.torch import TrainingOperator

    MyLightningOperator = TrainingOperator.from_ptl(MyLightningModule)

Step 3: Use the Operator with Ray SGD's ``TorchTrainer``, just like how you would normally. See :ref:`torch-guide` for a more full guide on ``TorchTrainer``.

.. code-block:: python

    import ray
    from ray.util.sgd.torch import TorchTrainer

    ray.init()
    trainer = TorchTrainer(training_operator_cls=MyLightningOperator, num_workers=4, use_gpu=True)
    train_stats = trainer.train()

And that's it! For a more comprehensive guide, see the MNIST tutorial :ref:`below <ptl-mnist>`.

.. _ptl-mnist:

MNIST Tutorial
--------------
In this walkthrough we will go through how to train an MNIST classifier with Pytorch Lightning's ``LightningModule`` and Ray SGD.

We will follow `this tutorial from the PyTorch Lightning documentation
<https://pytorch-lightning.readthedocs.io/en/latest/introduction_guide.html>`_ for specifying our MNIST LightningModule.

Setup / Imports
~~~~~~~~~~~~~~~
Let's start with some basic imports:

.. literalinclude:: /../../python/ray/util/sgd/torch/examples/pytorch-lightning/mnist-ptl.py
   :language: python
   :start-after: __import_begin__
   :end-before: __import_end__

Most of these imports are needed for building our Pytorch model and training components.
Only a few additional imports are needed for Ray and Pytorch Lightning.

MNIST LightningModule
~~~~~~~~~~~~~~~~~~~~~
We now define our Pytorch Lightning ``LightningModule``:

.. literalinclude:: /../../python/ray/util/sgd/torch/examples/pytorch-lightning/mnist-ptl.py
   :language: python
   :start-after: __ptl_begin__
   :end-before: __ptl_end__

This is the same code that would normally be used in Pytorch Lightning, and is taken directly from `this PTL guide <https://pytorch-lightning.readthedocs.io/en/latest/introduction_guide.html>`_.
The only difference here is that the ``__init__`` method can optionally take in a ``config`` argument,
as a way to pass in hyperparameters to your model, optimizer, or schedulers. The ``config`` will be passed in directly from
the TorchTrainer. Or if using Ray SGD in conjunction with Tune (:ref:`raysgd-tune`), it will come directly from the config in your
``tune.run`` call.

Training with Ray SGD
~~~~~~~~~~~~~~~~~~~~~
We now can define our training function using our LitMNIST module and Ray SGD.

.. literalinclude:: /../../python/ray/util/sgd/torch/examples/pytorch-lightning/mnist-ptl.py
   :language: python
   :start-after: __train_begin__
   :end-before: __train_end__

With just a single ``from_ptl`` call, we can convert our LightningModule to a ``TrainingOperator`` class that's compatible
with Ray SGD. Now we can take full advantage of all of Ray SGD's distributed trainign features without having to rewrite our existing
LightningModule.

The last thing to do is initialize Ray, and run our training function!

.. code-block:: python

    # Use ray.init(address="auto") if running on a Ray cluster.
    ray.init()
    train_mnist(num_workers=32, use_gpu=True, num_epochs=5)

.. _ptl-unsupported-features:

Unsupported Features
--------------------
This integration is currently under active development, so not all Pytorch Lightning features are supported.
Please post any feature requests on `Github
<https://github.com/ray-project/ray/issues>`_ and we will get to it shortly!

A list of unsupported model hooks (as of v1.0.0) is as follows:
``test_dataloader``, ``on_test_batch_start``, ``on_test_epoch_start``, ``on_test_batch_end``, ``on_test_epoch_start``,
``get_progress_bar_dict``, ``on_fit_end``, ``on_pretrain_routine_end``, ``manual_backward``, ``tbtt_split_batch``.
