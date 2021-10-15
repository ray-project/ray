.. _sgd-v2-examples:

RaySGD Examples
===============

.. Example .rst files should be organized in the same manner as the
   .py files in ray/python/ray/util/sgd/v2/examples.

Below are examples for using RaySGD with a variety of models, frameworks, and use cases.

General Examples
----------------

PyTorch
~~~~~~~

* :doc:`/raysgd/v2/examples/train_linear_example`:
  Simple example for PyTorch.

* :doc:`/raysgd/v2/examples/train_fashion_mnist_example`:
  End-to-end example for PyTorch.

* :doc:`/raysgd/v2/examples/transformers/transformers_example`:
  End-to-end example for HuggingFace Transformers (PyTorch).

TensorFlow
~~~~~~~~~~

* :doc:`/raysgd/v2/examples/tensorflow_mnist_example`:
  End-to-end example for TensorFlow

Horovod
~~~~~~~~~~

* :doc:`/raysgd/v2/examples/horovod/horovod_example`:
  End-to-end example for Horovod (with PyTorch)


..
  TODO

  * :doc:`/raysgd/v2/examples/TODO`:
  Simple example for TensorFlow

  * :doc:`/raysgd/v2/examples/TODO`:
  Simple example for Horovod (with TensorFlow)


Iterator API Examples
---------------------

* :doc:`/raysgd/v2/examples/mlflow_fashion_mnist_example`:
  Example for using the Iterator API for custom MLFlow integration.

Ray Datasets Integration Examples
---------------------------------

* :doc:`/raysgd/v2/examples/tensorflow_linear_dataset_example`:
  Simple example for training a linear TensorFlow model.

* :doc:`/raysgd/v2/examples/train_linear_dataset_example`:
  Simple example for training a linear PyTorch model.

* :doc:`/raysgd/v2/examples/tune_linear_dataset_example`:
  Simple example for tuning a linear PyTorch model.

Ray Tune Integration Examples
-----------------------------

* :doc:`/raysgd/v2/examples/tune_linear_example`:
  Simple example for tuning a PyTorch model.

* :doc:`/raysgd/v2/examples/tune_tensorflow_mnist_example`:
  End-to-end example for tuning a TensorFlow model.

* :doc:`/raysgd/v2/examples/tune_cifar_pytorch_pbt_example`:
  End-to-end example for tuning a PyTorch model with PBT.

..
    TODO implement these examples!

    Features
    --------

    * Example for using a custom callback
    * End-to-end example for running on an elastic cluster (elastic training)

    Models
    ------

    * Example training on Vision model.
