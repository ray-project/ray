.. _train-examples:

Ray Train Examples
==================

.. Example .rst files should be organized in the same manner as the
   .py files in ray/python/ray/train/examples.

Below are examples for using Ray Train with a variety of models, frameworks, 
and use cases.

General Examples
----------------

PyTorch
~~~~~~~

* :doc:`/train/examples/train_linear_example`:
  Simple example for PyTorch.

* :doc:`/train/examples/train_fashion_mnist_example`:
  End-to-end example for PyTorch.

* :doc:`/train/examples/transformers/transformers_example`:
  End-to-end example for HuggingFace Transformers (PyTorch).

TensorFlow
~~~~~~~~~~

* :doc:`/train/examples/tensorflow_mnist_example`:
  End-to-end example for TensorFlow

Horovod
~~~~~~~

* :doc:`/train/examples/horovod/horovod_example`:
  End-to-end example for Horovod (with PyTorch)


..
  TODO

  * :doc:`/train/examples/TODO`:
  Simple example for TensorFlow

  * :doc:`/train/examples/TODO`:
  Simple example for Horovod (with TensorFlow)


Logger/Callback Examples
------------------------
* :doc:`/train/examples/mlflow_fashion_mnist_example`:
  Example for logging training to MLflow via the ``MLflowLoggerCallback``


Ray Datasets Integration Examples
---------------------------------

* :doc:`/train/examples/tensorflow_linear_dataset_example`:
  Simple example for training a linear TensorFlow model.

* :doc:`/train/examples/train_linear_dataset_example`:
  Simple example for training a linear PyTorch model.

* :doc:`/train/examples/tune_linear_dataset_example`:
  Simple example for tuning a linear PyTorch model.


Ray Tune Integration Examples
-----------------------------

* :doc:`/train/examples/tune_linear_example`:
  Simple example for tuning a PyTorch model.

* :doc:`/train/examples/tune_tensorflow_mnist_example`:
  End-to-end example for tuning a TensorFlow model.

* :doc:`/train/examples/tune_cifar_pytorch_pbt_example`:
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

Benchmarks
----------

* :doc:`/train/examples/torch_data_prefetch_benchmark/benchmark_example`:
  Benchmark example for the PyTorch data transfer auto pipeline.

