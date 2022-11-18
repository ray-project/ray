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

* :doc:`/train/examples/pytorch/torch_fashion_mnist_example`:
  End-to-end example for PyTorch.

* :doc:`/train/examples/transformers/transformers_example`:
  End-to-end example for HuggingFace Transformers (PyTorch).

TensorFlow
~~~~~~~~~~

* :doc:`/train/examples/tf/tensorflow_mnist_example`:
  End-to-end example for TensorFlow

Horovod
~~~~~~~

* :doc:`/train/examples/horovod/horovod_example`:
  End-to-end example for Horovod (with PyTorch)


Logger/Callback Examples
------------------------
* :doc:`/train/examples/mlflow_fashion_mnist_example`:
  Example for logging training to MLflow via the ``MLflowLoggerCallback``


Ray Tune Integration Examples
-----------------------------

* :doc:`/train/examples/tf/tune_tensorflow_mnist_example`:
  End-to-end example for tuning a TensorFlow model.

* :doc:`/train/examples/pytorch/tune_cifar_torch_pbt_example`:
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

* :doc:`/train/examples/pytorch/torch_data_prefetch_benchmark/benchmark_example`:
  Benchmark example for the PyTorch data transfer auto pipeline.

