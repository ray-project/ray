RaySGD Examples
===============

Here are some examples of using RaySGD for


PyTorchTrainer Example
----------------------

Below is an example of using Ray's PyTorchTrainer. Under the hood, ``PytorchTrainer`` will create *replicas* of your model (controlled by ``num_replicas``) which are each managed by a worker.

.. literalinclude:: ../../../python/ray/experimental/sgd/examples/train_example.py
   :language: python
   :start-after: __torch_train_example__
