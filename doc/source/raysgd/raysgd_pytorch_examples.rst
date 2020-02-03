.. _raysgd-pytorch-example:

RaySGD PyTorch Examples
=======================

Here are some examples of using RaySGD for training PyTorch models. If you'd like
to contribute an example, feel free to create a `pull request here <https://github.com/ray-project/ray/pull/>`_.


Toy Example
-----------

Below is an example of using Ray's PyTorchTrainer.


.. literalinclude:: ../../../python/ray/experimental/sgd/pytorch/examples/train_example.py
   :language: python
   :start-after: __torch_train_example__


CIFAR10 Example
---------------

Below is an example of training a ResNet18 model on CIFAR10. It uses a custom training
function, a custom validation function, and custom initialization code for each worker.

.. literalinclude:: ../../../python/ray/experimental/sgd/pytorch/examples/cifar_pytorch_example.py
   :language: python


DCGAN Example
-------------

Below is an example of training a Deep Convolutional GAN on MNIST. It constructs
two models and two optimizers and uses a custom training and validation function.

.. literalinclude:: ../../../python/ray/experimental/sgd/pytorch/examples/dcgan.py
   :language: python
