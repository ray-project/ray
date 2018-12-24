Tune Examples
=============

.. Keep this in sync with ray/doc/tune-examples.rst

In our repository, we provide a variety of examples for the various use cases and features of Tune.

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.


General Examples
----------------

- `async_hyperband_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/async_hyperband_example.py>`__:
   Example of using a Trainable class with AsyncHyperBandScheduler.
- `hyperband_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__:
   Example of using a Trainable class with HyperBandScheduler. Also uses the Experiment class API for specifying the experiment configuration.
- `hyperopt_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__:
   Optimizes a basic function using the function-based API and the HyperOptSearch (SearchAlgorithm wrapper for HyperOpt TPE).
   Also uses the AsyncHyperBandScheduler.
- `pbt_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_example.py>`__:
   Example of using a Trainable class with PopulationBasedTraining scheduler.
- `pbt_ppo_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_ppo_example.py>`__:
   Example of optimizing a distributed RLlib algorithm (PPO) with the PopulationBasedTraining scheduler.
- `logging_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py>`__:
   Example of custom loggers and custom trial directory naming.


Keras Examples
--------------

- `tune_mnist_keras <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_keras.py>`__:
   Converts the Keras MNIST example to use Tune with the function-based API and a Keras callback. Also shows how to easily convert something relying on argparse to use Tune.


PyTorch Examples
----------------

- `mnist_pytorch <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch.py>`__:
   Converts the PyTorch MNIST example to use Tune with the function-based API. Also shows how to easily convert something relying on argparse to use Tune.
- `mnist_pytorch_trainable <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch_trainable.py>`__:
   Converts the PyTorch MNIST example to use Tune with Trainable API. Also uses the HyperBandScheduler and checkpoints the model at the end.


TensorFlow Examples
-------------------

- `tune_mnist_ray <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_ray.py>`__:
   A basic example of tuning a TensorFlow model on MNIST using the Trainable class.
- `tune_mnist_ray_hyperband <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_ray_hyperband.py>`__:
   A basic example of tuning a TensorFlow model on MNIST using the Trainable class and the HyperBand scheduler.
- `tune_mnist_async_hyperband <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_async_hyperband.py>`__:
   Example of tuning a TensorFlow model on MNIST using AsyncHyperBand.


Contributed Examples
--------------------

- `pbt_tune_cifar10_with_keras <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_tune_cifar10_with_keras.py>`__:
   A contributed example of tuning a Keras model on CIFAR10 with the PopulationBasedTraining scheduler.
- `genetic_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/genetic_example.py>`__:
   Optimizing the michalewicz function using the contributed GeneticSearch search algorithm with AsyncHyperBandScheduler.
