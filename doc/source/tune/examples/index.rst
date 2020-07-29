========
Examples
========

.. Keep this in sync with ray/python/ray/tune/examples/README.rst

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.

.. tip:: Check out :ref:`the Tune tutorials page <tune-guides>` for guides on how to use Tune with your preferred machine learning library.

.. _tune-general-examples:

General Examples
~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/async_hyperband_example`: Example of using a Trainable class with AsyncHyperBandScheduler.
- :doc:`/tune/examples/hyperband_example`: Example of using a Trainable class with HyperBandScheduler. Also uses the Experiment class API for specifying the experiment configuration. Also uses the AsyncHyperBandScheduler.
- :doc:`/tune/examples/pbt_example`: Example of using a Trainable class with PopulationBasedTraining scheduler.
- :doc:`/tune/examples/pbt_function`: Example of using the function API with a PopulationBasedTraining scheduler.
- :doc:`/tune/examples/pbt_ppo_example`: Example of optimizing a distributed RLlib algorithm (PPO) with the PopulationBasedTraining scheduler.
- :doc:`/tune/examples/logging_example`: Example of custom loggers and custom trial directory naming.

Search Algorithm Examples
~~~~~~~~~~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/ax_example`: Optimize a Hartmann function with `Ax <https://ax.dev>`_ with 4 parallel workers.
- :doc:`/tune/examples/hyperopt_example`: Optimizes a basic function using the function-based API and the HyperOptSearch (SearchAlgorithm wrapper for HyperOpt TPE).
- :doc:`/tune/examples/nevergrad_example`: Optimize a simple toy function with the gradient-free optimization package `Nevergrad <https://github.com/facebookresearch/nevergrad>`_ with 4 parallel workers.
- :doc:`/tune/examples/bayesopt_example`: Optimize a simple toy function using `Bayesian Optimization <https://github.com/fmfn/BayesianOptimization>`_ with 4 parallel workers.

Tensorflow/Keras Examples
~~~~~~~~~~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/tune_mnist_keras`: Converts the Keras MNIST example to use Tune with the function-based API and a Keras callback. Also shows how to easily convert something relying on argparse to use Tune.
- :doc:`/tune/examples/pbt_memnn_example`: Example of training a Memory NN on bAbI with Keras using PBT.
- :doc:`/tune/examples/tf_mnist_example`: Converts the Advanced TF2.0 MNIST example to use Tune with the Trainable. This uses `tf.function`. Original code from tensorflow: https://www.tensorflow.org/tutorials/quickstart/advanced


PyTorch Examples
~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/mnist_pytorch`: Converts the PyTorch MNIST example to use Tune with the function-based API. Also shows how to easily convert something relying on argparse to use Tune.
- :doc:`/tune/examples/mnist_pytorch_trainable`: Converts the PyTorch MNIST example to use Tune with Trainable API. Also uses the HyperBandScheduler and checkpoints the model at the end.


XGBoost Example
~~~~~~~~~~~~~~~

- :ref:`XGBoost tutorial <tune-xgboost>`: A guide to tuning XGBoost parameters with Tune.
- :doc:`/tune/examples/xgboost_example`: Trains a basic XGBoost model with Tune with the function-based API and an XGBoost callback.


LightGBM Example
~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/lightgbm_example`: Trains a basic LightGBM model with Tune with the function-based API and a LightGBM callback.


Contributed Examples
~~~~~~~~~~~~~~~~~~~~

- :doc:`/tune/examples/pbt_tune_cifar10_with_keras`: A contributed example of tuning a Keras model on CIFAR10 with the PopulationBasedTraining scheduler.
- :doc:`/tune/examples/genetic_example`: Optimizing the michalewicz function using the contributed GeneticSearch algorithm with AsyncHyperBandScheduler.
- :doc:`/tune/examples/tune_cifar10_gluon`: MXNet Gluon example to use Tune with the function-based API on CIFAR-10 dataset.

Open Source Projects using Tune
-------------------------------

Here are some of the popular open source repositories and research projects that leverage Tune. Feel free to submit a pull-request adding (or requesting a removal!) of a listed project.

 - `Softlearning <https://github.com/rail-berkeley/softlearning>`_: Softlearning is a reinforcement learning framework for training maximum entropy policies in continuous domains. Includes the official implementation of the Soft Actor-Critic algorithm.
 - `Flambe <https://github.com/asappresearch/flambe>`_: An ML framework to accelerate research and its path to production. See `flambe.ai <https://flambe.ai>`_.
 - `Population Based Augmentation <https://github.com/arcelien/pba>`_: Population Based Augmentation (PBA) is a algorithm that quickly and efficiently learns data augmentation functions for neural network training. PBA matches state-of-the-art results on CIFAR with one thousand times less compute.
 - `Fast AutoAugment by Kakao <https://github.com/kakaobrain/fast-autoaugment>`_: Fast AutoAugment (Accepted at NeurIPS 2019) learns augmentation policies using a more efficient search strategy based on density matching.
 - `Allentune <https://github.com/allenai/allentune>`_: Hyperparameter Search for AllenNLP from AllenAI.
 - `machinable <https://github.com/frthjf/machinable>`_: A modular configuration system for machine learning research. See `machinable.org <https://machinable.org>`_.
