.. _tune-guides-overview:

Tutorials, User Guides, Examples
================================

In this section, you can find material on how to use Tune and its various features. If any of the materials is out of date or broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.


Tutorials
---------

Take a look at any of the below tutorials to get started with Tune.

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. customgalleryitem::
   :tooltip: Tune concepts in 60 seconds.
   :figure: /images/tune-workflow.png
   :description: :doc:`Tune concepts in 60 seconds <tune-60-seconds>`

.. customgalleryitem::
   :tooltip: A simple Tune walkthrough.
   :figure: /images/tune.png
   :description: :doc:`A walkthrough to setup your first Tune experiment <tune-tutorial>`

.. raw:: html

    </div>

.. toctree::
   :hidden:

   tune-60-seconds.rst
   tune-tutorial.rst


User Guides
-----------

These pages will demonstrate the various features and configurations of Tune.

.. raw:: html

    <div class="sphx-glr-bigcontainer">

.. customgalleryitem::
   :tooltip: A guide to Tune features.
   :figure: /images/tune.png
   :description: :doc:`A guide to Tune features <tune-usage>`

.. customgalleryitem::
   :tooltip: A simple guide to Population-based Training
   :figure: /images/tune-pbt-small.png
   :description: :doc:`A simple guide to Population-based Training <tune-advanced-tutorial>`

.. customgalleryitem::
   :tooltip: A guide to distributed hyperparameter tuning
   :figure: /images/tune.png
   :description: :doc:`A guide to distributed hyperparameter tuning <tune-distributed>`

.. raw:: html

    </div>

.. toctree::
   :hidden:

   tune-usage.rst
   tune-advanced-tutorial.rst
   tune-distributed.rst

Colab Exercises
---------------

Learn how to use Tune in your browser with the following Colab-based exercises.

.. raw:: html

    <table>
      <tr>
        <th class="tune-colab">Exercise Description</th>
        <th class="tune-colab">Library</th>
        <th class="tune-colab">Colab Link</th>
      </tr>
      <tr>
        <td class="tune-colab">Basics of using Tune.</td>
        <td class="tune-colab">TF/Keras</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_1_basics.ipynb" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>

      <tr>
        <td class="tune-colab">Using Search algorithms and Trial Schedulers to optimize your model.</td>
        <td class="tune-colab">Pytorch</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_2_optimize.ipynb" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>

      <tr>
        <td class="tune-colab">Using Population-Based Training (PBT).</td>
        <td class="tune-colab">Pytorch</td>
        <td class="tune-colab">
          <a href="https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_3_pbt.ipynb" target="_parent">
          <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Tune Tutorial"/>
          </a>
        </td>
      </tr>
    </table>

Tutorial source files `can be found here <https://github.com/ray-project/tutorial>`_.

Tune Examples
-------------

.. Keep this in sync with ray/python/ray/tune/examples/README.rst

If any example is broken, or if you'd like to add an example to this page, feel free to raise an issue on our Github repository.

.. _tune-general-examples:

General Examples
~~~~~~~~~~~~~~~~

- `async_hyperband_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/async_hyperband_example.py>`__: Example of using a Trainable class with AsyncHyperBandScheduler.
- `hyperband_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__: Example of using a Trainable class with HyperBandScheduler. Also uses the Experiment class API for specifying the experiment configuration. Also uses the AsyncHyperBandScheduler.
- `pbt_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_example.py>`__: Example of using a Trainable class with PopulationBasedTraining scheduler.
- `pbt_ppo_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_ppo_example.py>`__: Example of optimizing a distributed RLlib algorithm (PPO) with the PopulationBasedTraining scheduler.
- `logging_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/logging_example.py>`__: Example of custom loggers and custom trial directory naming.

Search Algorithm Examples
~~~~~~~~~~~~~~~~~~~~~~~~~

- `Ax example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/ax_example.py>`__: Optimize a Hartmann function with `Ax <https://ax.dev>`_ with 4 parallel workers.
- `HyperOpt Example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__: Optimizes a basic function using the function-based API and the HyperOptSearch (SearchAlgorithm wrapper for HyperOpt TPE).
- `Nevergrad example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/nevergrad_example.py>`__: Optimize a simple toy function with the gradient-free optimization package `Nevergrad <https://github.com/facebookresearch/nevergrad>`_ with 4 parallel workers.
- `Bayesian Optimization example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bayesopt_example.py>`__: Optimize a simple toy function using `Bayesian Optimization <https://github.com/fmfn/BayesianOptimization>`_ with 4 parallel workers.

Tensorflow/Keras Examples
~~~~~~~~~~~~~~~~~~~~~~~~~

- `tune_mnist_keras <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_mnist_keras.py>`__: Converts the Keras MNIST example to use Tune with the function-based API and a Keras callback. Also shows how to easily convert something relying on argparse to use Tune.
- `pbt_memnn_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_memnn_example.py>`__: Example of training a Memory NN on bAbI with Keras using PBT.
- `Tensorflow 2 Example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tf_mnist_example.py>`__: Converts the Advanced TF2.0 MNIST example to use Tune with the Trainable. This uses `tf.function`. Original code from tensorflow: https://www.tensorflow.org/tutorials/quickstart/advanced


PyTorch Examples
~~~~~~~~~~~~~~~~

- `mnist_pytorch <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch.py>`__: Converts the PyTorch MNIST example to use Tune with the function-based API. Also shows how to easily convert something relying on argparse to use Tune.
- `mnist_pytorch_trainable <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/mnist_pytorch_trainable.py>`__: Converts the PyTorch MNIST example to use Tune with Trainable API. Also uses the HyperBandScheduler and checkpoints the model at the end.


XGBoost Example
~~~~~~~~~~~~~~~

- `xgboost_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/xgboost_example.py>`__: Trains a basic XGBoost model with Tune with the function-based API and an XGBoost callback.


LightGBM Example
~~~~~~~~~~~~~~~~

- `lightgbm_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/lightgbm_example.py>`__: Trains a basic LightGBM model with Tune with the function-based API and a LightGBM callback.


Contributed Examples
~~~~~~~~~~~~~~~~~~~~

- `pbt_tune_cifar10_with_keras <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/pbt_tune_cifar10_with_keras.py>`__: A contributed example of tuning a Keras model on CIFAR10 with the PopulationBasedTraining scheduler.
- `genetic_example <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/genetic_example.py>`__: Optimizing the michalewicz function using the contributed GeneticSearch algorithm with AsyncHyperBandScheduler.
- `tune_cifar10_gluon <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/tune_cifar10_gluon.py>`__: MXNet Gluon example to use Tune with the function-based API on CIFAR-10 dataset.
