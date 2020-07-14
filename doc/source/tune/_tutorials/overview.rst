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
   :tooltip: Tune User Guide
   :figure: /images/tune.png
   :description: :doc:`Tune User Guide <tune-usage>`

.. customgalleryitem::
   :tooltip: A simple guide to Population-based Training
   :figure: /images/tune-pbt-small.png
   :description: :doc:`A simple guide to Population-based Training <tune-advanced-tutorial>`

.. customgalleryitem::
   :tooltip: A guide to distributed hyperparameter tuning
   :figure: /images/tune.png
   :description: :doc:`A guide to distributed hyperparameter tuning <tune-distributed>`

.. customgalleryitem::
   :tooltip: Tune's Scikit-Learn Adapters
   :figure: /images/tune-sklearn.png
   :description: :doc:`Tune's Scikit-Learn Adapters <tune-sklearn>`

.. customgalleryitem::
   :tooltip: Tuning PyTorch Lightning modules
   :figure: /images/pytorch_lightning_small.png
   :description: :doc:`Tuning PyTorch Lightning modules <tune-pytorch-lightning>`

.. customgalleryitem::
   :tooltip: Tuning XGBoost parameters.
   :figure: /images/xgboost_logo.png
   :description: :doc:`A guide to tuning XGBoost parameters with Tune <tune-xgboost>`


.. raw:: html

    </div>

.. toctree::
   :hidden:

   tune-usage.rst
   tune-advanced-tutorial.rst
   tune-distributed.rst
   tune-sklearn.rst
   tune-pytorch-lightning.rst
   tune-xgboost.rst

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
