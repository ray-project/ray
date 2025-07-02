.. _tune-examples-ref:
.. _tune-recipes:

=================
Ray Tune Examples
=================

.. tip:: 
    See :ref:`overview` to learn more about Tune features.


Below are examples for using Ray Tune for a variety use cases and sorted by categories:

* `ML frameworks`_
* `Experiment tracking tools`_
* `Hyperparameter optimization frameworks`_
* `Others`_
* `Exercises`_

.. _ml-frameworks:

ML frameworks
-------------

.. toctree::
    :hidden:

    PyTorch Example <tune-pytorch-cifar>
    PyTorch Lightning Example <tune-pytorch-lightning>
    XGBoost Example <tune-xgboost>
    LightGBM Example <lightgbm_example>
    Hugging Face Transformers Example <pbt_transformers>
    Ray RLlib Example <pbt_ppo_example>
    Keras Example <tune_mnist_keras>
    Horovod Example <horovod_simple>

Ray Tune integrates with many popular machine learning frameworks. Here you find a few practical examples showing you how to tune your models. At the end of these guides you will often find links to even more examples.

.. list-table::

  * - :doc:`How to use Tune with Keras and TensorFlow models <tune_mnist_keras>`
  * - :doc:`How to use Tune with PyTorch models <tune-pytorch-cifar>`
  * - :doc:`How to tune PyTorch Lightning models <tune-pytorch-lightning>`
  * - :doc:`Tuning RL experiments with Ray Tune and Ray Serve <pbt_ppo_example>`
  * - :doc:`Tuning XGBoost parameters with Tune <tune-xgboost>`
  * - :doc:`Tuning LightGBM parameters with Tune <lightgbm_example>`
  * - :doc:`Tuning Horovod parameters with Tune <horovod_simple>`
  * - :doc:`Tuning Hugging Face Transformers with Tune <pbt_transformers>`
  * - :doc:`End-to-end example for tuning a TensorFlow model <../../train/examples/tf/tune_tensorflow_mnist_example>`
  * - :doc:`End-to-end example for tuning a PyTorch model with PBT <../../train/examples/pytorch/tune_cifar_torch_pbt_example>`  

.. _experiment-tracking-tools:

Experiment tracking tools
-------------------------

.. toctree::
    :hidden:

    Weights & Biases Example <tune-wandb>
    MLflow Example <tune-mlflow>
    Aim Example <tune-aim>
    Comet Example <tune-comet>

Ray Tune integrates with some popular Experiment tracking and management tools,
such as CometML, or Weights & Biases. For how
to use Ray Tune with Tensorboard, see
:ref:`Guide to logging and outputs <tune-logging>`.

.. list-table::

  * - :doc:`Using Aim with Ray Tune for experiment management <tune-aim>`
  * - :doc:`Using Comet with Ray Tune for experiment management <tune-comet>`
  * - :doc:`Tracking your experiment process Weights & Biases <tune-wandb>`
  * - :doc:`Using MLflow tracking and auto logging with Tune <tune-mlflow>`

.. _hyperparameter-optimization-frameworks:

Hyperparameter optimization frameworks
--------------------------------------

.. toctree::
    :hidden:

    Ax Example <ax_example>
    HyperOpt Example <hyperopt_example>
    Bayesopt Example <bayesopt_example>
    BOHB Example <bohb_example>
    Nevergrad Example <nevergrad_example>
    Optuna Example <optuna_example>

Tune integrates with a wide variety of hyperparameter optimization frameworks
and their respective search algorithms. See the following detailed examples
for each integration:

.. list-table::

  * - :doc:`ax_example`
  * - :doc:`hyperopt_example`
  * - :doc:`bayesopt_example`
  * - :doc:`bohb_example`
  * - :doc:`nevergrad_example`
  * - :doc:`optuna_example`

.. _tune-examples-others:

Others
------

.. list-table::

  * - :doc:`Simple example for doing a basic random and grid search <includes/tune_basic_example>`
  * - :doc:`Example of using a simple tuning function with AsyncHyperBandScheduler <includes/async_hyperband_example>`
  * - :doc:`Example of using a trainable function with HyperBandScheduler and the AsyncHyperBandScheduler <includes/hyperband_function_example>`
  * - :doc:`Configuring and running (synchronous) PBT and understanding the underlying algorithm behavior with a simple example <pbt_visualization/pbt_visualization>`
  * - :doc:`includes/pbt_function`
  * - :doc:`includes/pb2_example`
  * - :doc:`includes/logging_example`

.. _tune-examples-exercises:

Exercises
---------

Learn how to use Tune in your browser with the following Colab-based exercises.

.. list-table::
   :widths: 50 30 20
   :header-rows: 1

   * - Description
     - Library
     - Colab link
   * - Basics of using Tune
     - PyTorch
     - .. image:: https://colab.research.google.com/assets/colab-badge.svg
          :target: https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_1_basics.ipynb
          :alt: Open in Colab
   * - Using search algorithms and trial schedulers to optimize your model
     - PyTorch
     - .. image:: https://colab.research.google.com/assets/colab-badge.svg
          :target: https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_2_optimize.ipynb
          :alt: Open in Colab
   * - Using Population-Based Training (PBT)
     - PyTorch
     - .. image:: https://colab.research.google.com/assets/colab-badge.svg
          :target: https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_3_pbt.ipynb" target="_parent
          :alt: Open in Colab
   * - Fine-tuning Hugging Face Transformers with PBT
     - Hugging Face Transformers and PyTorch
     - .. image:: https://colab.research.google.com/assets/colab-badge.svg
          :target: https://colab.research.google.com/drive/1tQgAKgcKQzheoh503OzhS4N9NtfFgmjF?usp=sharing
          :alt: Open in Colab
   * - Logging Tune runs to Comet ML
     - Comet
     - .. image:: https://colab.research.google.com/assets/colab-badge.svg
          :target: https://colab.research.google.com/drive/1dp3VwVoAH1acn_kG7RuT62mICnOqxU1z?usp=sharing
          :alt: Open in Colab

Tutorial source files are on `GitHub <https://github.com/ray-project/tutorial>`_.
