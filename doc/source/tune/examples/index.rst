.. _tune-examples-ref:
.. _tune-recipes:

=================
Ray Tune Examples
=================

.. toctree::
    :hidden:

    ml-frameworks
    experiment-tracking
    hpo-frameworks
    Other Examples <other-examples>
    Exercises <exercises>


.. tip:: See :ref:`overview` to learn more about Tune features.


Below are examples for using Ray Tune for a variety use cases.

ML frameworks
-------------

Ray Tune integrates with many popular machine learning frameworks. Here you find a few practical examples showing you how to tune your models. At the end of these guides you will often find links to even more examples.

.. list-table::

  * - :doc:`How to use Tune with Keras and TensorFlow models <tune_mnist_keras>`
  * - :doc:`How to use Tune with PyTorch models <tune-pytorch-cifar>`
  * - :doc:`How to tune PyTorch Lightning models <tune-pytorch-lightning>`
  * - :doc:`Tuning RL experiments with Ray Tune and Ray Serve <pbt_ppo_example>`
  * - :doc:`A guide to tuning XGBoost parameters with Tune <tune-xgboost>`
  * - :doc:`A guide to tuning LightGBM parameters with Tune <lightgbm_example>`
  * - :doc:`A guide to tuning Horovod parameters with Tune <horovod_simple>`
  * - :doc:`A guide to tuning Hugging Face Transformers with Tune <pbt_transformers>`
  * - :doc:`End-to-end example for tuning a TensorFlow model <../../train/examples/tf/tune_tensorflow_mnist_example>`
  * - :doc:`End-to-end example for tuning a PyTorch model with PBT <../../train/examples/pytorch/tune_cifar_torch_pbt_example>`  


Experiment tracking
-------------------

Ray Tune integrates with some popular Experiment tracking and management tools,
such as CometML, or Weights & Biases. If you're interested in learning how
to use Ray Tune with Tensorboard, you can find more information in our
:ref:`Guide to logging and outputs <tune-logging>`.

.. list-table::

  * - :doc:`Using Aim with Ray Tune for experiment management <tune-aim>`
  * - :doc:`Using Comet with Ray Tune for experiment management <tune-comet>`
  * - :doc:`Tracking your experiment process Weights & Biases <tune-wandb>`
  * - :doc:`Using MLflow tracking and auto logging with Tune <tune-mlflow>`


Hyperparameter optimization frameworks
--------------------------------------

Tune integrates with a wide variety of hyperparameter optimization frameworks
and their respective search algorithms. Here you can find detailed examples
on each of our integrations:

.. list-table::

  * - :doc:`ax_example`
  * - :doc:`hyperopt_example`
  * - :doc:`bayesopt_example`
  * - :doc:`bohb_example`
  * - :doc:`nevergrad_example`
  * - :doc:`optuna_example`


Others
------

.. list-table::

  * - :doc:`Simple example for doing a basic random and grid search <includes/tune_basic_example>`
  * - :doc:`Example of using a simple tuning function with AsyncHyperBandScheduler <includes/async_hyperband_example>`
  * - :doc:`Example of using a trainable function with HyperBandScheduler and the AsyncHyperBandScheduler <includes/hyperband_function_example>`
  * - :doc:`Configuring and running (synchronous) PBT and understanding the underlying algorithm behavior with a simple example <pbt_visualization/pbt_visualization>`
  * - :doc:`<includes/pbt_function>`
  * - :doc:`<includes/pb2_example>`
  * - :doc:`<includes/logging_example>`


Exercises
---------

.. list-table::

  * - `Basics of using Tune <https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_1_basics.ipynb>`_
  * - `Using Search algorithms and Trial Schedulers to optimize your model <https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_2_optimize.ipynb>`_
  * - `Using Population-Based Training (PBT) <https://colab.research.google.com/github/ray-project/tutorial/blob/master/tune_exercises/exercise_3_pbt.ipynb>`_
  * - `Fine-tuning Huggingface Transformers with PBT <https://colab.research.google.com/drive/1tQgAKgcKQzheoh503OzhS4N9NtfFgmjF?usp=sharing>`_
  * - `Logging Tune Runs to Comet ML <https://colab.research.google.com/drive/1dp3VwVoAH1acn_kG7RuT62mICnOqxU1z?usp=sharing>`_