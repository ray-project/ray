.. _tune-examples-ref:
.. _tune-recipes:

=================
Ray Tune Examples
=================

.. tip:: See :ref:`overview` to learn more about Tune features.


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

    ml-frameworks
    experiment-tracking
    hpo-frameworks
    Other Examples <other-examples>
    Exercises <exercises>


.. tip:: See :ref:<tune-guides> to learn more about Tune features.


Below are examples for using Ray Tune for a variety use cases.

ML frameworks
-------------

.. list-table::

  * - :doc:<tune-mnist-keras>
  * - :doc:<tune-pytorch-cifar-ref>
  * - :doc:<tune-pytorch-lightning-ref>
  * - :doc:<tune-rllib-example>
  * - :doc:<tune-xgboost-ref>
  * - :doc:<tune-lightgbm-example>
  * - :doc:<tune-horovod-example>
  * - :doc:<tune-huggingface-example>

  


Intermediate
------------

.. list-table::

  * - :doc:`Running a Simple MapReduce Example with Ray Core <map_reduce>`
  * - :doc:`Speed Up Your Web Crawler by Parallelizing it with Ray <web-crawler>`


Advanced
--------

.. list-table::

  * - :doc:`Build Simple AutoML for Time Series Using Ray <automl_for_time_series>`
  * - :doc:`Build Batch Prediction Using Ray <batch_prediction>`
  * - :doc:`Build a Simple Parameter Server Using Ray <plot_parameter_server>`
  * - :doc:`Simple Parallel Model Selection <plot_hyperparameter>`
  * - :doc:`Learning to Play Pong <plot_pong_example>`
