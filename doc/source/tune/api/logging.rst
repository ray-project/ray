.. _loggers-docstring:

Tune Loggers (tune.logger)
==========================

Tune automatically uses loggers for TensorBoard, CSV, and JSON formats.
By default, Tune only logs the returned result dictionaries from the training function.

If you need to log something lower level like model weights or gradients,
see :ref:`Trainable Logging <trainable-logging>`.

.. note::
    Tune's per-trial ``Logger`` classes have been deprecated. They can still be used, but we encourage you
    to use our new interface with the ``LoggerCallback`` class instead.


Viskit
------

Tune automatically integrates with `Viskit <https://github.com/vitchyr/viskit>`_ via the ``CSVLoggerCallback`` outputs.
To use VisKit (you may have to install some dependencies), run:

.. code-block:: bash

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

The non-relevant metrics (like timing stats) can be disabled on the left to show only the
relevant ones (like accuracy, loss, etc.).

.. image:: ../images/ray-tune-viskit.png


.. currentmodule:: ray

Tune Built-in Loggers
---------------------

.. autosummary::
    :toctree: doc/

    tune.logger.JsonLoggerCallback
    tune.logger.CSVLoggerCallback
    tune.logger.TBXLoggerCallback


MLFlow Integration: MLFlowLoggerCallback
----------------------------------------

Tune also provides a logger for `MLflow <https://mlflow.org>`_.
You can install MLflow via ``pip install mlflow``.
You can see the :doc:`tutorial here </tune/examples/tune-mlflow>`.

.. autosummary::
    :toctree: doc/

    air.integrations.mlflow.MLflowLoggerCallback

Wandb Integration: WandbLoggerCallback
--------------------------------------

Tune also provides a logger for `Weights & Biases <https://www.wandb.ai/>`_.
You can install Wandb via ``pip install wandb``.
You can see the :doc:`tutorial here </tune/examples/tune-wandb>`.

.. autosummary::
    :toctree: doc/

    air.integrations.wandb.WandbLoggerCallback

.. _logger-interface:

LoggerCallback Interface (tune.logger.LoggerCallback)
-----------------------------------------------------

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    ~tune.logger.LoggerCallback

.. autosummary::
    :toctree: doc/

    ~tune.logger.LoggerCallback.log_trial_start
    ~tune.logger.LoggerCallback.log_trial_restore
    ~tune.logger.LoggerCallback.log_trial_save
    ~tune.logger.LoggerCallback.log_trial_result
    ~tune.logger.LoggerCallback.log_trial_end
