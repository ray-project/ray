.. _loggers-docstring:

Tune Loggers (tune.logger)
==========================

Tune automatically uses loggers for TensorBoard, CSV, and JSON formats.
By default, Tune only logs the returned result dictionaries from the training function.

If you need to log something lower level like model weights or gradients,
see :ref:`Trainable Logging <trainable-logging>`.

.. note::

    Tune's per-trial ``Logger`` classes have been deprecated. Use the ``LoggerCallback`` interface instead.


.. currentmodule:: ray

.. _logger-interface:

LoggerCallback Interface (tune.logger.LoggerCallback)
-----------------------------------------------------

.. autosummary::
    :toctree: doc/

    ~tune.logger.LoggerCallback

.. autosummary::
    :toctree: doc/

    ~tune.logger.LoggerCallback.log_trial_start
    ~tune.logger.LoggerCallback.log_trial_restore
    ~tune.logger.LoggerCallback.log_trial_save
    ~tune.logger.LoggerCallback.log_trial_result
    ~tune.logger.LoggerCallback.log_trial_end


Tune Built-in Loggers
---------------------

.. autosummary::
    :toctree: doc/

    tune.logger.JsonLoggerCallback
    tune.logger.CSVLoggerCallback
    tune.logger.TBXLoggerCallback


MLFlow Integration
------------------

Tune also provides a logger for `MLflow <https://mlflow.org>`_.
You can install MLflow via ``pip install mlflow``.
See the :doc:`tutorial here </tune/examples/tune-mlflow>`.

.. autosummary::

    air.integrations.mlflow.MLflowLoggerCallback

Wandb Integration
-----------------

Tune also provides a logger for `Weights & Biases <https://www.wandb.ai/>`_.
You can install Wandb via ``pip install wandb``.
See the :doc:`tutorial here </tune/examples/tune-wandb>`.

.. autosummary::

    air.integrations.wandb.WandbLoggerCallback

Aim Integration
---------------

Tune also provides a logger for the `Aim <https://aimstack.io/>`_ experiment tracker.
You can install Aim via ``pip install aim``.
See the :doc:`tutorial here </tune/examples/tune-aim>`

.. autosummary::
    :toctree: doc/

    ~tune.logger.aim.AimLoggerCallback


Other Integrations
------------------

Viskit
~~~~~~

Tune automatically integrates with `Viskit <https://github.com/vitchyr/viskit>`_ via the ``CSVLoggerCallback`` outputs.
To use VisKit (you may have to install some dependencies), run:

.. code-block:: bash

    $ git clone https://github.com/rll/rllab.git
    $ python rllab/rllab/viskit/frontend.py ~/ray_results/my_experiment

The non-relevant metrics (like timing stats) can be disabled on the left to show only the
relevant ones (like accuracy, loss, etc.).

.. image:: ../images/ray-tune-viskit.png

