.. _loggers-docstring:

Loggers (tune.logger)
=====================

Tune automatically uses loggers for TensorBoard, CSV, and JSON formats.
By default, Tune only logs the returned result dictionaries from the training function.

If you need to log something lower level like model weights or gradients,
see :ref:`Trainable Logging <trainable-logging>`.

.. note::
    Tune's per-trial ``Logger`` classes have been deprecated. They can still be used, but we encourage you
    to use our new interface with the ``LoggerCallback`` class instead.


Aim
---------

.. autoclass:: ray.tune.logger.AimCallback

You can install Aim via ``pip install aim``.
You can see the :doc:`tutorial here </tune/examples/tune-aim>`

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


TBXLogger
---------

.. autoclass:: ray.tune.logger.TBXLoggerCallback

JsonLogger
----------

.. autoclass:: ray.tune.logger.JsonLoggerCallback

CSVLogger
---------

.. autoclass:: ray.tune.logger.CSVLoggerCallback

MLFlowLogger
------------

Tune also provides a logger for `MLflow <https://mlflow.org>`_.
You can install MLflow via ``pip install mlflow``.
You can see the :doc:`tutorial here </tune/examples/tune-mlflow>`.

WandbLogger
-----------

Tune also provides a logger for `Weights & Biases <https://www.wandb.ai/>`_.
You can install Wandb via ``pip install wandb``.
You can see the :doc:`tutorial here </tune/examples/tune-wandb>`


.. _logger-interface:

LoggerCallback
--------------

.. autoclass:: ray.tune.logger.LoggerCallback
    :members: log_trial_start, log_trial_restore, log_trial_save, log_trial_result, log_trial_end
