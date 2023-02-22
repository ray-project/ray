.. _tune-integration:

External library integrations for Ray Tune
===========================================

.. TODO: Clean this up. Both tune.integration and air.integrations are
..   captured here. Most of the `tune.integration` can be deprecated soon.
..   XGBoost/LightGBM callbacks are no longer recommended - use their trainers instead
..   which will automatically report+checkpoint.
..   After PTL trainer is introduced, we can also deprecate that callback.

.. currentmodule:: ray

.. _tune-monitoring-integrations:
.. _air-builtin-callbacks:

Tune Experiment Monitoring Integrations
----------------------------------------

Comet (air.integrations.comet)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:ref:`See here for an example. <tune-comet-ref>`

.. autosummary::
    :toctree: doc/

    ~air.integrations.comet.CometLoggerCallback


.. _tune-integration-mlflow:

MLflow (air.integrations.mlflow)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:ref:`See here for an example. <tune-mlflow-ref>`

.. autosummary::
    :toctree: doc/

    ~air.integrations.mlflow.MLflowLoggerCallback
    ~air.integrations.mlflow.setup_mlflow

.. _tune-integration-wandb:

Weights and Biases (air.integrations.wandb)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:ref:`See here for an example. <tune-wandb-ref>`

.. autosummary::
    :toctree: doc/

    ~air.integrations.wandb.WandbLoggerCallback
    ~air.integrations.wandb.setup_wandb


Integrations with ML Libraries
--------------------------------

.. _tune-integration-keras:

Keras (air.integrations.keras)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.keras.ReportCheckpointCallback


.. _tune-integration-mxnet:

MXNet (tune.integration.mxnet)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tune.integration.mxnet.TuneReportCallback
    ~tune.integration.mxnet.TuneCheckpointCallback


.. _tune-integration-pytorch-lightning:

PyTorch Lightning (tune.integration.pytorch_lightning)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tune.integration.pytorch_lightning.TuneReportCallback
    ~tune.integration.pytorch_lightning.TuneReportCheckpointCallback

.. _tune-integration-xgboost:

XGBoost (tune.integration.xgboost)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tune.integration.xgboost.TuneReportCallback
    ~tune.integration.xgboost.TuneReportCheckpointCallback


.. _tune-integration-lightgbm:

LightGBM (tune.integration.lightgbm)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tune.integration.lightgbm.TuneReportCallback
    ~tune.integration.lightgbm.TuneReportCheckpointCallback
