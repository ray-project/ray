.. _tune-integration:

External library integrations for Ray Tune (tune.integration)
=============================================================

.. currentmodule:: ray

Comet (air.integrations.comet)
-------------------------------------------

:ref:`See here for an example. <tune-comet-ref>`

.. autosummary::
    :toctree: doc/

    ~air.integrations.comet.CometLoggerCallback
    :noindex:

.. _tune-integration-keras:

Keras (tune.integration.keras)
------------------------------------------------------

.. autosummary::
    :toctree: doc/

    ~tune.integration.keras.TuneReportCallback
    ~tune.integration.keras.TuneReportCheckpointCallback


.. _tune-integration-mlflow:

MLflow (air.integrations.mlflow)
--------------------------------

:ref:`See here for an example. <tune-mlflow-ref>`

.. autosummary::
    :toctree: doc/

    ~air.integrations.mlflow.MLflowLoggerCallback
    :noindex:
    ~air.integrations.mlflow.setup_mlflow


.. _tune-integration-mxnet:

MXNet (tune.integration.mxnet)
------------------------------

.. autosummary::
    :toctree: doc/

    ~tune.integration.mxnet.TuneReportCallback
    ~tune.integration.mxnet.TuneCheckpointCallback


.. _tune-integration-pytorch-lightning:

PyTorch Lightning (tune.integration.pytorch_lightning)
------------------------------------------------------

.. autosummary::
    :toctree: doc/

    ~tune.integration.pytorch_lightning.TuneReportCallback
    ~tune.integration.pytorch_lightning.TuneReportCheckpointCallback

.. _tune-integration-wandb:

Weights and Biases (air.integrations.wandb)
-------------------------------------------

:ref:`See here for an example. <tune-wandb-ref>`

.. autosummary::
    :toctree: doc/

    ~air.integrations.wandb.WandbLoggerCallback
    :noindex:
    ~air.integrations.wandb.setup_wandb


.. _tune-integration-xgboost:

XGBoost (tune.integration.xgboost)
----------------------------------

.. autosummary::
    :toctree: doc/

    ~tune.integration.xgboost.TuneReportCallback
    ~tune.integration.xgboost.TuneReportCheckpointCallback


.. _tune-integration-lightgbm:

LightGBM (tune.integration.lightgbm)
------------------------------------

.. autosummary::
    :toctree: doc/

    ~tune.integration.lightgbm.TuneReportCallback
    ~tune.integration.lightgbm.TuneReportCheckpointCallback
