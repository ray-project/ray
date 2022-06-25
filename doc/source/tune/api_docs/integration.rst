.. _tune-integration:

External library integrations (tune.integration)
================================================

.. contents::
    :local:
    :depth: 1


.. _tune-integration-keras:

Keras (tune.integration.keras)
------------------------------------------------------

.. autoclass:: ray.tune.integration.keras.TuneReportCallback

.. autoclass:: ray.tune.integration.keras.TuneReportCheckpointCallback


.. _tune-integration-mlflow:

MLflow (tune.integration.mlflow)
--------------------------------

:ref:`See also here <tune-mlflow-ref>`.

.. autoclass:: ray.tune.integration.mlflow.MLflowLoggerCallback

.. autofunction:: ray.tune.integration.mlflow.mlflow_mixin


.. _tune-integration-mxnet:

MXNet (tune.integration.mxnet)
------------------------------

.. autoclass:: ray.tune.integration.mxnet.TuneReportCallback

.. autoclass:: ray.tune.integration.mxnet.TuneCheckpointCallback


.. _tune-integration-pytorch-lightning:

PyTorch Lightning (tune.integration.pytorch_lightning)
------------------------------------------------------

.. autoclass:: ray.tune.integration.pytorch_lightning.TuneReportCallback

.. autoclass:: ray.tune.integration.pytorch_lightning.TuneReportCheckpointCallback

.. _tune-integration-wandb:

Weights and Biases (tune.integration.wandb)
-------------------------------------------

:ref:`See also here <tune-wandb-ref>`.

.. autoclass:: ray.tune.integration.wandb.WandbLoggerCallback

.. autofunction:: ray.tune.integration.wandb.wandb_mixin


.. _tune-integration-xgboost:

XGBoost (tune.integration.xgboost)
----------------------------------

.. autoclass:: ray.tune.integration.xgboost.TuneReportCallback

.. autoclass:: ray.tune.integration.xgboost.TuneReportCheckpointCallback


.. _tune-integration-lightgbm:

LightGBM (tune.integration.lightgbm)
------------------------------------

.. autoclass:: ray.tune.integration.lightgbm.TuneReportCallback

.. autoclass:: ray.tune.integration.lightgbm.TuneReportCheckpointCallback
