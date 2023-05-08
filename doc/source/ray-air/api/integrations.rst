.. _air-integrations:

Ray AIR Integrations
====================

.. currentmodule:: ray

.. _air-monitoring-integrations:
.. _air-builtin-callbacks:

Experiment Monitoring Integrations
----------------------------------

Comet (air.integrations.comet)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.comet.CometLoggerCallback

.. seealso::

    :ref:`See here for an example. <tune-comet-ref>`


.. _air-integration-mlflow:

MLflow (air.integrations.mlflow)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.mlflow.MLflowLoggerCallback
    ~air.integrations.mlflow.setup_mlflow

.. seealso::

    :ref:`See here for an example. <tune-mlflow-ref>`


.. _air-integration-wandb:

Weights and Biases (air.integrations.wandb)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~air.integrations.wandb.WandbLoggerCallback
    ~air.integrations.wandb.setup_wandb

.. seealso::

    :ref:`See here for an example. <tune-wandb-ref>`


Integrations with ML Libraries
------------------------------

PyTorch
~~~~~~~

There are 2 recommended ways to train PyTorch models on a Ray cluster.

.. note::

    If you're training PyTorch models with PyTorch Lightning, see :ref:`below <air-pytorch-lightning>`
    for the available PyTorch Lightning Ray AIR integrations.

See |:one:| |:two:| below to see the options, along with the usage scenarios and API references for each:

|:one:| Vanilla PyTorch with Ray Tune
*************************************

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations).
Use vanilla PyTorch with Ray Tune to parallelize model training.

.. seealso::

    :ref:`See an example here. <tune-pytorch-cifar-ref>`


|:two:| ``TorchTrainer``
************************

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.torch.TorchTrainer

.. seealso::

    :ref:`See here for an example. <air-convert-torch-to-air>`


.. _air-pytorch-lightning:

PyTorch Lightning
~~~~~~~~~~~~~~~~~

There are 2 recommended ways to train with PyTorch Lightning on a Ray cluster.

See |:one:| |:two:| below to see the options, along with the usage scenarios and API references for each:

|:one:| Vanilla PyTorch Lightning with a Ray Callback
*****************************************************

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations).
Use vanilla PyTorch Lightning with Ray Tune to parallelize model training.

.. autosummary::

    ~tune.integration.pytorch_lightning.TuneReportCallback
    ~tune.integration.pytorch_lightning.TuneReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-pytorch-lightning-ref>`


|:two:| ``LightningTrainer``
****************************

**Usage Scenario:** Distributed training, such as multi-GPU or multi-node data-parallel training.

.. autosummary::

    ~train.lightning.LightningTrainer

.. seealso::

    :ref:`See the full API reference for the Ray Train Lightning integration. <train-lightning-integration>`
    :ref:`See an example here. <lightning_mnist_example>`


.. _tune-integration-keras:

Tensorflow/Keras
~~~~~~~~~~~~~~~~

There are 2 recommended ways to train Tensorflow/Keras models with Ray.

See |:one:| |:two:| below to see the options, along with the usage scenarios and API references for each:

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations).
Use vanilla Tensorflow/Keras with Ray Tune to parallelize model training.

|:one:| Vanilla Keras with a Ray Callback
*****************************************

.. autosummary::
    :toctree: doc/

    ~air.integrations.keras.ReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-mnist-keras>`


|:two:| TensorflowTrainer
*************************

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.lightning.LightningTrainer
    ~air.integrations.keras.ReportCheckpointCallback

.. seealso::

    :ref:`See here for an example. <air-convert-tf-to-air>`


XGBoost
~~~~~~~

There are 3 recommended ways to train XGBoost models with Ray.

See |:one:| |:two:| |:three:| below to see the options, along with the usage scenarios
and API references for each:


.. _air-integration-xgboost:

|:one:| Vanilla XGBoost with a Ray Callback
*******************************************

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations). Use vanilla XGBoost
with these Ray Tune callbacks to parallelize model training.

.. autosummary::
    :toctree: doc/

    ~tune.integration.xgboost.TuneReportCallback
    ~tune.integration.xgboost.TuneReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-xgboost-ref>`


|:two:| ``XGBoostTrainer``
**************************

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.xgboost.XGBoostTrainer

.. seealso::

    :ref:`See an example here. <air-xgboost-example-ref>`


|:three:| ``xgboost_ray``
*************************

**Usage Scenario:** Use as a (nearly) drop-in replacement for the regular xgboost API, with added support for distributed training on a Ray cluster.

See the `xgboost_ray <https://github.com/ray-project/xgboost_ray>`_ documentation.


.. _air-integration-lightgbm:

LightGBM
~~~~~~~~

There are 3 recommended ways to train LightGBM models with Ray.

See |:one:| |:two:| |:three:| below to see the options, along with the usage scenarios and API references for each:


|:one:| Vanilla LightGBM with a Ray Callback
********************************************

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations). Use vanilla LightGBM
with these Ray Tune callbacks to parallelize model training.

.. autosummary::
    :toctree: doc/

    ~tune.integration.lightgbm.TuneReportCallback
    ~tune.integration.lightgbm.TuneReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-lightgbm-example>`


|:two:| ``LightGBMTrainer``
***************************

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.lightgbm.LightGBMTrainer

.. seealso::

    :ref:`See an example here. <air-lightgbm-example-ref>`



|:three:| ``lightgbm_ray``
**************************

**Usage Scenario:** Use as a (nearly) drop-in replacement for the regular lightgbm API,
with added support for distributed training on a Ray cluster.

See the `lightgbm_ray <https://github.com/ray-project/lightgbm_ray>`_ documentation.
