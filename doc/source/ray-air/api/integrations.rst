.. _air-ml-integrations:

Integrations with ML Libraries
==============================

.. currentmodule:: ray

PyTorch
-------

There are 2 recommended ways to train PyTorch models on a Ray cluster.

.. note::

    If you're training PyTorch models with PyTorch Lightning, see :ref:`below <air-pytorch-lightning-integration>`
    for the available PyTorch Lightning Ray AIR integrations.

See the options |:one:| |:two:| below, along with the usage scenarios and API references for each:

|:one:| Vanilla PyTorch with Ray Tune
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations).
Use vanilla PyTorch with Ray Tune to parallelize model training.

.. seealso::

    :ref:`See an example here. <tune-pytorch-cifar-ref>`


|:two:| :class:`~ray.train.torch.TorchTrainer`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.torch.TorchTrainer

.. seealso::

    :ref:`See here for an example. <air-convert-torch-to-air>`


.. _air-pytorch-lightning-integration:

PyTorch Lightning
-----------------

There are 2 recommended ways to train with PyTorch Lightning on a Ray cluster.

See the options |:one:| |:two:| below, along with the usage scenarios and API references for each:

|:one:| Vanilla PyTorch Lightning with a Ray Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations).
Use vanilla PyTorch Lightning with Ray Tune to parallelize model training.

.. autosummary::

    ~tune.integration.pytorch_lightning.TuneReportCallback
    ~tune.integration.pytorch_lightning.TuneReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-pytorch-lightning-ref>`


|:two:| :class:`~ray.train.lightning.LightningTrainer`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Distributed training, such as multi-GPU or multi-node data-parallel training.

.. autosummary::

    ~train.lightning.LightningTrainer

.. seealso::

    :ref:`See the full API reference for the Ray Train Lightning integration. <train-lightning-integration>`

    :ref:`See an example here. <lightning_mnist_example>`


.. _air-keras-integration:

Tensorflow/Keras
----------------

There are 2 recommended ways to train Tensorflow/Keras models with Ray.

See the options |:one:| |:two:| below, along with the usage scenarios and API references for each:

|:one:| Vanilla Keras with a Ray Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations).
Use vanilla Tensorflow/Keras with Ray Tune to parallelize model training.


.. autosummary::
    :toctree: doc/

    ~air.integrations.keras.ReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-mnist-keras>`


|:two:| :class:`~ray.train.tensorflow.TensorflowTrainer`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.tensorflow.TensorflowTrainer
    ~air.integrations.keras.ReportCheckpointCallback

.. seealso::

    :ref:`See here for an example. <air-convert-tf-to-air>`


XGBoost
-------

There are 3 recommended ways to train XGBoost models with Ray.

See the options |:one:| |:two:| |:three:| below, along with the usage scenarios
and API references for each:


.. _air-integration-xgboost:

|:one:| Vanilla XGBoost with a Ray Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations). Use vanilla XGBoost
with these Ray Tune callbacks to parallelize model training.

.. autosummary::

    ~tune.integration.xgboost.TuneReportCallback
    ~tune.integration.xgboost.TuneReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-xgboost-ref>`


|:two:| :class:`~ray.train.xgboost.XGBoostTrainer`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.xgboost.XGBoostTrainer

.. seealso::

    :ref:`See an example here. <air-xgboost-example-ref>`


|:three:| `xgboost_ray <https://github.com/ray-project/xgboost_ray>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Use as a (nearly) drop-in replacement for the regular xgboost API, with added support for distributed training on a Ray cluster.

See the `xgboost_ray <https://github.com/ray-project/xgboost_ray>`_ documentation.


.. _air-integration-lightgbm:

LightGBM
--------

There are 3 recommended ways to train LightGBM models with Ray.

See the options |:one:| |:two:| |:three:| below, along with the usage scenarios and API references for each:


|:one:| Vanilla LightGBM with a Ray Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Non-distributed training, where the dataset is relatively small
and there are many trials (e.g., many hyperparameter configurations). Use vanilla LightGBM
with these Ray Tune callbacks to parallelize model training.

.. autosummary::

    ~tune.integration.lightgbm.TuneReportCallback
    ~tune.integration.lightgbm.TuneReportCheckpointCallback

.. seealso::

    :ref:`See an example here. <tune-lightgbm-example>`


|:two:| :class:`~ray.train.lightgbm.LightGBMTrainer`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Data-parallel training, such as multi-GPU or multi-node training.

.. autosummary::

    ~train.lightgbm.LightGBMTrainer

.. seealso::

    :ref:`See an example here. <air-lightgbm-example-ref>`



|:three:| `lightgbm_ray <https://github.com/ray-project/lightgbm_ray>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Usage Scenario:** Use as a (nearly) drop-in replacement for the regular lightgbm API,
with added support for distributed training on a Ray cluster.

See the `lightgbm_ray <https://github.com/ray-project/lightgbm_ray>`_ documentation.
