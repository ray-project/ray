.. _train-api:

Ray Train API
=============

This page covers framework specific integrations with Ray Train and Ray Train Developer APIs.

For core Ray AIR APIs, take a look at the :ref:`AIR Trainer package reference <air-trainer-ref>`.

.. _train-integration-api:

Trainer and Predictor Integrations
----------------------------------

XGBoost
~~~~~~~

``XGBoostTrainer``
******************

.. autoclass:: ray.train.xgboost.XGBoostTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``XGBoostPredictor``
********************

.. automodule:: ray.train.xgboost
    :members:
    :exclude-members: XGBoostTrainer
    :show-inheritance:

LightGBM
~~~~~~~~

``LightGBMTrainer``
*******************

.. autoclass:: ray.train.lightgbm.LightGBMTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``LightGBMPredictor``
*********************


.. automodule:: ray.train.lightgbm
    :members:
    :exclude-members: LightGBMTrainer
    :show-inheritance:

TensorFlow
~~~~~~~~~~

``TensorflowTrainer``
*********************

.. autoclass:: ray.train.tensorflow.TensorflowTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``TensorflowPredictor`` and ``TensorflowCheckpoint``
****************************************************

.. automodule:: ray.train.tensorflow
    :members:
    :exclude-members: TensorflowTrainer
    :show-inheritance:

PyTorch
~~~~~~~

``TorchTrainer``
****************

.. autoclass:: ray.train.torch.TorchTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


``TorchPredictor``
******************

.. automodule:: ray.train.torch
    :members:
    :exclude-members: TorchTrainer
    :show-inheritance:

Horovod
~~~~~~~

``HorovodTrainer``
******************

.. autoclass:: ray.train.horovod.HorovodTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``HorovodConfig``
*****************

.. automodule:: ray.train.horovod
    :members:
    :exclude-members: HorovodTrainer
    :show-inheritance:

HuggingFace
~~~~~~~~~~~

``HuggingFaceTrainer``
**********************

.. autoclass:: ray.train.huggingface.HuggingFaceTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``HuggingFacePredictor`` and ``HuggingFaceCheckpoint``
******************************************************

.. automodule:: ray.train.huggingface
    :members:
    :exclude-members: HuggingFaceTrainer
    :show-inheritance:

Scikit-Learn
~~~~~~~~~~~~

``SklearnTrainer``
******************

.. autoclass:: ray.train.sklearn.SklearnTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``SklearnPredictor`` and ``SklearnCheckpoint``
**********************************************

.. automodule:: ray.train.sklearn
    :members:
    :exclude-members: SklearnTrainer
    :show-inheritance:

Mosaic
~~~~~~

``MosaicTrainer``
*****************

.. autoclass:: ray.train.mosaic.MosaicTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.mosaic
    :members:
    :exclude-members: MosaicTrainer
    :show-inheritance:


Reinforcement Learning with RLlib
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``RLTrainer``
*************

.. autoclass:: ray.train.rl.RLTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

``RLPredictor`` and ``RLCheckpoint``
************************************

.. automodule:: ray.train.rl
    :members:
    :exclude-members: RLTrainer
    :show-inheritance:


Base Classes (Developer APIs)
-----------------------------

.. _train-base-trainer:

.. autoclass:: ray.train.trainer.BaseTrainer
    :members:
    :noindex:

    .. automethod:: __init__
        :noindex:

.. autoclass:: ray.train.data_parallel_trainer.DataParallelTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:

.. autoclass:: ray.train.gbdt_trainer.GBDTTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:

.. _train-backend:

.. autoclass:: ray.train.backend.Backend
    :members:

.. _train-backend-config:

.. autoclass:: ray.train.backend.BackendConfig
    :members:
