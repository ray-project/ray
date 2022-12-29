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

.. autoclass:: ray.train.xgboost.XGBoostTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.xgboost
    :members:
    :exclude-members: XGBoostTrainer
    :show-inheritance:

LightGBM
~~~~~~~~

.. autoclass:: ray.train.lightgbm.LightGBMTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.lightgbm
    :members:
    :exclude-members: LightGBMTrainer
    :show-inheritance:

TensorFlow
~~~~~~~~~~

.. autoclass:: ray.train.tensorflow.TensorflowTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.tensorflow
    :members:
    :exclude-members: TensorflowTrainer
    :show-inheritance:

PyTorch
~~~~~~~

.. autoclass:: ray.train.torch.TorchTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.torch
    :members:
    :exclude-members: TorchTrainer
    :show-inheritance:

Horovod
~~~~~~~

.. autoclass:: ray.train.horovod.HorovodTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.horovod
    :members:
    :exclude-members: HorovodTrainer
    :show-inheritance:

HuggingFace
~~~~~~~~~~~

.. autoclass:: ray.train.huggingface.HuggingFaceTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.huggingface
    :members:
    :exclude-members: HuggingFaceTrainer
    :show-inheritance:

Scikit-Learn
~~~~~~~~~~~~

.. autoclass:: ray.train.sklearn.SklearnTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.sklearn
    :members:
    :exclude-members: SklearnTrainer
    :show-inheritance:

Mosaic
~~~~~~

.. autoclass:: ray.train.mosaic.MosaicTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.mosaic
    :members:
    :exclude-members: MosaicTrainer
    :show-inheritance:


Reinforcement Learning (RLlib)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: ray.train.rl
    :members:
    :show-inheritance:


Base Classes (Developer APIs)
-----------------------------
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

.. autoclass:: ray.train.backend.Backend
    :members:

.. autoclass:: ray.train.backend.BackendConfig
    :members:
