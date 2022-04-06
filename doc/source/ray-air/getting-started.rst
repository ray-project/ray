Ray AI Runtime (alpha)
======================

Ray AI Runtime (AIR) is an open-source toolkit for building end-to-end ML applications. By leveraging Ray and its library ecosystem, it brings scalability and programmability to ML platforms.


The main focuses of the Ray AI Runtime:

* Ray AIR focuses on providing the compute layer for ML workloads.
* It is designed to interoperate with other systems for storage and metadata needs.


Ray AIR consists of 5 key components -- Data processing (Ray Data), Model Training (Ray Train), Reinforcement Learning (Ray RLlib), Hyperparameter Tuning (Ray Tune), and Model Serving (Ray Serve).

Users can use these libraries interchangeably to scale different parts of standard ML workflows.


.. tip::
    **Getting involved with Ray AIR.** We'll be holding office hours, development sprints, and other activities as we get closer to the Ray AIR Beta/GA release. Want to join us? Fill out `this short form <https://forms.gle/wCCdbaQDtgErYycT6>`__!


.. contents::
    :local:


Components
----------

Preprocessors
~~~~~~~~~~~~~

.. autoclass:: ray.ml.preprocessor.Preprocessor
    :members:

.. automodule:: ray.ml.preprocessors
    :members:
    :show-inheritance:


Trainer
~~~~~~~


.. autoclass:: ray.ml.trainer.Trainer
    :members:

.. automodule:: ray.ml.train.integrations.xgboost
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.lightgbm
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.tensorflow
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.torch
    :members:
    :show-inheritance:

.. autoclass:: ray.ml.train.data_parallel_trainer.DataParallelTrainer
    :members:
    :show-inheritance:

.. autoclass:: ray.ml.train.gbdt_trainer.GBDTTrainer
    :members:
    :show-inheritance:



Tuner
~~~~~

.. autoclass:: ray.tune.tuner.Tuner
    :members:

.. automodule:: ray.tune.result_grid
    :members:

Predictors
~~~~~~~~~~

.. autoclass:: ray.ml.predictor.Predictor
    :members:

.. automodule:: ray.ml.predictors.integrations.xgboost
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.lightgbm
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.tensorflow
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.torch
    :members:
    :show-inheritance:

.. _air-serve-integration:

Serving
~~~~~~~

.. autoclass:: ray.serve.model_wrappers.ModelWrapperDeployment

.. autoclass:: ray.serve.model_wrappers.ModelWrapper


Outputs
~~~~~~~

.. automodule:: ray.ml.checkpoint
    :members:


.. automodule:: ray.ml.result
    :members:


Configs
~~~~~~~

.. automodule:: ray.ml.config
    :members:

