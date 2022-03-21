Ray AIR (alpha)
===============

Ray AI Runtime (AIR) is an open-source toolkit for building end-to-end ML applications. By leveraging Ray and its library ecosystem, it brings scalability and programmability to ML platforms.

The main focuses of the Ray AI Runtime:

* Ray AIR focuses on providing the compute layer for ML workloads.
* It is designed to interoperate with other systems for storage and metadata needs.

Ray AIR consists of 5 key components -- Data processing (Ray Data), Model Training (Ray Train), Reinforcement Learning (Ray RLlib), Hyperparameter Tuning (Ray Tune), and Model Serving (Ray Serve).


Users can use these libraries interchangeably to scale different parts of standard ML workflows.


Components
----------

Preprocessors
~~~~~~~~~~~~~

.. automodule:: ray.ml.preprocessors
    :members:

Trainer
~~~~~~~

.. automodule:: ray.ml.train
    :members:


.. automodule:: ray.ml.train.integrations.xgboost
    :members:


.. automodule:: ray.ml.train.integrations.lightgbm
    :members:

.. automodule:: ray.ml.train.integrations.tensorflow
    :members:

.. automodule:: ray.ml.train.integrations.torch
    :members:

Tuner
~~~~~

.. autoclass:: ray.tune.tuner.Tuner

.. automodule:: ray.tune.result_grid
    :members:

Predictors
~~~~~~~~~~

.. automodule:: ray.ml.predictors.integrations.xgboost
    :members:


.. automodule:: ray.ml.predictors.integrations.lightgbm
    :members:

.. automodule:: ray.ml.predictors.integrations.tensorflow
    :members:

.. automodule:: ray.ml.predictors.integrations.torch
    :members:



Serving
~~~~~~~

.. automodule:: ray.serve.model_wrappers
    :members:


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

