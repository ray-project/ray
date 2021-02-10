********************
RayDP (Spark on Ray)
********************

RayDP combines your Spark and Ray clusters, making it easy to do large scale
data processing using the PySpark API and seemlessly use that data to train
your models using TensorFlow and PyTorch.

For more information and examples, see the RayDP Github page:
https://github.com/oap_project/raydp

================
Installing RayDP
================

RayDP can be installed from PyPI and supports PySpark 3.0 and 3.1.

.. code-block bash

  pip install raydp

.. note::
  RayDP requires ray >= 1.2.0

.. note::
  In order to run Spark, the head and worker nodes will need Java installed.

========================
Creating a Spark Session
========================

To create a spark session, call ``raydp.init_spark``

For example,

.. code-block:: python

  import raydp

  spark = raydp.init_spark(
    app_name = "example",
    num_executors = 10,
    executor_cores = 64,
    memory_per_executor = "256GB"
  )

====================================
Deep Learning with a Spark DataFrame
====================================

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Training a Spark DataFrame with TensorFlow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``raydp.tf.TFEstimator`` provides an API for training with TensorFlow.

.. code-block:: python

  d = [{'age': 17 , 'grade': 12}]
  df = spark.createDataFrame(d).collect()


  from tensorflow import keras
  model = keras.Sequential([])

  estimator = raydp.tf.TFEstimator(
    model = model,
    num_worker = 10,
    feature_columns = ["age"],
    label_column = ["grade"]
  )

  estimator.fit_on_spark(df, test_df=None)

  tensorflow_model = estimator.get_model()


^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Training a Spark DataFrame with TensorFlow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similarly, ``raydp.torch.TorchEstimator`` provides an API for training with
PyTorch.

.. code-block:: python

  d = [{'age': 17 , 'grade': 12}]
  df = spark.createDataFrame(d).collect()


  import torch
  model = torch.nn.Sequential()

  estimator = raydp.tf.TFEstimator(
    model = model,
    num_worker = 10,
    feature_columns = ["age"],
    label_column = ["grade"]
  )

  estimator.fit_on_spark(df, test_df=None)

  pytorch_model = estimator.get_model()
  
