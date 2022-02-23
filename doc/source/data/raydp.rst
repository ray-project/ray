.. _spark-on-ray:

**************************
Using Spark on Ray (RayDP)
**************************

RayDP combines your Spark and Ray clusters, making it easy to do large scale
data processing using the PySpark API and seemlessly use that data to train
your models using TensorFlow and PyTorch.

For more information and examples, see the RayDP Github page:
https://github.com/oap-project/raydp

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

  import ray
  import raydp

  ray.init()
  spark = raydp.init_spark(
    app_name = "example",
    num_executors = 10,
    executor_cores = 64,
    executor_memory = "256GB"
  )

====================================
Deep Learning with a Spark DataFrame
====================================

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Training a Spark DataFrame with TensorFlow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``raydp.tf.TFEstimator`` provides an API for training with TensorFlow.

.. code-block:: python

  from pyspark.sql.functions import col
  df = spark.range(1, 1000)
  # calculate z = x + 2y + 1000
  df = df.withColumn("x", col("id")*2)\
    .withColumn("y", col("id") + 200)\
    .withColumn("z", col("x") + 2*col("y") + 1000)
  
  from raydp.utils import random_split
  train_df, test_df = random_split(df, [0.7, 0.3])

  # TensorFlow code
  from tensorflow import keras
  input_1 = keras.Input(shape=(1,))
  input_2 = keras.Input(shape=(1,))

  concatenated = keras.layers.concatenate([input_1, input_2])
  output = keras.layers.Dense(1, activation='sigmoid')(concatenated)
  model = keras.Model(inputs=[input_1, input_2],
                      outputs=output)

  optimizer = keras.optimizers.Adam(0.01)
  loss = keras.losses.MeanSquaredError()

  from raydp.tf import TFEstimator
  estimator = TFEstimator(
    num_workers=2,
    model=model,
    optimizer=optimizer,
    loss=loss,
    metrics=["accuracy", "mse"],
    feature_columns=["x", "y"],
    label_column="z",
    batch_size=1000,
    num_epochs=2,
    use_gpu=False,
    config={"fit_config": {"steps_per_epoch": 2}})

  estimator.fit_on_spark(train_df, test_df)

  tensorflow_model = estimator.get_model()

  estimator.shutdown()


^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Training a Spark DataFrame with PyTorch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similarly, ``raydp.torch.TorchEstimator`` provides an API for training with
PyTorch.

.. code-block:: python

  from pyspark.sql.functions import col
  df = spark.range(1, 1000)
  # calculate z = x + 2y + 1000
  df = df.withColumn("x", col("id")*2)\
    .withColumn("y", col("id") + 200)\
    .withColumn("z", col("x") + 2*col("y") + 1000)
  
  from raydp.utils import random_split
  train_df, test_df = random_split(df, [0.7, 0.3])

  # PyTorch Code 
  import torch
  class LinearModel(torch.nn.Module):
      def __init__(self):
          super(LinearModel, self).__init__()
          self.linear = torch.nn.Linear(2, 1)

      def forward(self, x, y):
          x = torch.cat([x, y], dim=1)
          return self.linear(x)

  model = LinearModel()
  optimizer = torch.optim.Adam(model.parameters())
  loss_fn = torch.nn.MSELoss()

  def lr_scheduler_creator(optimizer, config):
      return torch.optim.lr_scheduler.MultiStepLR(
        optimizer, milestones=[150, 250, 350], gamma=0.1)

  # You can use the RayDP Estimator API or libraries like Ray Train for distributed training.
  from raydp.torch import TorchEstimator
  estimator = TorchEstimator(
    num_workers = 2,
    model = model,
    optimizer = optimizer,
    loss = loss_fn,
    lr_scheduler_creator=lr_scheduler_creator,
    feature_columns = ["x", "y"],
    label_column = ["z"],
    batch_size = 1000,
    num_epochs = 2
  )

  estimator.fit_on_spark(train_df, test_df)

  pytorch_model = estimator.get_model()

  estimator.shutdown()
  
