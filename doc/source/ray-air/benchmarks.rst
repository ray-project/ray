AIR Benchmarks
==============

Below we document key performance benchmarks for common AIR tasks and workflows.

XGBoost Batch Prediction
------------------------

This task uses the BatchPredictor module to process different amounts of data
using an XGBoost model.

We test out the performance across different cluster sizes and data sizes.

.. TODO: Link to script and cluster configuration when merged

.. list-table::

    * - **Cluster Configuration**
      - **Parallel workers**
      - **Data Size**
      - **Number of rows**
      - **Time taken**
      - **Throughput**
    * - 1 m5.4xlarge
      - 1 actor
      - 10 GB
      - 26M rows
      - 275 s
      - 94.5k rows/sec
    * - 10 m5.4xlarge nodes
      - 10 actors (12 CPUs each)
      - 100 GB
      - 260M rows
      - 331 s
      - 786k rows/sec


XGBoost training
----------------

This task uses the XGBoostTrainer module to train on different sizes of data
with different amounts of parallelism.

XGBoost parameters were kept as defaults for xgboost==1.6.1 this task.

.. list-table::

    * - Cluster Configuration
      - Parallel workers
      - Data Size
      - Number of rows
      - Time taken
    * - 1 m5.4xlarge
      - 1 actor
      - 10 GB
      - 26M rows
      - 692 s
    * - 10 m5.4xlarge nodes
      - 10 actors (12 CPUs each)
      - 100 GB
      - 260M rows
      - 693 s
