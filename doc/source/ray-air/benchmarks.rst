AIR Benchmarks
==============

Below we document key performance benchmarks for common AIR tasks and workflows.

XGBoost Batch Prediction
------------------------

This task uses the BatchPredictor module to process different amounts of data
using an XGBoost model.

We test out the performance across different cluster sizes and data sizes.

.. TODO: Link to script and cluster configuration when merged

.. list-table:: XGBoost Batch Prediction Performance

    * - Cluster Configuration
      - 1 m5.4xlarge
      - 10 m5.4xlarge nodes (with fastest possible EBS disks)
    * - Parallel workers
      - 1 actor
      - 10 actors (12 CPUs each)
    * - Data Size
      - 10 GB
      - 100 GB
    * - Number of rows
      - 26M rows
      - 260M rows
    * - Time taken
      - 275 s
      - 331 s
    * - Throughput
      - 94.5k rows/sec
      - 786k rows/sec


XGBoost training
----------------

This task uses the XGBoostTrainer module to train on different sizes of data
with different amounts of parallelism.

XGBoost parameters were kept as defaults for xgboost==1.6.1 this task.

.. list-table:: XGBoost Training Performance

    * - Cluster Configuration
      - 1 m5.4xlarge
      - 10 m5.4xlarge nodes (with fastest possible EBS disks)
    * - Parallel workers
      - 1 actor
      - 10 actors (12 CPUs each)
    * - Data Size
      - 10 GB
      - 100 GB
    * - Number of rows
      - 26M rows
      - 260M rows
    * - Time taken
      - 692 s
      - 693 s
