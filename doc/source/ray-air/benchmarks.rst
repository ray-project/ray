AIR Benchmarks
==============

Below we document key performance benchmarks for common AIR tasks and workflows.

XGBoost Batch Prediction
------------------------

This task uses the BatchPredictor module to process different amounts of data
using an XGBoost model.

We test out the performance across different cluster sizes and data sizes.

- `XGBoost Prediction Script`_
- `XGBoost Cluster configuration`_

.. TODO: Add script for generating data and running the benchmark.

.. list-table::

    * - **Cluster Setup**
      - **# workers**
      - **Data Size**
      - **# of rows**
      - **Time taken**
      - **Throughput**
      - **Command**
    * - 1 m5.4xlarge
      - 1 actor
      - 10 GB
      - 26M rows
      - 275 s
      - 94.5k rows/sec
      - `python xgboost_benchmark.py --size 10GB`
    * - 10 m5.4xlarge nodes
      - 10 actors (12 CPUs each)
      - 100 GB
      - 260M rows
      - 331 s
      - 786k rows/sec
      - `python xgboost_benchmark.py --size 100GB`


XGBoost training
----------------

This task uses the XGBoostTrainer module to train on different sizes of data
with different amounts of parallelism.

XGBoost parameters were kept as defaults for xgboost==1.6.1 this task.


- `XGBoost Training Script`_
- `XGBoost Cluster configuration`_

.. list-table::

    * - **Cluster Setup**
      - **# workers**
      - **Data Size**
      - **# of rows**
      - **Time taken**
      - **Command**
    * - 1 m5.4xlarge
      - 1 actor
      - 10 GB
      - 26M rows
      - 692 s
      - `python xgboost_benchmark.py --size 10GB`
    * - 10 m5.4xlarge nodes
      - 10 actors (12 CPUs each)
      - 100 GB
      - 260M rows
      - 693 s
      - `python xgboost_benchmark.py --size 100GB`



.. _`XGBoost Training Script`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py#L40-L58
.. _`XGBoost Prediction Script`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py#L63-L71
.. _`XGBoost Cluster configuration`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/xgboost_compute_tpl.yaml#L6-L24
