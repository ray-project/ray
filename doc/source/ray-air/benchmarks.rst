Benchmarks
==========

Below we document key performance benchmarks for common AIR tasks and workflows.

Bulk Ingest
-----------

This task uses the DummyTrainer module to ingest 200GiB of synthetic data.

We test out the performance across different cluster sizes.

- `Bulk Ingest Script`_
- `Bulk Ingest Cluster Configuration`_

For this benchmark, we configured the nodes to have reasonable disk size and throughput to account for object spilling.

.. code-block:: yaml

    aws:
        BlockDeviceMappings:
            - DeviceName: /dev/sda1
              Ebs:
                Iops: 5000
                Throughput: 1000
                VolumeSize: 1000
                VolumeType: gp3

.. list-table::

    * - **Cluster Setup**
      - **# workers**
      - **Time taken**
      - **Throughput**
      - **Data Spilled**
      - **Command**
    * - 1 m5.4xlarge
      - 1 actor
      - 390 s
      - 0.51 GB/s
      - 205 GiB
      - `python data_benchmark.py --dataset-size-gib=200 --num-workers=1 --placement-strategy=SPREAD`
    * - 5 m5.4xlarge
      - 5 actors
      - 70 s
      - 2.85 GiB/s
      - 206 GiB
      - `python data_benchmark.py --dataset-size-gib=200 --num-workers=5 --placement-strategy=SPREAD`
    * - 20 m5.4xlarge nodes
      - 20 actors
      - 3.8 s
      - 52.6 GiB/s
      - 0 GB
      - `python data_benchmark.py --dataset-size-gib=200 --num-workers=20 --placement-strategy=SPREAD`


XGBoost Batch Prediction
------------------------

This task uses the BatchPredictor module to process different amounts of data
using an XGBoost model.

We test out the performance across different cluster sizes and data sizes.

- `XGBoost Prediction Script`_
- `XGBoost Cluster Configuration`_

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
- `XGBoost Cluster Configuration`_

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


.. _`Bulk Ingest Script`: https://github.com/ray-project/ray/blob/a30bdf9ef34a45f973b589993f7707a763df6ebf/release/air_tests/air_benchmarks/workloads/data_benchmark.py#L25-L40
.. _`Bulk Ingest Cluster Configuration`: https://github.com/ray-project/ray/blob/a30bdf9ef34a45f973b589993f7707a763df6ebf/release/air_tests/air_benchmarks/data_20_nodes.yaml#L6-L15
.. _`XGBoost Training Script`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py#L40-L58
.. _`XGBoost Prediction Script`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py#L63-L71
.. _`XGBoost Cluster Configuration`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/xgboost_compute_tpl.yaml#L6-L24
