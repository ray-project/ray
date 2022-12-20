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
      - **Performance**
      - **Disk Spill**
      - **Command**
    * - 1 m5.4xlarge node (1 actor)
      - 390 s (0.51 GiB/s)
      - 205 GiB
      - `python data_benchmark.py --dataset-size-gb=200 --num-workers=1`
    * - 5 m5.4xlarge nodes (5 actors)
      - 70 s (2.85 GiB/S)
      - 206 GiB
      - `python data_benchmark.py --dataset-size-gb=200 --num-workers=5`
    * - 20 m5.4xlarge nodes (20 actors)
      - 3.8 s (52.6 GiB/s)
      - 0 GiB
      - `python data_benchmark.py --dataset-size-gb=200 --num-workers=20`


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
      - **Data Size**
      - **Performance**
      - **Command**
    * - 1 m5.4xlarge node (1 actor)
      - 10 GB (26M rows)
      - 275 s (94.5k rows/s)
      - `python xgboost_benchmark.py --size 10GB`
    * - 10 m5.4xlarge nodes (10 actors)
      - 100 GB (260M rows)
      - 331 s (786k rows/s)
      - `python xgboost_benchmark.py --size 100GB`

.. _xgboost-benchmark:

XGBoost training
----------------

This task uses the XGBoostTrainer module to train on different sizes of data
with different amounts of parallelism.

XGBoost parameters were kept as defaults for xgboost==1.6.1 this task.


- `XGBoost Training Script`_
- `XGBoost Cluster Configuration`_

.. list-table::

    * - **Cluster Setup**
      - **Data Size**
      - **Performance**
      - **Command**
    * - 1 m5.4xlarge node (1 actor)
      - 10 GB (26M rows)
      - 692 s
      - `python xgboost_benchmark.py --size 10GB`
    * - 10 m5.4xlarge nodes (10 actors)
      - 100 GB (260M rows)
      - 693 s
      - `python xgboost_benchmark.py --size 100GB`


GPU image batch prediction
--------------------------

This task uses the BatchPredictor module to process different amounts of data
using a Pytorch pre-trained ResNet model.

We test out the performance across different cluster sizes and data sizes.

- `GPU image batch prediction script`_
- `GPU prediction small cluster configuration`_
- `GPU prediction large cluster configuration`_

.. list-table::

    * - **Cluster Setup**
      - **Data Size**
      - **Performance**
      - **Command**
    * - 1 g4dn.8xlarge node
      - 1 GB (1623 images)
      - 46.12 s (35.19 images/sec)
      - `python gpu_batch_prediction.py --data-size-gb=1`
    * - 1 g4dn.8xlarge node
      - 20 GB (32460 images)
      - 285.2 s (113.81 images/sec)
      - `python gpu_batch_prediction.py --data-size-gb=20`
    * - 4 g4dn.12xlarge nodes
      - 100 GB (162300 images)
      - 304.01 s (533.86 images/sec)
      - `python gpu_batch_prediction.py --data-size-gb=100`

.. _pytorch_gpu_training_benchmark:

GPU image training
------------------

This task uses the TorchTrainer module to train different amounts of data
using an Pytorch ResNet model.

We test out the performance across different cluster sizes and data sizes.

- `GPU image training script`_
- `GPU training small cluster configuration`_
- `GPU training large cluster configuration`_

.. note::

    For multi-host distributed training, on AWS we need to ensure ec2 instances are in the same VPC and
    all ports are open in the secure group.


.. list-table::

    * - **Cluster Setup**
      - **Data Size**
      - **Performance**
      - **Command**
    * - 1 g3.8xlarge node (1 worker)
      - 1 GB (1623 images)
      - 79.76 s (2 epochs, 40.7 images/sec)
      - `python pytorch_training_e2e.py --data-size-gb=1`
    * - 1 g3.8xlarge node (1 worker)
      - 20 GB (32460 images)
      - 1388.33 s (2 epochs, 46.76 images/sec)
      - `python pytorch_training_e2e.py --data-size-gb=20`
    * - 4 g3.16xlarge nodes (16 workers)
      - 100 GB (162300 images)
      - 434.95 s (2 epochs, 746.29 images/sec)
      - `python pytorch_training_e2e.py --data-size-gb=100 --num-workers=16`

.. _pytorch-training-parity:

Pytorch Training Parity
-----------------------

This task checks the performance parity between native Pytorch Distributed and
Ray Train's distributed TorchTrainer.

We demonstrate that the performance is similar (within 2.5\%) between the two frameworks.
Performance may vary greatly across different model, hardware, and cluster configurations.

The reported times are for the raw training times. There is an unreported constant setup
overhead of a few seconds for both methods that is negligible for longer training runs.

- `Pytorch comparison training script`_
- `Pytorch comparison CPU cluster configuration`_
- `Pytorch comparison GPU cluster configuration`_

.. list-table::

    * - **Cluster Setup**
      - **Dataset**
      - **Performance**
      - **Command**
    * - 4 m5.2xlarge nodes (4 workers)
      - FashionMNIST
      - 196.64 s (vs 194.90 s Pytorch)
      - `python workloads/torch_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 4 --cpus-per-worker 8`
    * - 4 m5.2xlarge nodes (16 workers)
      - FashionMNIST
      - 430.88 s (vs 475.97 s Pytorch)
      - `python workloads/torch_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 16 --cpus-per-worker 2`
    * - 4 g4dn.12xlarge node (16 workers)
      - FashionMNIST
      - 149.80 s (vs 146.46 s Pytorch)
      - `python workloads/torch_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 16 --cpus-per-worker 4 --use-gpu`


.. _tf-training-parity:

Tensorflow Training Parity
--------------------------

This task checks the performance parity between native Tensorflow Distributed and
Ray Train's distributed TensorflowTrainer.

We demonstrate that the performance is similar (within 1\%) between the two frameworks.
Performance may vary greatly across different model, hardware, and cluster configurations.

The reported times are for the raw training times. There is an unreported constant setup
overhead of a few seconds for both methods that is negligible for longer training runs.

.. note:: The batch size and number of epochs is different for the GPU benchmark, resulting in a longer runtime.

- `Tensorflow comparison training script`_
- `Tensorflow comparison CPU cluster configuration`_
- `Tensorflow comparison GPU cluster configuration`_

.. list-table::

    * - **Cluster Setup**
      - **Dataset**
      - **Performance**
      - **Command**
    * - 4 m5.2xlarge nodes (4 workers)
      - FashionMNIST
      - 78.81 s (vs 79.67 s Tensorflow)
      - `python workloads/tensorflow_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 4 --cpus-per-worker 8`
    * - 4 m5.2xlarge nodes (16 workers)
      - FashionMNIST
      - 64.57 s (vs 67.45 s Tensorflow)
      - `python workloads/tensorflow_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 16 --cpus-per-worker 2`
    * - 4 g4dn.12xlarge node (16 workers)
      - FashionMNIST
      - 465.16 s (vs 461.74 s Tensorflow)
      - `python workloads/tensorflow_benchmark.py run --num-runs 3 --num-epochs 200 --num-workers 16 --cpus-per-worker 4 --batch-size 64 --use-gpu`


.. _`Bulk Ingest Script`: https://github.com/ray-project/ray/blob/a30bdf9ef34a45f973b589993f7707a763df6ebf/release/air_tests/air_benchmarks/workloads/data_benchmark.py#L25-L40
.. _`Bulk Ingest Cluster Configuration`: https://github.com/ray-project/ray/blob/a30bdf9ef34a45f973b589993f7707a763df6ebf/release/air_tests/air_benchmarks/data_20_nodes.yaml#L6-L15
.. _`XGBoost Training Script`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py#L40-L58
.. _`XGBoost Prediction Script`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/workloads/xgboost_benchmark.py#L63-L71
.. _`XGBoost Cluster Configuration`: https://github.com/ray-project/ray/blob/a241e6a0f5a630d6ed5b84cce30c51963834d15b/release/air_tests/air_benchmarks/xgboost_compute_tpl.yaml#L6-L24
.. _`GPU image batch prediction script`: https://github.com/ray-project/ray/blob/cec82a1ced631525a4d115e4dc0c283fa4275a7f/release/air_tests/air_benchmarks/workloads/gpu_batch_prediction.py#L18-L49
.. _`GPU image training script`: https://github.com/ray-project/ray/blob/cec82a1ced631525a4d115e4dc0c283fa4275a7f/release/air_tests/air_benchmarks/workloads/pytorch_training_e2e.py#L95-L106
.. _`GPU prediction small cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_1_g4_8xl.yaml#L6-L15
.. _`GPU prediction large cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4_g4_12xl.yaml#L6-L15
.. _`GPU training small cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_1.yaml#L6-L24
.. _`GPU training large cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_16.yaml#L5-L25
.. _`Pytorch comparison training script`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/workloads/torch_benchmark.py
.. _`Pytorch comparison CPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_cpu_4.yaml
.. _`Pytorch comparison GPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4.yaml
.. _`Tensorflow comparison training script`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/workloads/tensorflow_benchmark.py
.. _`Tensorflow comparison CPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_cpu_4.yaml
.. _`Tensorflow comparison GPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4.yaml
