.. _train-benchmarks:

Ray Train Benchmarks
====================

Below we document key performance benchmarks for common Ray Train tasks and workflows.

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

.. _xgboost-benchmark:

XGBoost training
----------------

This task uses the XGBoostTrainer module to train on different sizes of data
with different amounts of parallelism to show near-linear scaling from distributed
data parallelism.

XGBoost parameters were kept as defaults for ``xgboost==1.7.6`` this task.


- `XGBoost Training Script`_
- `XGBoost Cluster Configuration`_

.. list-table::

    * - **Cluster Setup**
      - **Number of distributed training workers**
      - **Data Size**
      - **Performance**
      - **Command**
    * - 1 m5.4xlarge node with 16 CPUs
      - 1 training worker using 12 CPUs, leaving 4 CPUs for Ray Data tasks
      - 10 GB (26M rows)
      - 310.22 s
      - `python train_batch_inference_benchmark.py "xgboost" --size=10GB`
    * - 10 m5.4xlarge nodes
      - 10 training workers (one per node), using 10x12 CPUs, leaving 10x4 CPUs for Ray Data tasks
      - 100 GB (260M rows)
      - 326.86 s
      - `python train_batch_inference_benchmark.py "xgboost" --size=100GB`

.. _`GPU image training script`: https://github.com/ray-project/ray/blob/cec82a1ced631525a4d115e4dc0c283fa4275a7f/release/air_tests/air_benchmarks/workloads/pytorch_training_e2e.py#L95-L106
.. _`GPU training small cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_1_aws.yaml#L6-L24
.. _`GPU training large cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4_aws.yaml#L5-L25
.. _`Pytorch comparison training script`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/workloads/torch_benchmark.py
.. _`Pytorch comparison CPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_cpu_4_aws.yaml
.. _`Pytorch comparison GPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4_aws.yaml
.. _`Tensorflow comparison training script`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/workloads/tensorflow_benchmark.py
.. _`Tensorflow comparison CPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_cpu_4_aws.yaml
.. _`Tensorflow comparison GPU cluster configuration`: https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4_aws.yaml
.. _`XGBoost Training Script`: https://github.com/ray-project/ray/blob/9ac58f4efc83253fe63e280106f959fe317b1104/release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py
.. _`XGBoost Cluster Configuration`: https://github.com/ray-project/ray/tree/9ac58f4efc83253fe63e280106f959fe317b1104/release/train_tests/xgboost_lightgbm
