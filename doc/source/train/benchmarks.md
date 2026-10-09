---
myst:
  html_meta:
    description: "Ray Train performance benchmarks: GPU image-classification throughput across cluster sizes, and parity with native PyTorch Distributed."
---

(train-benchmarks)=

# Ray Train benchmarks

This page lists key performance benchmarks for common Ray Train tasks and workflows.

(pytorch_gpu_training_benchmark)=

## GPU image training

This task uses `TorchTrainer` to train a PyTorch ResNet model on different amounts of data. It measures performance across different cluster sizes and data sizes.

- [GPU image training script](https://github.com/ray-project/ray/blob/cec82a1ced631525a4d115e4dc0c283fa4275a7f/release/air_tests/air_benchmarks/workloads/pytorch_training_e2e.py#L95-L106)
- [GPU training small cluster configuration](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_1_aws.yaml#L6-L24)
- [GPU training large cluster configuration](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4_aws.yaml#L5-L25)

:::{note}
For multi-host distributed training on AWS, make sure the EC2 instances are in the same VPC and all ports are open in the security group.
:::


```{list-table}
* - **Cluster setup**
  - **Data size**
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
```

(pytorch-training-parity)=

## PyTorch training parity

This task checks performance parity between native PyTorch Distributed and Ray Train's distributed `TorchTrainer`.

The two frameworks perform within 2.5\% of each other. Performance can vary greatly across model, hardware, and cluster configurations.

The reported times are raw training times. Both methods also have an unreported constant setup overhead of a few seconds, which is negligible for longer training runs.

- [PyTorch comparison training script](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/workloads/torch_benchmark.py)
- [PyTorch comparison CPU cluster configuration](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_cpu_4_aws.yaml)
- [PyTorch comparison GPU cluster configuration](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4_aws.yaml)

```{list-table}
* - **Cluster setup**
  - **Dataset**
  - **Performance**
  - **Command**
* - 4 m5.2xlarge nodes (4 workers)
  - FashionMNIST
  - 196.64 s (versus 194.90 s PyTorch)
  - `python workloads/torch_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 4 --cpus-per-worker 8`
* - 4 m5.2xlarge nodes (16 workers)
  - FashionMNIST
  - 430.88 s (versus 475.97 s PyTorch)
  - `python workloads/torch_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 16 --cpus-per-worker 2`
* - 4 g4dn.12xlarge nodes (16 workers)
  - FashionMNIST
  - 149.80 s (versus 146.46 s PyTorch)
  - `python workloads/torch_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 16 --cpus-per-worker 4 --use-gpu`
```


(tf-training-parity)=

## TensorFlow training parity

This task checks performance parity between native TensorFlow Distributed and Ray Train's distributed `TensorflowTrainer`.

The two frameworks perform within 1\% of each other. Performance can vary greatly across model, hardware, and cluster configurations.

The reported times are raw training times. Both methods also have an unreported constant setup overhead of a few seconds, which is negligible for longer training runs.

:::{note}
The GPU benchmark uses a different batch size and number of epochs, which results in a longer runtime.
:::

- [TensorFlow comparison training script](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/workloads/tensorflow_benchmark.py)
- [TensorFlow comparison CPU cluster configuration](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_cpu_4_aws.yaml)
- [TensorFlow comparison GPU cluster configuration](https://github.com/ray-project/ray/blob/master/release/air_tests/air_benchmarks/compute_gpu_4x4_aws.yaml)

```{list-table}
* - **Cluster setup**
  - **Dataset**
  - **Performance**
  - **Command**
* - 4 m5.2xlarge nodes (4 workers)
  - FashionMNIST
  - 78.81 s (versus 79.67 s TensorFlow)
  - `python workloads/tensorflow_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 4 --cpus-per-worker 8`
* - 4 m5.2xlarge nodes (16 workers)
  - FashionMNIST
  - 64.57 s (versus 67.45 s TensorFlow)
  - `python workloads/tensorflow_benchmark.py run --num-runs 3 --num-epochs 20 --num-workers 16 --cpus-per-worker 2`
* - 4 g4dn.12xlarge nodes (16 workers)
  - FashionMNIST
  - 465.16 s (versus 461.74 s TensorFlow)
  - `python workloads/tensorflow_benchmark.py run --num-runs 3 --num-epochs 200 --num-workers 16 --cpus-per-worker 4 --batch-size 64 --use-gpu`
```

(xgboost-benchmark)=

## XGBoost training

This task uses `XGBoostTrainer` to train on different data sizes with different amounts of parallelism to show near-linear scaling from distributed data parallelism.

This task uses the default XGBoost parameters for `xgboost==1.7.6`.


- [XGBoost training script](https://github.com/ray-project/ray/blob/9ac58f4efc83253fe63e280106f959fe317b1104/release/train_tests/xgboost_lightgbm/train_batch_inference_benchmark.py)
- [XGBoost cluster configuration](https://github.com/ray-project/ray/tree/9ac58f4efc83253fe63e280106f959fe317b1104/release/train_tests/xgboost_lightgbm)

```{list-table}
* - **Cluster setup**
  - **Number of distributed training workers**
  - **Data size**
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
```
