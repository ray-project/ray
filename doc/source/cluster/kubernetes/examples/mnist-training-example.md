(kuberay-mnist-training-example)=

# Train MNIST on a Neural Network with CPUs on Kubernetes

This guide runs a sample Ray Train workload with CPUs on Kubernetes infrastructure.

## Step 1: Install KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository. After installation, you should see the operator running:

```sh
# Confirm that the operator is running in the namespace `default`.
kubectl get pods
# NAME                                                      READY   STATUS              RESTARTS   AGE
# kuberay-operator-6dddd689fb-ksmcs                         1/1     Running             0          48s
```

## Step 2: Create a RayJob

A RayJob consists of a RayCluster custom resource and a job that can be submitted to the RayCluster. With RayJob, KubeRay creates a RayCluster and submit a job when the cluster is ready. Here, we provide a CPU-only RayJob description yaml file for the MNIST training on a Neural Network.

```sh
# Download `ray-job.pytorch-mnist.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-mnist/ray-job.pytorch-mnist.yaml

# Create a RayJob
kubectl apply -f ray-job.pytorch-mnist.yaml
```

Feel free to adjust the `NUM_WORKERS` field and the `replicas` field under `workerGroupSpecs` in `rayClusterSpec` in the yaml file such that all the worker pods can reach `Running` status.

```sh
# `replicas` and `NUM_WORKERS` set to 2
kubectl get pods
# NAME                                                      READY   STATUS    RESTARTS   AGE
# kuberay-operator-6dddd689fb-ksmcs                         1/1     Running   0          6m8s
# pytorch-mnist-raycluster-rkdmq-worker-small-group-c8bwx   1/1     Running   0          5m32s
# pytorch-mnist-raycluster-rkdmq-worker-small-group-s7wvm   1/1     Running   0          5m32s
# rayjob-pytorch-mnist-nxmj2                                1/1     Running   0          4m17s
# rayjob-pytorch-mnist-raycluster-rkdmq-head-m4dsl          1/1     Running   0          5m32s
```

Check that the job is in the `RUNNING` status:

```sh
kubectl get rayjob
# NAME                   JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME   AGE
# rayjob-pytorch-mnist   RUNNING      Running             2024-06-17T04:08:25Z              11m
```

## Step 3: Wait until the job is completed and check the training results

Wait until the job is completed. It might take several minutes.

```sh
kubectl get rayjob
# NAME                   JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME               AGE
# rayjob-pytorch-mnist   SUCCEEDED    Complete            2024-06-17T04:08:25Z   2024-06-17T04:22:21Z   16m
```

After seeing `JOB_STATUS` marked as `SUCCEEDED`, you can check the training logs:

```sh
# check pods name
kubectl get pods
# NAME                                                      READY   STATUS      RESTARTS   AGE
# kuberay-operator-6dddd689fb-ksmcs                         1/1     Running     0          113m
# pytorch-mnist-raycluster-rkdmq-worker-small-group-c8bwx   1/1     Running     0          38m
# pytorch-mnist-raycluster-rkdmq-worker-small-group-s7wvm   1/1     Running     0          38m
# rayjob-pytorch-mnist-nxmj2                                0/1     Completed   0          38m
# rayjob-pytorch-mnist-raycluster-rkdmq-head-m4dsl          1/1     Running     0          38m

# check training logs
kubectl logs -f rayjob-pytorch-mnist-nxmj2

# 2024-06-16 22:23:01,047 INFO cli.py:36 -- Job submission server address: http://rayjob-pytorch-mnist-raycluster-rkdmq-head-svc.default.svc.cluster.local:8265
# 2024-06-16 22:23:01,844 SUCC cli.py:60 -- -------------------------------------------------------
# 2024-06-16 22:23:01,844 SUCC cli.py:61 -- Job 'rayjob-pytorch-mnist-l6ccc' submitted successfully
# 2024-06-16 22:23:01,844 SUCC cli.py:62 -- -------------------------------------------------------
# ...
# (RayTrainWorker pid=1138, ip=10.244.0.18) 
#   0%|          | 0/26421880 [00:00<?, ?it/s]
# (RayTrainWorker pid=1138, ip=10.244.0.18) 
#   0%|          | 32768/26421880 [00:00<01:27, 301113.97it/s]
# ...
# Training finished iteration 10 at 2024-06-16 22:33:05. Total running time: 7min 9s
# ╭───────────────────────────────╮
# │ Training result               │
# ├───────────────────────────────┤
# │ checkpoint_dir_name           │
# │ time_this_iter_s      28.2635 │
# │ time_total_s          423.388 │
# │ training_iteration         10 │
# │ accuracy               0.8748 │
# │ loss                  0.35477 │
# ╰───────────────────────────────╯

# Training completed after 10 iterations at 2024-06-16 22:33:06. Total running time: 7min 10s

# Training result: Result(
#   metrics={'loss': 0.35476621258825347, 'accuracy': 0.8748},
#   path='/home/ray/ray_results/TorchTrainer_2024-06-16_22-25-55/TorchTrainer_122aa_00000_0_2024-06-16_22-25-55',
#   filesystem='local',
#   checkpoint=None
# )
# ...
```

## Clean-up

Delete your RayCluster and KubeRay with the following commands:

```sh
# check <raycluster-name> by `kubectl get raycluster`
kubectl delete raycluster <raycluster-name>

# Please make sure the ray cluster has already been removed before delete the operator.
helm uninstall kuberay-operator

# remove kubernetes cluster created by kind
kind delete cluster
```