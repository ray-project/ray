(kuberay-mnist-training-example)=

# Train a PyTorch model on Fashion MNIST with CPUs on Kubernetes

This example runs distributed training of a PyTorch model on Fashion MNIST with Ray Train. See [Train a PyTorch model on Fashion MNIST](train-pytorch-fashion-mnist) for more details.

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/). If you already have a Kubernetes cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Step 3: Create a RayJob

A RayJob consists of a RayCluster custom resource and a job that can you can submit to the RayCluster. With RayJob, KubeRay creates a RayCluster and submits a job when the cluster is ready. The following is a CPU-only RayJob description YAML file for MNIST training on a PyTorch model.

```sh
# Download `ray-job.pytorch-mnist.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-mnist/ray-job.pytorch-mnist.yaml
```

You might need to adjust some fields in the RayJob description YAML file so that it can run in your environment:
* `replicas` under `workerGroupSpecs` in `rayClusterSpec`: This field specifies the number of worker Pods that KubeRay schedules to the Kubernetes cluster. Each worker Pod and the head Pod, as described in the `template` field, requires 2 CPUs. A RayJob submitter Pod requires 1 CPU. For example, if your machine has 8 CPUs, the maximum `replicas` value is 2 to allow all Pods to reach the `Running` status.
* `NUM_WORKERS` under `runtimeEnvYAML` in `spec`: This field indicates the number of Ray actors to launch (see `ScalingConfig` in this [Document](ray-train-configs-api) for more information). Each Ray actor must be served by a worker Pod in the Kubernetes cluster. Therefore, `NUM_WORKERS` must be less than or equal to `replicas`.

```sh
# `replicas` and `NUM_WORKERS` set to 2.
# Create a RayJob.
kubectl apply -f ray-job.pytorch-mnist.yaml

# Check existing Pods: According to `replicas`, there should be 2 worker Pods.
# Make sure all the Pods are in the `Running` status.
kubectl get pods
# NAME                                                      READY   STATUS    RESTARTS   AGE
# kuberay-operator-6dddd689fb-ksmcs                         1/1     Running   0          6m8s
# pytorch-mnist-raycluster-rkdmq-worker-small-group-c8bwx   1/1     Running   0          5m32s
# pytorch-mnist-raycluster-rkdmq-worker-small-group-s7wvm   1/1     Running   0          5m32s
# rayjob-pytorch-mnist-nxmj2                                1/1     Running   0          4m17s
# rayjob-pytorch-mnist-raycluster-rkdmq-head-m4dsl          1/1     Running   0          5m32s
```

Check that the RayJob is in the `RUNNING` status:

```sh
kubectl get rayjob
# NAME                   JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME   AGE
# rayjob-pytorch-mnist   RUNNING      Running             2024-06-17T04:08:25Z              11m
```

## Step 4: Wait until the RayJob completes and check the training results

Wait until the RayJob completes. It might take several minutes.

```sh
kubectl get rayjob
# NAME                   JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME               AGE
# rayjob-pytorch-mnist   SUCCEEDED    Complete            2024-06-17T04:08:25Z   2024-06-17T04:22:21Z   16m
```

After seeing `JOB_STATUS` marked as `SUCCEEDED`, you can check the training logs:

```sh
# Check Pods name.
kubectl get pods
# NAME                                                      READY   STATUS      RESTARTS   AGE
# kuberay-operator-6dddd689fb-ksmcs                         1/1     Running     0          113m
# pytorch-mnist-raycluster-rkdmq-worker-small-group-c8bwx   1/1     Running     0          38m
# pytorch-mnist-raycluster-rkdmq-worker-small-group-s7wvm   1/1     Running     0          38m
# rayjob-pytorch-mnist-nxmj2                                0/1     Completed   0          38m
# rayjob-pytorch-mnist-raycluster-rkdmq-head-m4dsl          1/1     Running     0          38m

# Check training logs.
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

## Clean up

Delete your RayJob with the following command:

```sh
kubectl delete -f ray-job.pytorch-mnist.yaml
```