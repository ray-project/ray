(kuberay-image-resize-example)=

# Image Resize Example

In this guide, we show you how to run a sample Ray workload reading images from Google Cloud Storage and resizing them on Kubernetes infrastructure. The image resources are managed using Ray Data, a key component of the Ray ecosystem for handling large datasets.

## Step 1: Install the KubeRay operator

Follow the [RayCluster Quickstart](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

## Step 2: Install the RayJob

```shell
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-data-image-resize/ray-job.image-resize.yaml
```

You might need to modify the following field in the Rayjob description YAML file based on your machine. 
* `replica` under `workerGroupSpecs` in `rayClusterSpec`: The field specifies the number of worker Pods that will be scheduled to the Kubernetes cluster. Each worker Pod and the head Pod requests 2 CPUs as specified in the `template` field. A RayJob submitter Pod require 1 CPU.

The following example output is based on setting `replica` to 2.

## Step 3: Verify the Kubernetes cluster status

Check RayJob status. Expected status is `RUNNING` under `JOB STATUS` and `DEPLOYMENT STATUS`.

```shell
kubectl get rayjob

# [Example output]
# NAME           JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME   AGE
# image-resize   RUNNING      Running             2024-07-02T14:01:50Z              6m40s
```

Check RayCluster status. Here are the expected state.
* `DESIRED WORKERS`: the same as `replicas`
* `AVAILABLE WORKERS`: the same as `replicas`
* `CPUS`: total CPU requirement of the worker Pods and the head Pod.
* `MEMORY`: total memory requiment of the worker Pods and the head Pod.

```shell
kubectl get raycluster

# [Example output]
# NAME                            DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
# image-resize-raycluster-2bmfw   2                 2                   6      12Gi     0      ready    7m45s
```

List all pods in the `default` namespace.

```shell
kubectl get pods

# [Example output]
# NAME                                                     READY   STATUS      RESTARTS   AGE
# image-resize-bhwc7                                       0/1     Completed   0          7m32s
# image-resize-raycluster-2bmfw-head-bddxx                 1/1     Running     0          8m22s
# image-resize-raycluster-2bmfw-worker-small-group-dg29d   1/1     Running     0          8m22s
# image-resize-raycluster-2bmfw-worker-small-group-ldwcf   1/1     Running     0          8m22s
# kuberay-operator-6dddd689fb-l5gz4                        1/1     Running     0          128m
```

Check the status of the RayJob.

```shell
# The field `jobStatus` in the RayJob custom resource will be updated to `SUCCEEDED` and `jobDeploymentStatus`
# should be `Complete` once the job finishes.
kubectl get rayjobs.ray.io image-resize -o jsonpath='{.status.jobStatus}'
# [Expected output]: "SUCCEEDED"

kubectl get rayjobs.ray.io image-resize -o jsonpath='{.status.jobDeploymentStatus}'
# [Expected output]: "Complete"
```

## Step 4: Check the output of the Ray job

```shell
kubectl logs -l=job-name=image-resize

# [Example output]
# Running: 2.0/6.0 CPU, 0.0/0.0 GPU, 0.0 MiB/846.0 MiB object_store_memory:  97%|█████████▋| 63/65 [02:07<00:59, 29.93s/it]
# Running: 1.0/6.0 CPU, 0.0/0.0 GPU, 1.0 MiB/846.0 MiB object_store_memory:  97%|█████████▋| 63/65 [02:37<00:59, 29.93s/it]
# Running: 1.0/6.0 CPU, 0.0/0.0 GPU, 1.0 MiB/846.0 MiB object_store_memory:  98%|█████████▊| 64/65 [02:37<00:23, 23.47s/it]
# Running: 1.0/6.0 CPU, 0.0/0.0 GPU, 0.0 MiB/846.0 MiB object_store_memory:  98%|█████████▊| 64/65 [02:37<00:23, 23.47s/it]
# Running: 0.0/6.0 CPU, 0.0/0.0 GPU, 1.0 MiB/846.0 MiB object_store_memory:  98%|█████████▊| 64/65 [02:37<00:23, 23.47s/it]
# Running: 0.0/6.0 CPU, 0.0/0.0 GPU, 1.0 MiB/846.0 MiB object_store_memory: 100%|██████████| 65/65 [02:37<00:00, 18.06s/it]
                                                                                                                         
# 2024-07-02 07:10:03,399 SUCC cli.py:60 -- ----------------------------------
# 2024-07-02 07:10:03,399 SUCC cli.py:61 -- Job 'image-resize-j4kv5' succeeded
# 2024-07-02 07:10:03,399 SUCC cli.py:62 -- ----------------------------------
```

## Step 5: Clean up

```shell
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-data-image-resize/ray-job.image-resize.yaml
```
