(kuberay-rayjob-quickstart)=

# RayJob Quickstart

:::{warning}
RayJob support in KubeRay v0.x is in alpha.
:::

## Prerequisites

* Ray 1.10 or higher
* KubeRay v0.3.0+. (v0.6.0+ is recommended)

## What is a RayJob?

A RayJob manages two aspects:

* **RayCluster**: Manages resources in a Kubernetes cluster.
* **Job**: A Kubernetes Job runs `ray job submit` to submit a Ray job to the RayCluster.

## What does the RayJob provide?

* **Kubernetes-native support for Ray clusters and Ray jobs**: You can use a Kubernetes config to define a Ray cluster and job, and use `kubectl` to create them. The cluster can be deleted automatically once the job is finished.

## RayJob Configuration

* `entrypoint` - The shell command to run for this job.
* `rayClusterSpec` - The spec for the **RayCluster** to run the job on.
* `jobId` - _(Optional)_ Job ID to specify for the job. If not provided, one will be generated.
* `metadata` - _(Optional)_ Arbitrary user-provided metadata for the job.
* `runtimeEnvYAML` - _(Optional)_ The runtime environment configuration provided as a multi-line YAML string. _(New in KubeRay version 1.0.)_
* `shutdownAfterJobFinishes` - _(Optional)_ whether to recycle the cluster after the job finishes. Defaults to false.
* `ttlSecondsAfterFinished` - _(Optional)_ TTL to clean up the cluster. This only works if `shutdownAfterJobFinishes` is set.
* `submitterPodTemplate` - _(Optional)_ Pod template spec for the pod that runs `ray job submit` against the Ray cluster.
* `entrypointNumCpus` - _(Optional)_ Specifies the quantity of CPU cores to reserve for the entrypoint command. _(New in KubeRay version 1.0.)_
* `entrypointNumGpus` - _(Optional)_ Specifies the number of GPUs to reserve for the entrypoint command. _(New in KubeRay version 1.0.)_
* `entrypointResources` - _(Optional)_ A json formatted dictionary to specify custom resources and their quantity. _(New in KubeRay version 1.0.)_
* `runtimeEnv` - [DEPRECATED] _(Optional)_ base64-encoded string of the runtime env json string.

## Example: Run a simple Ray job with RayJob

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.23.0
```

## Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.
Please note that the YAML file in this example uses `serveConfigV2` to specify a multi-application Serve config, which is supported starting from KubeRay v0.6.0.

## Step 3: Install a RayJob

```sh
# Step 3.1: Download `ray_v1alpha1_rayjob.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.0.0/ray-operator/config/samples/ray_v1alpha1_rayjob.yaml

# Step 3.2: Create a RayJob
kubectl apply -f ray_v1alpha1_rayjob.yaml
```

## Step 4: Verify the Kubernetes cluster status

```shell
# Step 4.1: List all RayJob custom resources in the `default` namespace.
kubectl get rayjob

# [Example output]
# NAME            AGE
# rayjob-sample   7s

# Step 4.2: List all RayCluster custom resources in the `default` namespace.
kubectl get raycluster

# [Example output]
# NAME                                 DESIRED WORKERS   AVAILABLE WORKERS   STATUS   AGE
# rayservice-sample-raycluster-6mj28   1                 1                   ready    2m27s

# Step 4.3: List all Pods in the `default` namespace.
# The Pod created by the Kubernetes Job will be terminated after the Kubernetes Job finishes.
kubectl get pods

# [Example output]
# kuberay-operator-7456c6b69b-rzv25                         1/1     Running     0          3m57s
# rayjob-sample-lk9jx                                       0/1     Completed   0          2m49s => Pod created by a Kubernetes Job
# rayjob-sample-raycluster-9c546-head-gdxkg                 1/1     Running     0          3m46s
# rayjob-sample-raycluster-9c546-worker-small-group-nfbxm   1/1     Running     0          3m46s

# Step 4.4: Check the status of the RayJob.
# The field `jobStatus` in the RayJob custom resource will be updated to `SUCCEEDED` once the job finishes.
kubectl get rayjobs.ray.io rayjob-sample -o json | jq '.status.jobStatus'

# [Example output]
# "SUCCEEDED"
```

The KubeRay operator will create a RayCluster as defined by the `rayClusterSpec` custom resource, as well as a Kubernetes Job to submit a Ray job to the RayCluster.
The Ray job is defined in the `entrypoint` field of the RayJob custom resource.
In this example, the `entrypoint` is `python /home/ray/samples/sample_code.py`,
and `sample_code.py` is a Python script stored in a Kubernetes ConfigMap mounted to the head Pod of the RayCluster.
Since the default value of `shutdownAfterJobFinishes` is false, the RayCluster will not be deleted after the job finishes.

## Step 5: Check the output of the Ray job

```sh
kubectl logs -l=job-name=rayjob-sample

# [Example output]
# 2023-08-21 17:08:22,530 INFO cli.py:27 -- Job submission server address: http://rayjob-sample-raycluster-9c546-head-svc.default.svc.cluster.local:8265
# 2023-08-21 17:08:23,726 SUCC cli.py:33 -- ------------------------------------------------
# 2023-08-21 17:08:23,727 SUCC cli.py:34 -- Job 'rayjob-sample-5ntcr' submitted successfully
# 2023-08-21 17:08:23,727 SUCC cli.py:35 -- ------------------------------------------------
# 2023-08-21 17:08:23,727 INFO cli.py:226 -- Next steps
# 2023-08-21 17:08:23,727 INFO cli.py:227 -- Query the logs of the job:
# 2023-08-21 17:08:23,727 INFO cli.py:229 -- ray job logs rayjob-sample-5ntcr
# 2023-08-21 17:08:23,727 INFO cli.py:231 -- Query the status of the job:
# 2023-08-21 17:08:23,727 INFO cli.py:233 -- ray job status rayjob-sample-5ntcr
# 2023-08-21 17:08:23,727 INFO cli.py:235 -- Request the job to be stopped:
# 2023-08-21 17:08:23,728 INFO cli.py:237 -- ray job stop rayjob-sample-5ntcr
# 2023-08-21 17:08:23,739 INFO cli.py:245 -- Tailing logs until the job exits (disable with --no-wait):
# 2023-08-21 17:08:34,288 INFO worker.py:1335 -- Using address 10.244.0.6:6379 set in the environment variable RAY_ADDRESS
# 2023-08-21 17:08:34,288 INFO worker.py:1452 -- Connecting to existing Ray cluster at address: 10.244.0.6:6379...
# 2023-08-21 17:08:34,302 INFO worker.py:1633 -- Connected to Ray cluster. View the dashboard at http://10.244.0.6:8265
# test_counter got 1
# test_counter got 2
# test_counter got 3
# test_counter got 4
# test_counter got 5
# 2023-08-21 17:08:46,040 SUCC cli.py:33 -- -----------------------------------
# 2023-08-21 17:08:46,040 SUCC cli.py:34 -- Job 'rayjob-sample-5ntcr' succeeded
# 2023-08-21 17:08:46,040 SUCC cli.py:35 -- -----------------------------------
```

The Python script `sample_code.py` used by `entrypoint` is a simple Ray script that executes a counter's increment function 5 times.


## Step 6: Cleanup

```sh
# Step 6.1: Delete the RayJob
kubectl delete -f ray_v1alpha1_rayjob.yaml

# Step 6.2: Delete the KubeRay operator
helm uninstall kuberay-operator

# Step 6.3: Delete the Kubernetes cluster
kind delete cluster
```

## Advanced Usage

The Pod template for the Kubernetes Job that runs `ray job submit` can be customized by setting the `submitterPodTemplate` field in the RayJob custom resource.  See <https://raw.githubusercontent.com/ray-project/kuberay/f6546651ff37140211913214642ce7a1d8cf20e2/ray-operator/config/samples/ray_v1alpha1_rayjob.yaml> for an example (commented out in this file).

If `submitterPodTemplate` is unspecified, the Pod will consist of a container named `ray-job-submitter` with image matching that of the Ray head, resource requests of 500m CPU and 200MiB memory, and limits of 1 CPU and 1GiB memory.
