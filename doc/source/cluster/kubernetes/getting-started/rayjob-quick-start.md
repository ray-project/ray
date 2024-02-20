(kuberay-rayjob-quickstart)=

# RayJob Quickstart

## Prerequisites

* KubeRay v0.6.0 or higher
  * KubeRay v0.6.0 or v1.0.0: Ray 1.10 or higher.
  * KubeRay v1.1.0 is highly recommended: Ray 2.8.0 or higher. This document is mainly for KubeRay v1.1.0.

## What's a RayJob?

A RayJob manages two aspects:

* **RayCluster**: A RayCluster custom resource manages all Pods in a Ray cluster, including a head Pod and multiple worker Pods.
* **Job**: A Kubernetes Job runs `ray job submit` to submit a Ray job to the RayCluster.

## What does the RayJob provide?

With RayJob, KubeRay automatically creates a RayCluster and submits a job when the cluster is ready. You can also configure RayJob to automatically delete the RayCluster once the Ray job finishes.

To understand the following content better, you should understand the difference between:
* RayJob: A Kubernetes custom resource definition (CRD) provided by KubeRay.
* Ray job: A Ray job is a packaged Ray application that can run on a remote Ray cluster. See [this document](jobs-overview) for more details.
* Submitter: The submitter is a Kubernetes Job that runs `ray job submit` to submit a Ray job to the RayCluster.

## RayJob Configuration

* RayCluster configuration
  * `rayClusterSpec` - Defines the **RayCluster** custom resource to run the Ray job on.
* Ray job configuration
  * `entrypoint` - The submitter runs `ray job submit --address ... --submission-id ... -- $entrypoint` to submit a Ray job to the RayCluster.
  * `runtimeEnvYAML` - _(Optional)_ A runtime environment that describes the dependencies the Ray job needs to run, including files, packages, environment variables, and more. Provide the configuration as a multi-line YAML string. See {ref}`Runtime Environments <runtime-environments>` for more details. _(New in KubeRay version 1.0.0)_
  * `jobId` - _(Optional)_ Defines the submission ID for the Ray job. If not provided, KubeRay generates one automatically. See {ref}`Ray Jobs CLI API Reference <ray-job-submission-cli-ref>` for more details about the submission ID.
  * `metadata` - _(Optional)_ See {ref}`Ray Jobs CLI API Reference <ray-job-submission-cli-ref>` for more details about the `--metadata-json` option.
  * `entrypointNumCpus` / `entrypointNumGpus` / `entrypointResources` _(Optional)_: See {ref}`Ray Jobs CLI API Reference <ray-job-submission-cli-ref>` for more details.
* Submitter configuration
  * `submitterPodTemplate` - _(Optional)_ Defines the Pod template for the submitter Kubernetes Job.
    * `RAY_DASHBOARD_ADDRESS` - The KubeRay operator injects this environment variable to the submitter Pod. The value is `$HEAD_SERVICE:$DASHBOARD_PORT`.
    * `RAY_JOB_SUBMISSION_ID` - The KubeRay operator injects this environment variable to the submitter Pod. The value is the `RayJob.Status.JobId` of the RayJob.
    * Example: `ray job submit --address=http://$RAY_DASHBOARD_ADDRESS --submission-id=$RAY_JOB_SUBMISSION_ID ...`
* Automatic resource cleanup
  * `shutdownAfterJobFinishes` - _(Optional)_ Determines whether to recycle the RayCluster and the submitter after the Ray job finishes. The default value is false.
  * `ttlSecondsAfterFinished` - _(Optional)_ Only works if `shutdownAfterJobFinishes` is true. The KubeRay operator deletes the RayCluster and the submitter `ttlSecondsAfterFinished` seconds after the Ray job finishes. The default value is 0.

## Example: Run a simple Ray job with RayJob

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.23.0
```

## Step 2: Install the KubeRay operator

Follow the [RayCluster Quickstart](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

## Step 3: Install a RayJob

```sh
# Step 3.1: Download `ray-job.sample.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.1.0/ray-operator/config/samples/ray-job.sample.yaml

# Step 3.2: Create a RayJob
kubectl apply -f ray-job.sample.yaml
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
# NAME                             DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
# rayjob-sample-raycluster-tlsxc   1                 1                   400m   0        0      ready    91m

# Step 4.3: List all Pods in the `default` namespace.
# The Pod created by the Kubernetes Job will be terminated after the Kubernetes Job finishes.
kubectl get pods

# [Example output]
# kuberay-operator-7456c6b69b-rzv25                         1/1     Running     0          3m57s
# rayjob-sample-lk9jx                                       0/1     Completed   0          2m49s => Pod created by a Kubernetes Job
# rayjob-sample-raycluster-9c546-head-gdxkg                 1/1     Running     0          3m46s
# rayjob-sample-raycluster-9c546-worker-small-group-nfbxm   1/1     Running     0          3m46s

# Step 4.4: Check the status of the RayJob.
# The field `jobStatus` in the RayJob custom resource will be updated to `SUCCEEDED` and `jobDeploymentStatus`
# should be `Complete` once the job finishes.
kubectl get rayjobs.ray.io rayjob-sample -o jsonpath='{.status.jobStatus}'
# [Expected output]: "SUCCEEDED"

kubectl get rayjobs.ray.io rayjob-sample -o jsonpath='{.status.jobDeploymentStatus}'
# [Expected output]: "Complete"
```

The KubeRay operator creates a RayCluster custom resource based on the `rayClusterSpec` and a submitter Kubernetes Job to submit a Ray job to the RayCluster.
In this example, the `entrypoint` is `python /home/ray/samples/sample_code.py`, and `sample_code.py` is a Python script stored in a Kubernetes ConfigMap mounted to the head Pod of the RayCluster.
Because the default value of `shutdownAfterJobFinishes` is false, the KubeRay operator doesn't delete the RayCluster or the submitter when the Ray job finishes.

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

## Step 6: Delete the RayJob

```sh
kubectl delete -f ray-job.sample.yaml
```

## Step 7: Create a RayJob with `shutdownAfterJobFinishes` set to true

```sh
# Step 7.1: Download `ray-job.shutdown.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.1.0/ray-operator/config/samples/ray-job.shutdown.yaml

# Step 7.2: Create a RayJob
kubectl apply -f ray-job.shutdown.yaml
```

The `ray-job.shutdown.yaml` defines a RayJob custom resource with `shutdownAfterJobFinishes: true` and `ttlSecondsAfterFinished: 10`.
Hence, the KubeRay operator deletes the RayCluster and the submitter 10 seconds after the Ray job finishes.

## Step 8: Check the RayJob status

```sh
# Wait until `jobStatus` is `SUCCEEDED` and `jobDeploymentStatus` is `Complete`.
kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobDeploymentStatus}'
kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobStatus}'
```

## Step 9: Check if the KubeRay operator deletes the RayCluster and the submitter

```sh
# List the RayCluster custom resources in the `default` namespace. The RayCluster and the submitter Kubernetes 
# Job associated with the RayJob `rayjob-sample-shutdown` should be deleted.
kubectl get raycluster
kubectl get jobs
```

## Step 10: Clean up

```sh
# Step 10.1: Delete the RayJob
kubectl delete -f ray-job.shutdown.yaml

# Step 10.2: Delete the KubeRay operator
helm uninstall kuberay-operator

# Step 10.3: Delete the Kubernetes cluster
kind delete cluster
```

## Advanced Usage

The Pod template for the Kubernetes Job that runs `ray job submit` can be customized by setting the `submitterPodTemplate` field in the RayJob custom resource.  See <https://raw.githubusercontent.com/ray-project/kuberay/f6546651ff37140211913214642ce7a1d8cf20e2/ray-operator/config/samples/ray_v1alpha1_rayjob.yaml> for an example (commented out in this file).

If `submitterPodTemplate` is unspecified, the Pod will consist of a container named `ray-job-submitter` with image matching that of the Ray head, resource requests of 500m CPU and 200MiB memory, and limits of 1 CPU and 1GiB memory.
