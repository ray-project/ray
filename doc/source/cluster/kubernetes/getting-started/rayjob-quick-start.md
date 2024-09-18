(kuberay-rayjob-quickstart)=

# RayJob Quickstart

## Prerequisites

* KubeRay v0.6.0 or higher
  * KubeRay v0.6.0 or v1.0.0: Ray 1.10 or higher.
  * KubeRay v1.1.1 is highly recommended: Ray 2.8.0 or higher. This document is mainly for KubeRay v1.1.1.

## What's a RayJob?

A RayJob manages two aspects:

* **RayCluster**: A RayCluster custom resource manages all Pods in a Ray cluster, including a head Pod and multiple worker Pods.
* **Job**: A Kubernetes Job runs `ray job submit` to submit a Ray job to the RayCluster.

## What does the RayJob provide?

With RayJob, KubeRay automatically creates a RayCluster and submits a job when the cluster is ready. You can also configure RayJob to automatically delete the RayCluster once the Ray job finishes.

To understand the following content better, you should understand the difference between:
* RayJob: A Kubernetes custom resource definition provided by KubeRay.
* Ray job: A Ray job is a packaged Ray application that can run on a remote Ray cluster. See [this document](jobs-overview) for more details.
* Submitter: The submitter is a Kubernetes Job that runs `ray job submit` to submit a Ray job to the RayCluster.

## RayJob Configuration

* RayCluster configuration
  * `rayClusterSpec` - Defines the **RayCluster** custom resource to run the Ray job on.
* Ray job configuration
  * `entrypoint` - The submitter runs `ray job submit --address ... --submission-id ... -- $entrypoint` to submit a Ray job to the RayCluster.
  * `runtimeEnvYAML` (Optional): A runtime environment that describes the dependencies the Ray job needs to run, including files, packages, environment variables, and more. Provide the configuration as a multi-line YAML string.
  Example:

    ```yaml
    spec:
      runtimeEnvYAML: |
        pip:
          - requests==2.26.0
          - pendulum==2.1.2
        env_vars:
          KEY: "VALUE"
    ```

  See {ref}`Runtime Environments <runtime-environments>` for more details. _(New in KubeRay version 1.0.0)_
  * `jobId` (Optional): Defines the submission ID for the Ray job. If not provided, KubeRay generates one automatically. See {ref}`Ray Jobs CLI API Reference <ray-job-submission-cli-ref>` for more details about the submission ID.
  * `metadata` (Optional): See {ref}`Ray Jobs CLI API Reference <ray-job-submission-cli-ref>` for more details about the `--metadata-json` option.
  * `entrypointNumCpus` / `entrypointNumGpus` / `entrypointResources` (Optional): See {ref}`Ray Jobs CLI API Reference <ray-job-submission-cli-ref>` for more details.
  * `backoffLimit` (Optional, added in version 1.2.0): Specifies the number of retries before marking this RayJob failed. Each retry creates a new RayCluster. The default value is 0.
* Submission configuration
  * `submissionMode` (Optional): `submissionMode` specifies how RayJob submits the Ray job to the RayCluster. In "K8sJobMode", the KubeRay operator creates a submitter Kubernetes Job to submit the Ray job. In "HTTPMode", the KubeRay operator sends a request to the RayCluster to create a Ray job. The default value is "K8sJobMode".
  * `submitterPodTemplate` (Optional): Defines the Pod template for the submitter Kubernetes Job. This field is only effective when `submissionMode` is "K8sJobMode".
    * `RAY_DASHBOARD_ADDRESS` - The KubeRay operator injects this environment variable to the submitter Pod. The value is `$HEAD_SERVICE:$DASHBOARD_PORT`.
    * `RAY_JOB_SUBMISSION_ID` - The KubeRay operator injects this environment variable to the submitter Pod. The value is the `RayJob.Status.JobId` of the RayJob.
    * Example: `ray job submit --address=http://$RAY_DASHBOARD_ADDRESS --submission-id=$RAY_JOB_SUBMISSION_ID ...`
    * See [ray-job.sample.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-job.sample.yaml) for more details.
  * `submitterConfig` (Optional): Additional configurations for the submitter Kubernetes Job.
    * `backoffLimit` (Optional, added in version 1.2.0): The number of retries before marking the submitter Job as failed. The default value is 2.
* Automatic resource cleanup
  * `shutdownAfterJobFinishes` (Optional): Determines whether to recycle the RayCluster after the Ray job finishes. The default value is false.
  * `ttlSecondsAfterFinished` (Optional): Only works if `shutdownAfterJobFinishes` is true. The KubeRay operator deletes the RayCluster and the submitter `ttlSecondsAfterFinished` seconds after the Ray job finishes. The default value is 0.
  * `activeDeadlineSeconds` (Optional): If the RayJob doesn't transition the `JobDeploymentStatus` to `Complete` or `Failed` within `activeDeadlineSeconds`, the KubeRay operator transitions the `JobDeploymentStatus` to `Failed`, citing `DeadlineExceeded` as the reason.
  * `DELETE_RAYJOB_CR_AFTER_JOB_FINISHES` (Optional, added in version 1.2.0): Set this environment variable for the KubeRay operator, not the RayJob resource. If you set this environment variable to true, the RayJob custom resource itself is deleted if you also set `shutdownAfterJobFinishes` to true. Note that KubeRay deletes all resources created by the RayJob, including the Kubernetes Job.

## Example: Run a simple Ray job with RayJob

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install the KubeRay operator

Follow the [RayCluster Quickstart](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

## Step 3: Install a RayJob

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.1.1/ray-operator/config/samples/ray-job.sample.yaml
```

## Step 4: Verify the Kubernetes cluster status

```shell
# Step 4.1: List all RayJob custom resources in the `default` namespace.
kubectl get rayjob

# [Example output]
# NAME            JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME   AGE
# rayjob-sample                Running             2024-03-02T19:09:15Z              96s

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
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.1.1/ray-operator/config/samples/ray-job.sample.yaml
```

## Step 7: Create a RayJob with `shutdownAfterJobFinishes` set to true

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.1.1/ray-operator/config/samples/ray-job.shutdown.yaml
```

The `ray-job.shutdown.yaml` defines a RayJob custom resource with `shutdownAfterJobFinishes: true` and `ttlSecondsAfterFinished: 10`.
Hence, the KubeRay operator deletes the RayCluster 10 seconds after the Ray job finishes. Note that the submitter job is not deleted
because it contains the ray job logs and does not use any cluster resources once completed. In addition, the submitter job will always
be cleaned up when the RayJob is eventually deleted due to its owner reference back to the RayJob.

## Step 8: Check the RayJob status

```sh
# Wait until `jobStatus` is `SUCCEEDED` and `jobDeploymentStatus` is `Complete`.
kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobDeploymentStatus}'
kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobStatus}'
```

## Step 9: Check if the KubeRay operator deletes the RayCluster

```sh
# List the RayCluster custom resources in the `default` namespace. The RayCluster
# associated with the RayJob `rayjob-sample-shutdown` should be deleted.
kubectl get raycluster
```

## Step 10: Clean up

```sh
# Step 10.1: Delete the RayJob
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.1.1/ray-operator/config/samples/ray-job.shutdown.yaml

# Step 10.2: Delete the KubeRay operator
helm uninstall kuberay-operator

# Step 10.3: Delete the Kubernetes cluster
kind delete cluster
```

## Next steps

* [RayJob Batch Inference Example](kuberay-batch-inference-example)
* [Priority Scheduling with RayJob and Kueue](kuberay-kueue-priority-scheduling-example)
* [Gang Scheduling with RayJob and Kueue](kuberay-kueue-gang-scheduling-example)
