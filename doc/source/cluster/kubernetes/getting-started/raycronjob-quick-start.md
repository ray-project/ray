(kuberay-raycronjob-quickstart)=

# RayCronJob Quickstart

## Prerequisites

* This feature requires KubeRay version 1.6.0 or newer, and it's in alpha testing.
    * A running Kubernetes cluster.
    * The KubeRay operator installed and running in your cluster.

## What is a RayCronJob?

A `RayCronJob` is a Custom Resource (CR) that allows you to run `RayJob` workloads on a recurring, time-based schedule. It is heavily inspired by the native Kubernetes `CronJob` and brings automated, scheduled execution to your distributed Ray applications.


This is particularly useful for recurring tasks such as scheduled model retraining, nightly batch inferences, or regular data processing pipelines.


## RayCronJob Configuration

The `RayCronJob` CRD acts as an automated scheduler specifically designed to create and manage **RayJob** custom resources on a recurring basis. It does not execute workloads directly; instead, it acts as a factory that generates a new `RayJob` every time its schedule triggers.

* `schedule` - The cron schedule string defining when a new Ray job should be created and run (e.g., `* * * * *` for every minute).
* `jobTemplate` - Wraps a standard **RayJob** spec that the controller will use for each scheduled run. It supports the same fields as a RayJob spec. See the standard [RayJob Configuration](kuberay-rayjob-quickstart) documentation for the complete list of supported fields within the `jobTemplate`.*
* `suspend` (Optional): If `suspend` is true, the controller suspends the scheduling of future jobs. This does not apply to or interrupt any `RayJob`s that have already been created and are currently running.

## How to Configure a RayCronJob

Configuring a `RayCronJob` requires wrapping a standard `RayJob` specification inside a `jobTemplate`, alongside your scheduling parameters.

The high-level structure looks like this:

```yaml
apiVersion: ray.io/v1
kind: RayCronJob
metadata:
  name: example-raycronjob
spec:
  schedule: "*/5 * * * *" # Run every 5 minutes
  jobTemplate:
    # Everything below here is a standard RayJob spec
    entrypoint: python /home/ray/samples/sample_code.py
    # ... (RayCluster spec, runtimeEnv, etc.)
```

## How to Run an Simple RayCronJob

Let's deploy a simple `RayCronJob` that executes a short Python script every minute.

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator
Install the KubeRay operator, following [these instructions](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html). The minimum version for this guide is v1.5.1. To use this feature, the `RayServiceIncrementalUpgrade` feature gate must be enabled. To enable the feature gate when installing the kuberay operator, run the following command: 

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install KubeRay operator v1.6.0 with the RayCronJob feature gate enabled
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.6.0 \
  --set "featureGates[0].name=RayCronJob" \
  --set "featureGates[0].enabled=true"
```

### Step 3: Install a RayCronJob

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.6.0/ray-operator/config/samples/ray-cronjob.sample.yaml
```

### Step 4: Monitor the RayCronJob

Check the status of your `RayCronJob`. The `SCHEDULE` field should be visible immediately, while `LAST SCHEDULE` may be empty until the first scheduled run is triggered.

```sh
kubectl get raycronjob raycronjob-sample

#You should see output listing the RayCronJob created
# [Example output]

# NAME                SCHEDULE    LAST SCHEDULE   AGE   SUSPEND
# raycronjob-sample   * * * * *                   10s   
```

Because our schedule is `* * * * *`, a new `RayJob` will be generated at the start of the next minute. You can watch the `RayJob` instances being created:

```shell
kubectl get rayjob -w
# [Example output]
# NAME                      JOB STATUS   DEPLOYMENT STATUS   RAY CLUSTER NAME                 START TIME             END TIME   AGE
# raycronjob-sample-l76h8                Initializing        raycronjob-sample-l76h8-hjtrs   2026-04-03T05:57:00Z              2s
# raycronjob-sample-l76h8   RUNNING      Running             raycronjob-sample-l76h8-hjtrs   2026-04-03T05:57:00Z              48s
# raycronjob-sample-l76h8   SUCCEEDED    Complete            raycronjob-sample-l76h8-hjtrs   2026-04-03T05:57:00Z   2026-04-03T05:58:02Z   62s
# raycronjob-sample-pct47                Initializing        raycronjob-sample-pct47-bdspj   2026-04-03T05:58:00Z              0s
# (Press Ctrl+C to stop watching once the job completes)
```

### Step 5: Verify the Output

Once a generated `RayJob` completes, you can inspect the logs of the job submitter pod to verify that the Python script ran successfully:

```shell
# Step 5.1: Find the specific pod running the job (usually named after the RayJob with a suffix)
kubectl get pods
# [Example output]
# NAME                                                     READY   STATUS      RESTARTS      AGE
# kuberay-operator-7fc88c69f5-n2g5k                        1/1     Running     1 (12m ago)   45h
# raycronjob-sample-gmsnw-d6n2r-head-hdtmt                 0/1     Pending     0             61s
# raycronjob-sample-gmsnw-d6n2r-small-group-worker-nd56f   0/1     Init:0/1    0             61s
# raycronjob-sample-l76h8-hjtrs-head-9b568                 1/1     Running     0             61s
# raycronjob-sample-l76h8-hjtrs-small-group-worker-zgx89   1/1     Running     0             61s
# raycronjob-sample-l76h8-w9flp                            0/1     Completed   0             30s
# raycronjob-sample-pct47-bdspj-head-cdwwc                 0/1     Pending     0             1s
# raycronjob-sample-pct47-bdspj-small-group-worker-9r7cm   0/1     Init:0/1    0             1s

# (Optional) Quickly filter completed pods
# kubectl get pods | grep Completed

# Step 5.2: Identify the job pod
# Look for the pod with STATUS=Completed that corresponds to your RayJob
# (This is the Ray job submitter pod)
# In this example:
# job-pod-name = raycronjob-sample-l76h8-w9flp

# Step 5.3: Fetch the logs of the job pod
kubectl logs <job-pod-name>
# Example:
# kubectl logs raycronjob-sample-l76h8-w9flp

# [Example output]
# 2026-04-02 22:57:35,742 INFO cli.py:41 -- Job submission server address: http://raycronjob-sample-l76h8-hjtrs-head-svc.default.svc.cluster.local:8265
# 2026-04-02 22:57:36,709 SUCC cli.py:65 -- ----------------------------------------------------------
# 2026-04-02 22:57:36,710 SUCC cli.py:66 -- Job 'raycronjob-sample-l76h8-hmjz2' submitted successfully
# 2026-04-02 22:57:36,710 SUCC cli.py:67 -- ----------------------------------------------------------
# 2026-04-02 22:57:36,710 INFO cli.py:291 -- Next steps
# 2026-04-02 22:57:36,710 INFO cli.py:292 -- Query the logs of the job:
# 2026-04-02 22:57:36,710 INFO cli.py:294 -- ray job logs raycronjob-sample-l76h8-hmjz2
# 2026-04-02 22:57:36,710 INFO cli.py:296 -- Query the status of the job:
# 2026-04-02 22:57:36,710 INFO cli.py:298 -- ray job status raycronjob-sample-l76h8-hmjz2
# 2026-04-02 22:57:36,710 INFO cli.py:300 -- Request the job to be stopped:
# 2026-04-02 22:57:36,710 INFO cli.py:302 -- ray job stop raycronjob-sample-l76h8-hmjz2
# 2026-04-02 22:57:38,771 INFO cli.py:41 -- Job submission server address: http://raycronjob-sample-l76h8-hjtrs-head-svc.default.svc.cluster.local:8265
# 2026-04-02 22:57:36,400 INFO job_manager.py:568 -- Runtime env is setting up.
# Running entrypoint for job raycronjob-sample-l76h8-hmjz2: python /home/ray/samples/sample_code.py
# 2026-04-02 22:57:46,654 INFO worker.py:1696 -- Using address 10.244.0.39:6379 set in the environment variable RAY_ADDRESS
# 2026-04-02 22:57:46,659 INFO worker.py:1837 -- Connecting to existing Ray cluster at address: 10.244.0.39:6379...
# 2026-04-02 22:57:46,681 INFO worker.py:2014 -- Connected to Ray cluster. View the dashboard at 10.244.0.39:8265 
# /home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/worker.py:2062: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
#   warnings.warn(
# test_counter got 1
# test_counter got 2
# test_counter got 3
# test_counter got 4
# test_counter got 5
# 2026-04-02 22:57:58,968 SUCC cli.py:65 -- ---------------------------------------------
# 2026-04-02 22:57:58,968 SUCC cli.py:66 -- Job 'raycronjob-sample-l76h8-hmjz2' succeeded
# 2026-04-02 22:57:58,968 SUCC cli.py:67 -- ---------------------------------------------
```

### Step 6: Clean Up

To stop the recurring jobs and delete the resource, run:

```bash
# Step 6.1: Delete the RayCronJob
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.6.0/ray-operator/config/samples/ray-cronjob.sample.yaml

# Step 6.2: Delete the KubeRay operator
helm uninstall kuberay-operator

# Step 6.3: Delete the Kubernetes cluster
kind delete cluster
```