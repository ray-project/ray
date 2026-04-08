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

The `RayCronJob` CRD acts as an automated scheduler specifically designed to create and manage **RayJob** custom resources on a recurring basis. It does not execute workloads directly. Instead, it acts as a controller that creates a new `RayJob` each time the schedule triggers.

* `schedule` - The cron schedule string defining when a new Ray job should be created and run (e.g., `* * * * *` for every minute).
* `jobTemplate` - Wraps a standard **RayJob** spec that the controller will use for each scheduled run. It supports the same fields as a RayJob spec. See the standard [RayJob Configuration](kuberay-rayjob-quickstart) documentation for the complete list of supported fields within the `jobTemplate`.
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

## How to Run a Simple RayCronJob

Let's deploy a simple `RayCronJob` that executes a short Python script every minute.

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator
Install the KubeRay operator, following [these instructions](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html). The minimum version for this guide is v1.6.0. To use this feature, the `RayCronJob` feature gate must be enabled. To enable the feature gate when installing the kuberay operator, run the following command:

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

Check the status of your `RayCronJob`. The `SCHEDULE` field should be visible, while `LAST SCHEDULE` may be empty until the first run.

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

### Step 5: Check the output of the RayCronJob

```shell
# From the previous step, note the RayJob name label (e.g., raycronjob-sample-l76h8)
# Use it to fetch the submitter pod logs directly:
kubectl logs -l=job-name=<rayjob-name>
# Example:
# kubectl logs -l=job-name=raycronjob-sample-l76h8

# [Example output]
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