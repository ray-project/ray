---
myst:
  html_meta:
    description: "Run Ray jobs on a recurring schedule with the RayCronJob custom resource, including configuration and a worked example."
---

(kuberay-raycronjob-quickstart)=

# RayCronJob Quickstart

## Prerequisites

* This feature requires KubeRay version 1.6.0 or newer, and it's in alpha testing. KubeRay 1.8.0 uses a new `jobTemplate` format. See the note in [How to Configure a RayCronJob](#how-to-configure-a-raycronjob).
* A running Kubernetes cluster.
* The KubeRay operator installed and running in your cluster.

## What is a RayCronJob?

A `RayCronJob` is a Custom Resource (CR) that allows you to run `RayJob` workloads on a recurring, time-based schedule. It is heavily inspired by the native Kubernetes `CronJob` and brings automated, scheduled execution to your distributed Ray applications.


This is particularly useful for recurring tasks such as scheduled model retraining, nightly batch inferences, or regular data processing pipelines.


## RayCronJob Configuration

The `RayCronJob` CRD acts as an automated scheduler specifically designed to create and manage **RayJob** custom resources on a recurring basis. It does not execute workloads directly. Instead, it acts as a controller that creates a new `RayJob` each time the schedule triggers.

* `schedule` - The cron schedule string defining when a new Ray job should be created and run (e.g., `* * * * *` for every minute).
* `jobTemplate` - The template for the RayJob that the controller creates on each scheduled run.
  * `metadata` (Optional): Labels and annotations that the controller copies onto each RayJob, such as a Kueue `kueue.x-k8s.io/queue-name` label.
  * `spec` - A standard RayJob spec. See [RayJob Configuration](kuberay-rayjob-quickstart) for the supported fields.
* `suspend` (Optional): If `suspend` is true, the controller suspends the scheduling of future jobs. This does not apply to or interrupt any `RayJob`s that have already been created and are currently running.
* `timeZone` (Optional): The time zone for the `schedule`, in [IANA Time Zone Database](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) format (e.g., `America/Los_Angeles`). If omitted, the schedule uses the local time zone of the KubeRay operator. Do not set `TZ` or `CRON_TZ` in the `schedule` string, use this field instead.

## How to Configure a RayCronJob

Configuring a `RayCronJob` requires wrapping a standard `RayJob` specification inside `jobTemplate.spec`, alongside your scheduling parameters.

The high-level structure looks like this:

```yaml
apiVersion: ray.io/v1
kind: RayCronJob
metadata:
  name: example-raycronjob
spec:
  schedule: "*/5 * * * *" # Run every 5 minutes
  timeZone: "America/Los_Angeles" # Optional, defaults to the operator's local time zone
  jobTemplate:
    metadata: # Optional, copied onto each RayJob
      labels:
        kueue.x-k8s.io/queue-name: user-queue
    spec:
      # Everything below here is a standard RayJob spec
      entrypoint: python /home/ray/samples/sample_code.py
      # ... (RayCluster spec, runtimeEnv, etc.)
```

:::{note}
KubeRay v1.6 and v1.7 take the RayJob spec fields directly under `jobTemplate`. Starting in v1.8.0, the API server rejects that format. Move those fields under `jobTemplate.spec`.
:::

## How to Run a Simple RayCronJob

Let's deploy a simple `RayCronJob` that executes a short Python script every minute.

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator
Install the KubeRay operator, following [these instructions](operator-installation.md). The minimum version for this guide is v1.8.0. To use this feature, the `RayCronJob` feature gate must be enabled. To enable the feature gate when installing the kuberay operator, run the following command:

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install KubeRay operator v1.8.0 with the RayCronJob feature gate enabled
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.8.0 \
  --set "featureGates[0].name=RayCronJob" \
  --set "featureGates[0].enabled=true"
```

### Step 3: Install a RayCronJob

```sh
kubectl apply -f https://raw.githubusercontent.com/kuberay-project/kuberay/v1.8.0/ray-operator/config/samples/ray-cronjob.sample.yaml
```

### Step 4: Monitor the RayCronJob

Check the status of your `RayCronJob`. The `SCHEDULE` field should be visible, while `LAST SCHEDULE` may be empty until the first run.

```sh
kubectl get raycronjob raycronjob-sample

#You should see output listing the RayCronJob created
# [Example output]

# NAME                SCHEDULE    TIMEZONE   LAST SCHEDULE   AGE   SUSPEND
# raycronjob-sample   * * * * *                              10s
```

Because our schedule is `* * * * *`, a new `RayJob` will be generated at the start of the next minute. You can watch the `RayJob` instances being created:

```shell
kubectl get rayjob -w
# [Example output]
# NAME                         JOB STATUS   DEPLOYMENT STATUS   RAY CLUSTER NAME                   START TIME             END TIME               AGE
# raycronjob-sample-29905023                Initializing        raycronjob-sample-29905023-r6sf2   2026-11-10T09:03:00Z                          1s
# raycronjob-sample-29905023   RUNNING      Running             raycronjob-sample-29905023-r6sf2   2026-11-10T09:03:00Z                          42s
# raycronjob-sample-29905023   SUCCEEDED    Complete            raycronjob-sample-29905023-r6sf2   2026-11-10T09:03:00Z   2026-11-10T09:03:58Z   58s
# raycronjob-sample-29905024                Initializing        raycronjob-sample-29905024-4lvrd   2026-11-10T09:04:00Z                          0s
# (Press Ctrl+C to stop watching once the job completes)
```

### Step 5: Check the output of the RayCronJob

```shell
# From the previous step, note the RayJob name label (e.g., raycronjob-sample-29905023)
# Use it to fetch the submitter pod logs directly:
kubectl logs -l=job-name=<rayjob-name>
# Example:
# kubectl logs -l=job-name=raycronjob-sample-29905023

# [Example output]
# /home/ray/anaconda3/lib/python3.10/site-packages/ray/_private/worker.py:2062: FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var if num_gpus=0 or num_gpus=None (default). To enable this behavior and turn off this error message, set RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO=0
#   warnings.warn(
# test_counter got 1
# test_counter got 2
# test_counter got 3
# test_counter got 4
# test_counter got 5
# 2026-11-10 01:03:54,406 SUCC cli.py:65 -- ------------------------------------------------
# 2026-11-10 01:03:54,407 SUCC cli.py:66 -- Job 'raycronjob-sample-29905023-r62fr' succeeded
# 2026-11-10 01:03:54,407 SUCC cli.py:67 -- ------------------------------------------------
```

### Step 6: Clean Up

To stop the recurring jobs and delete the resource, run:

```bash
# Step 6.1: Delete the RayCronJob
kubectl delete -f https://raw.githubusercontent.com/kuberay-project/kuberay/v1.8.0/ray-operator/config/samples/ray-cronjob.sample.yaml

# Step 6.2: Delete the KubeRay operator
helm uninstall kuberay-operator

# Step 6.3: Delete the Kubernetes cluster
kind delete cluster
```
