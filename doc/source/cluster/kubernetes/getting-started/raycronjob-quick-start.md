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
* `jobTemplate` - Defines the exact **RayJob** blueprint that the controller will use for each scheduled run. Because it inherits the entire `RayJob` schema, it supports all standard RayJob configurations directly, including:
  * `rayClusterSpec` - Defines the RayCluster custom resource to run the Ray job on.
  * `entrypoint` - The command to execute for the job.
  * `shutdownAfterJobFinishes` - Determines whether to recycle the RayCluster after the scheduled Ray job finishes.
  * *Note: See the standard [RayJob Configuration](#rayjob-quick-start) documentation for the complete list of supported fields within the `jobTemplate`.*
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

## How to Run an Easy RayCronJob

Let's deploy a simple `RayCronJob` that executes a short Python script every minute.

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install the KubeRay operator

Follow the [KubeRay Operator Installation](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

### Step 3: Install a RayCronJob

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.6.0/ray-operator/config/samples/ray-cronjob.sample.yaml
```

### Step 4: Monitor the RayCronJob

Check the status of your `RayCronJob`. You should see the schedule and the last time it successfully scheduled a job.

```bash
kubectl get raycronjob raycronjob-sample
```

You should see output listing the RayCronJob created, for example:

```
NAME                SCHEDULE    LAST SCHEDULE   AGE   SUSPEND
raycronjob-sample   * * * * *                   10s   
```

Because our schedule is `* * * * *`, a new `RayJob` will be generated at the start of the next minute. You can watch the `RayJob` instances being created:

```bash
kubectl get rayjob -w
```

### Step 5: Verify the Output

Once a generated `RayJob` completes, you can check the logs of the Ray job submitter or the head node to verify our Python script ran successfully:

```bash
# Find the specific pod running the job (usually named after the RayJob with a suffix)
kubectl get pods -l ray.io/is-job-worker=true

# Fetch the logs of the job pod
kubectl logs <job-pod-name>
```

### Step 6: Clean Up

To stop the recurring jobs and delete the resource, run:

```bash
kubectl delete -f ray-cronjob.sample.yaml
```