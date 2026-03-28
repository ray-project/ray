(kuberay-raycronjob-quickstart)=

# RayCronJob Quickstart

## What is a RayCronJob?

A `RayCronJob` is a KubeRay Custom Resource (CR) that allows you to run `RayJob` workloads on a recurring, time-based schedule. It is heavily inspired by the native Kubernetes `CronJob` and brings automated, scheduled execution to your distributed Ray applications. 


This is particularly useful for recurring tasks such as scheduled model retraining, nightly batch inferences, or regular data processing pipelines.

## RayCronJob Features

The `RayCronJob` CRD provides standard cron-like functionality tailored for Ray workloads:

* **Automated Scheduling:** Define your run schedule using standard cron format strings (e.g., `* * * * *` for every minute).
* **Concurrency Policy (`concurrencyPolicy`):** Control what happens if a new job is scheduled to run before the previous one has finished:
    * `Allow`: (Default) Allows concurrent jobs to run.
    * `Forbid`: Skips the new job run if the previous one is still active.
    * `Replace`: Cancels the currently running job and replaces it with the new one.
* **Job History Limits:** Automatically clean up the cluster by specifying how many completed or failed `RayJob`s to keep using `successfulJobsHistoryLimit` and `failedJobsHistoryLimit`.
* **Suspend (`suspend`):** Temporarily pause the scheduling of future jobs without deleting the `RayCronJob` resource.

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
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 1
  jobTemplate:
    # Everything below here is a standard RayJob spec
    spec:
      entrypoint: python /home/ray/samples/sample_code.py
      # ... (RayCluster spec, runtimeEnv, etc.)
```

## How to Run an Easy RayCronJob

Let's deploy a simple `RayCronJob` that executes a short Python script every minute.

### Prerequisites

* A running Kubernetes cluster.
* The KubeRay operator installed and running in your cluster.

### Step 1: Create the RayCronJob YAML

Create a file named `raycronjob-sample.yaml` with the following content. This job will simply print the current date and time in the Ray cluster every minute.

```yaml
apiVersion: ray.io/v1
kind: RayCronJob
metadata:
  name: raycronjob-sample
  namespace: default
spec:
  schedule: "* * * * *"
  jobTemplate:
    # Notice: No "spec:" wrapper here! 
    # These are standard RayJob fields placed directly under jobTemplate
    entrypoint: python -c "import datetime; print(f'Hello from RayCronJob! Current time is {datetime.datetime.now()}')"
    shutdownAfterJobFinishes: true
    ttlSecondsAfterFinished: 60
    rayClusterSpec:
      rayVersion: '2.52.0'
      headGroupSpec:
        rayStartParams:
          dashboard-host: '0.0.0.0'
        template:
          spec:
            containers:
              - name: ray-head
                image: rayproject/ray:2.52.0
                resources:
                  limits:
                    cpu: "1"
                    memory: "2Gi"
                  requests:
                    cpu: "200m"
      workerGroupSpecs:
        - replicas: 1
          minReplicas: 1
          maxReplicas: 1
          groupName: small-group
          rayStartParams: {}
          template:
            spec:
              containers:
                - name: ray-worker
                  image: rayproject/ray:2.52.0
                  resources:
                    limits:
                      cpu: "1"
                      memory: "2Gi"
                    requests:
                      cpu: "200m"
```

### Step 2: Apply the Resource

Apply the configuration to your cluster:

```bash
kubectl apply -f raycronjob-sample.yaml
```

### Step 3: Monitor the RayCronJob

Check the status of your `RayCronJob`. You should see the schedule and the last time it successfully scheduled a job.

```bash
kubectl get raycronjob raycronjob-sample
```

Because our schedule is `* * * * *`, a new `RayJob` will be generated at the start of the next minute. You can watch the `RayJob` instances being created:

```bash
kubectl get rayjob -w
```

### Step 4: Verify the Output

Once a generated `RayJob` completes, you can check the logs of the Ray job submitter or the head node to verify our Python script ran successfully:

```bash
# Find the specific pod running the job (usually named after the RayJob with a suffix)
kubectl get pods -l ray.io/is-job-worker=true

# Fetch the logs of the job pod
kubectl logs <job-pod-name>
```

### Step 5: Clean Up

To stop the recurring jobs and delete the resource, run:

```bash
kubectl delete -f raycronjob-sample.yaml
```