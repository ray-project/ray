---
myst:
  html_meta:
    description: "Run Ray jobs on a recurring schedule with the RayCronJob custom resource, including configuration and a worked example."
---

(kuberay-raycronjob-quickstart)=

# RayCronJob quickstart

A RayCronJob creates RayJobs on a recurring schedule, the same way a Kubernetes CronJob creates Jobs.

:::{note}
RayCronJob is alpha and disabled by default. To use it, enable the `RayCronJob` feature gate.
:::

## Prerequisites

* KubeRay v1.8.0 or newer

## Configure a RayCronJob

The RayCronJob spec has four fields:

* `schedule`: The cron schedule for creating RayJobs, such as `*/5 * * * *` for every five minutes.
* `jobTemplate`: The template for the RayJobs that the RayCronJob creates.
  * `metadata`: Optional. Labels and annotations to copy onto each RayJob, such as a Kueue `kueue.x-k8s.io/queue-name` label.
  * `spec`: The RayJob spec. See the {ref}`RayJob quickstart <kuberay-rayjob-quickstart>` for its fields.
* `timeZone`: Optional. The [time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) for `schedule`, such as `America/Los_Angeles`. Defaults to the KubeRay operator's local time zone. Don't put `TZ` or `CRON_TZ` in `schedule`.
* `suspend`: Optional. Set it to `true` to stop creating RayJobs. Existing RayJobs keep running.

The following RayCronJob creates a RayJob every five minutes:

```yaml
apiVersion: ray.io/v1
kind: RayCronJob
metadata:
  name: example-raycronjob
spec:
  schedule: "*/5 * * * *"
  timeZone: "America/Los_Angeles"
  jobTemplate:
    metadata:
      labels:
        kueue.x-k8s.io/queue-name: user-queue
    spec:
      entrypoint: python /home/ray/samples/sample_code.py
      # Add the other RayJob fields, such as rayClusterSpec.
```

:::{note}
KubeRay v1.6 and v1.7 take the RayJob spec fields directly under `jobTemplate`. Starting in v1.8.0, the API server rejects that format. Move the fields under `jobTemplate.spec`.
:::

## Run a RayCronJob

This example runs a short Python script every minute.

### Step 1: Create a Kubernetes cluster with kind

```bash
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator

Install the KubeRay operator with the `RayCronJob` feature gate enabled:

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update

# Install KubeRay operator v1.8.0 with the RayCronJob feature gate enabled.
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.8.0 \
  --set "featureGates[0].name=RayCronJob" \
  --set "featureGates[0].enabled=true"
```

### Step 3: Create a RayCronJob

```bash
kubectl apply -f https://raw.githubusercontent.com/kuberay-project/kuberay/v1.8.0/ray-operator/config/samples/ray-cronjob.sample.yaml
```

### Step 4: Monitor the RayCronJob

Check the RayCronJob. `LAST SCHEDULE` stays empty until the first run.

```bash
kubectl get raycronjob raycronjob-sample
# [Example output]
# NAME                SCHEDULE    TIMEZONE   LAST SCHEDULE   AGE   SUSPEND
# raycronjob-sample   * * * * *                              8s
```

The sample's schedule is `* * * * *`, so the RayCronJob creates a RayJob at the start of every minute. Watch the RayJobs, and press Ctrl+C to stop:

```bash
kubectl get rayjob -w
# [Example output]
# NAME                         JOB STATUS   DEPLOYMENT STATUS   RAY CLUSTER NAME                   START TIME             END TIME               AGE
# raycronjob-sample-29904996                Initializing        raycronjob-sample-29904996-qjh7t   2026-11-10T08:36:00Z                          2s
# raycronjob-sample-29904996   RUNNING      Running             raycronjob-sample-29904996-qjh7t   2026-11-10T08:36:00Z                          40s
# raycronjob-sample-29904996   SUCCEEDED    Complete            raycronjob-sample-29904996-qjh7t   2026-11-10T08:36:00Z   2026-11-10T08:36:50Z   50s
# raycronjob-sample-29904997                Initializing        raycronjob-sample-29904997-pc4ls   2026-11-10T08:37:00Z                          0s
```

### Step 5: Check the Ray job output

Each RayJob submits its Ray job through a Kubernetes Job with the same name. Get the Job's logs, replacing `<rayjob-name>` with a name from the previous step, such as `raycronjob-sample-29904996`:

```bash
kubectl logs -l=job-name=<rayjob-name>
# [Example output]
# ...
# test_counter got 1
# test_counter got 2
# test_counter got 3
# test_counter got 4
# test_counter got 5
# 2026-11-10 00:36:46,969 SUCC cli.py:65 -- ------------------------------------------------
# 2026-11-10 00:36:46,969 SUCC cli.py:66 -- Job 'raycronjob-sample-29904996-lnzzd' succeeded
# 2026-11-10 00:36:46,970 SUCC cli.py:67 -- ------------------------------------------------
```

### Step 6: Clean up

Delete the RayCronJob, the KubeRay operator, and the kind cluster:

```bash
kubectl delete -f https://raw.githubusercontent.com/kuberay-project/kuberay/v1.8.0/ray-operator/config/samples/ray-cronjob.sample.yaml
helm uninstall kuberay-operator
kind delete cluster
```
