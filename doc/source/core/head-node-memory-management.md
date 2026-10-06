---
myst:
  html_meta:
    description: "Why Ray head node memory grows and how to mitigate it by keeping work off the head node, disabling the dashboard, and sizing the head pod."
---

(head-node-memory-management)=

# Head node memory management

When you run a Ray cluster for an extended period, the head node's memory usage can increase steadily. This growth can lead to out-of-memory (OOM) errors that can make the entire cluster unusable. This page explains what causes head node memory growth and how to mitigate it.

```{contents}
:local:
```

## Why head node memory grows

The Ray dashboard provides a web interface for cluster monitoring and debugging. See {ref}`observability-getting-started`. The dashboard adds to head node memory usage in the following ways:

- The dashboard caches cluster events in memory for display and debugging. The `RAY_DASHBOARD_MAX_EVENTS_TO_CACHE` environment variable controls the cache size. For implementation details, see the [event caching code](https://github.com/ray-project/ray/blob/814768317813afca2f0af740f58d024b059ae7d7/python/ray/dashboard/modules/event/event_head.py#L35).
- The dashboard processes and stores logs and metadata from jobs and workers. This data accumulates in long-running clusters.

## Mitigation strategies

### Avoid scheduling on the head node

Avoid running tasks or actors on the head node, because it hosts critical system components. Keeping work off the head node helps reduce contention and memory pressure.

For head node best practices, see {ref}`vms-large-cluster-configure-head-node`.

### Disable the dashboard

If you don't need the dashboard, disable it to remove event caching and the related memory overhead. Disabling the dashboard reduces observability into the system, so Ray doesn't recommend disabling it on production clusters.

To disable the dashboard with the Python API, run the following:

```python
import ray
ray.init(include_dashboard=False)
```

To disable the dashboard with the CLI, run the following:

```bash
ray start --head --include-dashboard=False
```

On Kubernetes, set `spec.headGroupSpec.rayStartParams.include-dashboard` to `"false"` in your RayCluster configuration.

:::{warning}
Disabling the dashboard prevents KubeRay's `RayJob` and `RayService` features from working properly.
:::

## Kubernetes configuration

### Head pod memory settings

When you deploy on Kubernetes, configure memory requests and limits for the head pod.

:::{important}
Set memory and GPU resource requests equal to their limits. KubeRay uses the container's resource limits to configure Ray's logical resource capacities and ignores memory and GPU requests.
:::

The following example sets the head pod's requests equal to its limits:

```yaml
headGroupSpec:
  template:
    spec:
      containers:
      - name: ray-head
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
          limits:
            memory: "8Gi"
            cpu: "4"
```

### Recommended head node specifications

For large clusters, start with the following head node specification:

- **CPU:** 16 cores
- **Memory:** 64 GB

Actual requirements depend on your workload and cluster size.

To prevent Ray from scheduling tasks on the head node, set `num-cpus: "0"` in `rayStartParams`.

## Best practices

Follow these practices to keep head node memory under control:

1. Avoid scheduling on the head node to reduce contention and memory pressure.
1. Scale vertically and use a larger head node before you adjust internal settings.
1. Set Kubernetes resource limits, and set memory and GPU requests to match them.

:::{note}
Disabling the dashboard severely limits observability, so Ray doesn't recommend it for production. If you choose to disable it, see [Disable the dashboard](#disable-the-dashboard).
:::

## Troubleshooting

If your head node runs into OOM errors, do the following:

1. Check memory usage with `ray memory`. See {ref}`debug-with-ray-memory`.
1. Consider increasing the head node's memory allocation.

For more information on OOM prevention, see {ref}`ray-oom-prevention`.
