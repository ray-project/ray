(kuberay-in-place-pod-resizing)=

# KubeRay In-Place Pod Resizing (IPPR)

This guide explains how to configure In-Place Pod Resizing (IPPR) for the Ray Autoscaler on Kubernetes 1.35+ with Ray 2.56 or later.
IPPR allows the Ray Autoscaler to vertically resize running Pods (CPU and memory) to change the cluster capacity.

```{admonition} Alpha feature
:class: warning

Ray Autoscaler integration with Kubernetes [In-Place Pod Resize](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/) is still an alpha feature.
APIs and behavior may change in future releases.
```

## Overview

For background on the underlying Kubernetes feature, see [Resize CPU and Memory Resources assigned to Containers](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/) in the Kubernetes documentation.

Without IPPR, the Ray Autoscaler scales horizontally only: when pending tasks, actors, or placement groups can't fit on existing Ray worker nodes, the Autoscaler launches new worker Pods. If the underlying Kubernetes cluster doesn't have capacity for those Pods, the [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler) (which you configure separately) can in turn provision new Kubernetes nodes. See {ref}`the 3 levels of autoscaling in KubeRay for more details <kuberay-autoscaling>`.

With IPPR, the Autoscaler can first try to satisfy pending demand by *resizing* existing worker Pods without restarting them up to a per-group maximum, and only falls back to launching new Pods when in-place resizing isn't sufficient.

```{admonition} When to use IPPR?
IPPR can reduce Pod-launch overhead and improve packing of long-lived workloads on a smaller number of larger Pods.
It is most useful when:

* You have workloads with bursty resource demand that benefit from vertical scaling.
* Worker Pod startup latency dominates your scale-up time.
* The underlying Kubernetes nodes have headroom for larger worker Pods (or your Kubernetes cluster autoscaler can provide it).
```

The Autoscaler's high-level behavior with IPPR enabled is:

1. After bin-packing pending tasks onto existing Ray worker nodes at their current capacity, the Autoscaler tries to bin-pack remaining demand onto Ray worker nodes that have no ongoing resize, this time using their *maximum* capacity. If a Ray worker node can absorb more demand by resizing, the Autoscaler issues a Kubernetes resize request for that node's Pod and records the status in the `ray.io/ippr-status` annotation on the Pod.
2. If demand still remains, the Autoscaler falls back to horizontal scale-out, taking each worker group's maximum capacity into account.
3. On the next reconciliation, the Autoscaler checks each in-flight resize:

   * If the resize succeeded, the Autoscaler updates the Raylet's logical resources to match the new Pod size and updates `ray.io/ippr-status` accordingly.
   * If the resize timed out or errored, the Autoscaler queues a new Kubernetes resize request to adjust the Pod (for example, rolling it back to its previous size after a timeout, as shown in [Case 4](#case-4-resize-timeout-and-rollback)).

## Prerequisites

* **Kubernetes 1.35 or later.** In-Place Pod Resize graduated to GA in Kubernetes 1.35 ([blog post](https://kubernetes.io/blog/2025/12/19/kubernetes-v1-35-in-place-pod-resize-ga/)).
* **KubeRay v1.5.0 or later.**
* **Ray Autoscaler V2** enabled on the RayCluster. See {ref}`kuberay-autoscaler-v2`.

## Configuration

Enable IPPR by setting the `ray.io/ippr` annotation on the RayCluster custom resource. The annotation value is a JSON document keyed by worker `groupName`:

```text
{
  "groups": {
    "<groupName>": {
      "max-cpu":        string|number,
      "max-memory":     string|integer,
      "resize-timeout": integer
    }
  }
}
```

`<groupName>` must match a `groupName` under `workerGroupSpecs` on the RayCluster. Each entry has the following required fields:

* **`max-cpu`**: The maximum CPU the Autoscaler may resize a Pod in this group to. Accepts any [Kubernetes CPU quantity](https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/), for example `"2"` or `"1500m"`.
* **`max-memory`**: The maximum memory the Autoscaler may resize a Pod in this group to. Accepts any Kubernetes memory quantity, for example `"8Gi"` or `2147483648` (raw bytes).
* **`resize-timeout`**: Number of seconds to wait for a Kubernetes Pod resize to complete before considering it timed out and rolling it back.

### Validation rules

In addition to the schema above, the Ray Autoscaler validates the following requirements when IPPR is enabled for a worker group. If validation fails, the Autoscaler raises an error during reconciliation (visible in the autoscaler logs in the head Pod) and will refuse to launch worker Pods for the misconfigured group until the RayCluster is updated accordingly.

1. **No CPU/memory in `rayStartParams`.** The corresponding worker group must not set `num-cpus` or `memory` in `rayStartParams`. Hard-coding logical resources there would cause Ray's view of the node's capacity to drift from the Pod's physical resources after a resize.
2. **CPU and memory requests are required.** The Ray container in the worker group must specify both `cpu` and `memory` under `resources.requests`.
3. **`resizePolicy` must use `restartPolicy: NotRequired`.** That's the Kubernetes default, so leaving `resizePolicy` unset is fine. Setting it to `RestartContainer`, which causes the container to be restarted, is not yet supported by Ray.

## Example

The following RayCluster excerpt enables IPPR for a worker group `small-group` whose Pods start at 1 CPU / 1Gi and may be resized up to 4 CPU / 4Gi.

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-ippr
  annotations:
    ray.io/ippr: |
      {
        "groups": {
          "small-group": {
            "max-cpu": "4",
            "max-memory": "4Gi",
            "resize-timeout": 60
          }
        }
      }
spec:
  enableInTreeAutoscaling: true
  autoscalerOptions:
    # IPPR requires Autoscaler V2.
    version: v2
    env:
    - name: AUTOSCALER_UPDATE_INTERVAL_S
      value: "1"
  headGroupSpec:
    rayStartParams:
      num-cpus: "0"
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.56.0
          resources:
            requests:
              cpu: "1"
              memory: "4Gi"
            limits:
              cpu: "1"
              memory: "4Gi"
  workerGroupSpecs:
  - groupName: small-group
    replicas: 1
    minReplicas: 1
    maxReplicas: 10
    # Note: do NOT set num-cpus or memory in rayStartParams when using IPPR.
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.56.0
          # CPU and memory requests are required for IPPR.
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"
            limits:
              cpu: "1"
              memory: "1Gi"
          # `resizePolicy` is omitted; IPPR requires the default
          # `restartPolicy: NotRequired` for cpu and memory.
```

## Resize behavior

In the examples below, each entry of `WorkerGroup1=[...]` represents one Ray worker node (a worker Pod). `CPU` is the node's total CPU capacity (which IPPR can change in place) and `Available` is the unallocated portion. The cluster has one head node (`CPU=0`) and one worker group `WorkerGroup1` whose Pods start at `CPU=1` and may be resized up to `CPU=4`.

### Case 1: Resize an existing Ray worker node

A single idle worker can absorb a pending 2-CPU task by resizing in place.

```text
T1: WorkerGroup1=[{CPU: 1, Available: 1}]   Pending=[{CPU: 2}]
    -> Autoscaler resizes the worker to 4 CPUs:
       WorkerGroup1=[{CPU: 1 -> 4, Available: 1 -> 4}]

T2: WorkerGroup1=[{CPU: 4, Available: 2}]   Pending=[]
```

### Case 2: Resize an existing worker and scale out a new worker

A combination of resize and scale-out is used when demand exceeds what one resized Pod can absorb.

```text
T1: WorkerGroup1=[{CPU: 1, Available: 1}]   Pending=[{CPU: 2}, {CPU: 4}]
    -> Resize the existing worker to 4 CPUs and launch a new worker:
       WorkerGroup1=[{CPU: 1 -> 4, Available: 1 -> 4}, {CPU: 1, Available: 1}]

T2: WorkerGroup1=[{CPU: 4, Available: 0}, {CPU: 1, Available: 1}]   Pending=[{CPU: 2}]
    -> Resize the new worker to 4 CPUs:
       WorkerGroup1=[{CPU: 4, Available: 0}, {CPU: 1 -> 4, Available: 1 -> 4}]

T3: WorkerGroup1=[{CPU: 4, Available: 0}, {CPU: 4, Available: 2}]   Pending=[]
```

### Case 3: Scale out with IPPR capacity in mind

When no worker exists yet, the Autoscaler still launches a new Pod. On a subsequent reconciliation, that Pod can be resized in place to absorb pending demand (as in Case 1).

```text
T1: WorkerGroup1=[]   Pending=[{CPU: 2}]
    -> Launch a new worker (IPPR capacity is considered for sizing decisions):
       WorkerGroup1=[{CPU: 1, Available: 1}]

T2: WorkerGroup1=[{CPU: 1, Available: 1}]   Pending=[{CPU: 2}]
    -> Same as Case 1.
```

### Case 4: Resize timeout and rollback

If a Kubernetes resize request doesn't complete within `resize-timeout` seconds, the Autoscaler rolls it back and falls back to horizontal scale-out.

```text
T1: WorkerGroup1=[{CPU: 1, Available: 1}]   Pending=[{CPU: 2}]
    -> Resize attempt:
       WorkerGroup1=[{CPU: 1 -> 4, Available: 1 -> 4}]

T2: (resize times out)
    -> Roll back the resize and scale out:
       WorkerGroup1=[{CPU: 4 -> 1, Available: 4 -> 1}, {CPU: 1, Available: 1}]

T3: WorkerGroup1=[{CPU: 1, Available: 1}, {CPU: 1, Available: 1}]   Pending=[{CPU: 2}]
    -> Try resizing the second worker:
       WorkerGroup1=[{CPU: 1, Available: 1}, {CPU: 1 -> 4, Available: 1 -> 4}]

T4: WorkerGroup1=[{CPU: 1, Available: 1}, {CPU: 4, Available: 2}]   Pending=[]
```

## Observability

In-flight and recent resize state for each worker Pod is recorded in the `ray.io/ippr-status` annotation on the Pod. You can inspect it with `kubectl`:

```bash
kubectl get pod <worker-pod> -o jsonpath='{.metadata.annotations.ray\.io/ippr-status}'
```

For example, after a successful resize, `resizing-at: null` means no resize is in flight and `last-failed-at: null` means the most recent attempt succeeded:

```json
{
  "resizing-at": null,
  "last-failed-at": null,
  "last-failed-reason": null
}
```

You can watch the Kubernetes-side progress of an in-flight resize via the Pod's [resize status conditions](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#pod-resize-status) (`PodResizePending` and `PodResizeInProgress`) and the resource events the Kubelet emits during the resize. The simplest way to view both at once is `kubectl describe pod <worker-pod>`.

After a successful resize, the `Conditions:` table contains only the standard Pod conditions (the transient `PodResizePending` / `PodResizeInProgress` fields are gone), the Pod's `Limits` / `Requests` reflect the new size, and the Kubelet emits a `ResizeCompleted` event:

```text
Conditions:
  Type                        Status
  PodReadyToStartContainers   True 
  Initialized                 True 
  Ready                       True 
  ContainersReady             True 
  PodScheduled                True 
Containers:
  ray-worker:
    ...
    Limits:
      cpu:     4
      memory:  4Gi
    Requests:
      cpu:     4
      memory:  4Gi
    ...
Events:
  Type    Reason           Age   From     Message
  ----    ------           ----  ----     -------
  Normal  ResizeCompleted  2s    kubelet  Pod resize completed: {"containers":[{"name":"ray-worker","resources":{"limits":{"cpu":"4","memory":"4Gi"},"requests":{"cpu":"4","memory":"4Gi"}}}]}
```

If a resize can't be granted, the Pod gains a `PodResizePending` condition:

```text
Conditions:
  Type                        Status
  PodResizePending            True 
  PodReadyToStartContainers   True 
  Initialized                 True 
  Ready                       True 
  ContainersReady             True 
  PodScheduled                True 
```

See [Pod resize status](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#pod-resize-status) and [Troubleshooting: Infeasible resize request](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#troubleshooting-infeasible-resize-request) in the Kubernetes documentation for the meaning of the condition's `reason` and `message` and how to inspect them.

## Limitations

* **Resize-up only.** The current implementation can only scale workers up to the configured maximum. Gradual resizing and downsizing will be added in future releases.
* **CPU and memory only.** Other resources, including GPUs, can't be resized in place.
* **Per-worker-group configuration.** IPPR is configured per worker `groupName`. Worker groups without an entry in `ray.io/ippr` aren't resized in place.
* **Autoscaler V2 only.** IPPR is integrated only with Autoscaler V2.
* **Upstream Kubernetes limitations apply.** Any [limitations of the underlying Kubernetes In-Place Pod Resize feature](https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/#limitations) (such as restrictions on QoS-class changes, swap, or specific runtimes) also apply to IPPR-managed Pods.
