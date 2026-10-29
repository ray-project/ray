---
myst:
  html_meta:
    description: "Automatically suspend or delete an idle RayCluster with no connected drivers using KubeRay idle termination and the Ray autoscaler v2."
---

(kuberay-idle-termination)=

# KubeRay idle termination

Idle termination reclaims a RayCluster that has no drivers connected. The Ray autoscaler can scale worker Pods down to zero based on `idleTimeoutSeconds`, but the head Pod keeps running and might reserve capacity even when the Ray cluster is idle. With idle termination, the KubeRay operator and Ray autoscaler v2 work together to suspend or delete a RayCluster after it has had no active drivers for a configured period.

## How does the Ray autoscaler detect an idle Ray cluster?

The Ray autoscaler queries the Global Control Service (GCS) job table and checks that no driver has connected to the Ray cluster for `spec.idleTerminationOptions.timeoutSeconds`. The autoscaler ignores the following when it looks for drivers:

- Detached actors. A Ray cluster that runs only detached actors is still idle. See {ref}`actor-lifetimes`.
- Ray-internal jobs, such as the dashboard.
- A Ray job whose entrypoint never calls `ray.init()`. The entrypoint process doesn't connect to the Ray cluster, so it isn't a driver.

The autoscaler suspends or deletes the RayCluster `timeoutSeconds` after the last driver disconnects. If no driver has run, the countdown starts when the RayCluster is created or resumed. A connected driver keeps the RayCluster alive for as long as it runs.

If the autoscaler can't query the GCS job table, it treats the Ray cluster as having a connected driver and doesn't terminate it.

## How does each policy work?

Idle termination offers two policies, `Suspend` and `Delete`. `Suspend` is the default.

### `Suspend` policy

`Suspend` deletes the RayCluster's Pods but keeps the RayCluster custom resource. To resume the Ray cluster, patch `spec.idleSuspend` to `false`.

The autoscaler merge-patches `spec.idleSuspend: true` on the RayCluster, and the KubeRay operator handles `idleSuspend` the same way it handles `spec.suspend: true`. It deletes the RayCluster's Pods and keeps the RayCluster. The RayCluster reports the `RayClusterSuspended` status condition with the reason `RayClusterIdleSuspended`.

### `Delete` policy

`Delete` deletes the RayCluster's Pods and the RayCluster custom resource. You can't resume a deleted RayCluster.

The autoscaler adds the `ray.io/idle-termination-cleanup-finalizer` finalizer to the RayCluster with a JSON patch, then deletes the RayCluster. The KubeRay operator records a `DeletedIdleRayCluster` event and removes the finalizer so that the deletion can proceed.

## Prerequisites

Idle termination has the following requirements:

- Ray autoscaler v2 is enabled on the RayCluster. See {ref}`kuberay-autoscaler-v2`.
- `spec.rayVersion` is 2.60.0 or later, and the image runs Ray 2.60.0 or later.
- KubeRay is v1.8.0 or later.

The KubeRay operator validates the RayCluster against these requirements and rejects a RayCluster that doesn't meet them.

## RBAC and security

Idle termination doesn't need extra role-based access control (RBAC) setup. The KubeRay operator creates a Role for the Ray autoscaler of each RayCluster that grants `get`, `patch`, and `delete` on `rayclusters`. The Role uses `resourceNames` to limit these permissions to the autoscaler's own RayCluster, so the autoscaler can't suspend or delete any other RayCluster.

## Supported custom resources

Idle termination only works with the RayCluster custom resource. The KubeRay operator rejects a RayJob or RayService whose `rayClusterSpec` sets `idleTerminationOptions`.

## Configure idle termination

Set `spec.idleTerminationOptions` in your RayCluster:

```yaml
spec:
  rayVersion: "2.60.0"
  enableInTreeAutoscaling: true
  autoscalerOptions:
    version: v2
  idleTerminationOptions:
    timeoutSeconds: 300
    policy: Suspend
```

`idleTerminationOptions` has two fields:

- `timeoutSeconds`: Required. The number of seconds the Ray cluster must have no driver connected before the autoscaler applies the policy. The value must be non-negative.
- `policy`: The action the autoscaler takes once the Ray cluster has been idle for `timeoutSeconds`. Set it to `Suspend` or `Delete`. The default is `Suspend`.

See the [full sample YAML](https://github.com/kuberay-project/kuberay/blob/v1.8.0/ray-operator/config/samples/ray-cluster.autoscaler-v2-terminate-idle-cluster.yaml) in the KubeRay repository.

## Example

This example walks through the `Suspend` policy end to end, shows how a connected driver keeps the Ray cluster alive, and then switches to the `Delete` policy.

### Step 1: Create a RayCluster with idle termination enabled

Apply the [sample YAML](https://github.com/kuberay-project/kuberay/blob/v1.8.0/ray-operator/config/samples/ray-cluster.autoscaler-v2-terminate-idle-cluster.yaml). It suspends the RayCluster after no driver has connected for 30 seconds:

```bash
kubectl apply -f https://raw.githubusercontent.com/kuberay-project/kuberay/v1.8.0/ray-operator/config/samples/ray-cluster.autoscaler-v2-terminate-idle-cluster.yaml
kubectl wait --for=jsonpath='{.status.state}'=ready raycluster/ray-cluster-autoscaler-v2-terminate-idle-cluster --timeout=300s
kubectl get raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster
```

```text
NAME                                               DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
ray-cluster-autoscaler-v2-terminate-idle-cluster                                         1      2Gi      0      ready    50s
```

### Step 2: Wait for the RayCluster to be idle-suspended

Don't submit any workload. Once no driver has connected for `timeoutSeconds`, the autoscaler sets `spec.idleSuspend` to `true` and the KubeRay operator deletes the RayCluster's Pods. This step might take a few minutes. Check `spec.idleSuspend`:

```bash
kubectl get raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster -o jsonpath='{.spec.idleSuspend}'
```

```text
true
```

Confirm the suspension through the RayCluster state and the `RayClusterSuspended` status condition:

```bash
kubectl get raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster -o jsonpath='{.status.state}{"\n"}'
kubectl get raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster -o jsonpath='{.status.conditions[?(@.type=="RayClusterSuspended")].status}{" "}{.status.conditions[?(@.type=="RayClusterSuspended")].reason}{"\n"}'
```

```text
suspended
True RayClusterIdleSuspended
```

List the RayCluster's Pods to confirm that the KubeRay operator deleted them:

```bash
kubectl get pods -l ray.io/cluster=ray-cluster-autoscaler-v2-terminate-idle-cluster
```

```text
No resources found in default namespace.
```

### Step 3: Resume the RayCluster

Set `spec.idleSuspend` back to `false`. The KubeRay operator recreates the RayCluster's Pods and the RayCluster returns to `ready`:

```bash
kubectl patch raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster --type merge -p '{"spec":{"idleSuspend":false}}'
kubectl wait --for=jsonpath='{.status.state}'=ready raycluster/ray-cluster-autoscaler-v2-terminate-idle-cluster --timeout=120s
```

### Step 4: Keep the Ray cluster alive with a driver

The idle timer starts again after the RayCluster resumes, so run this step right after the RayCluster becomes `ready`. If the RayCluster is idle-suspended again before you submit the Ray job, repeat Step 3.

Submit a Ray job that connects to the Ray cluster and runs for 10 minutes. The entrypoint must call `ray.init()` to count as a driver. The `--no-wait` flag returns as soon as Ray accepts the job:

```bash
HEAD_POD=$(kubectl get pods -l ray.io/cluster=ray-cluster-autoscaler-v2-terminate-idle-cluster,ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
kubectl exec $HEAD_POD -c ray-head -- ray job submit --address http://localhost:8265 --no-wait -- python -c "import ray, time; ray.init(); time.sleep(600)"
```

Wait longer than `timeoutSeconds`, then verify that the RayCluster isn't suspended while the driver is running:

```bash
kubectl get raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster -o jsonpath='{.spec.idleSuspend}'
```

```text
false
```

Once the Ray job finishes and `timeoutSeconds` elapses with no driver connected, the autoscaler idle-suspends the RayCluster again.

### Step 5: Switch to the `Delete` policy

The Ray autoscaler reads `idleTerminationOptions` from the RayCluster every time it syncs with the Kubernetes API server, so you can change the policy of an existing RayCluster. Resume the RayCluster and set the policy to `Delete` in one patch:

```bash
kubectl patch raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster --type merge -p '{"spec":{"idleSuspend":false,"idleTerminationOptions":{"policy":"Delete"}}}'
```

Don't submit any workload. Once no driver has connected for `timeoutSeconds`, the autoscaler deletes the RayCluster:

```bash
kubectl get raycluster ray-cluster-autoscaler-v2-terminate-idle-cluster
```

```text
Error from server (NotFound): rayclusters.ray.io "ray-cluster-autoscaler-v2-terminate-idle-cluster" not found
```

The KubeRay operator records a `DeletedIdleRayCluster` event before it removes the finalizer. List the event:

```bash
kubectl get events --field-selector reason=DeletedIdleRayCluster
```

```text
LAST SEEN   TYPE     REASON                  OBJECT                                                        MESSAGE
5s          Normal   DeletedIdleRayCluster   raycluster/ray-cluster-autoscaler-v2-terminate-idle-cluster   RayCluster default/ray-cluster-autoscaler-v2-terminate-idle-cluster is being deleted due to idle termination
```

### Step 6: Clean up

Step 5 already deletes the RayCluster. If you skipped Step 5, delete the RayCluster:

```bash
kubectl delete -f https://raw.githubusercontent.com/kuberay-project/kuberay/v1.8.0/ray-operator/config/samples/ray-cluster.autoscaler-v2-terminate-idle-cluster.yaml
```
