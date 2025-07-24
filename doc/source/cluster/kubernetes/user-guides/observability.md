(kuberay-observability)=

# KubeRay Observability

## KubeRay / Kubernetes Observability

### Check KubeRay operator's logs for errors

```bash
# Typically, the operator's Pod name is kuberay-operator-xxxxxxxxxx-yyyyy.
kubectl logs $KUBERAY_OPERATOR_POD -n $YOUR_NAMESPACE | tee operator-log
```

Use this command to redirect the operator's logs to a file called `operator-log`. Then search for errors in the file.

### Check the status and events of custom resources

```bash
kubectl describe [raycluster|rayjob|rayservice] $CUSTOM_RESOURCE_NAME -n $YOUR_NAMESPACE
```

After running this command, check events and the `state`, and `conditions` in the status of the custom resource for any errors and progress.


#### RayCluster `.Status.State`

The `.Status.State` field represents the cluster's situation, but its limited representation restricts its utility. Replace it with the new `Status.Conditions` field.

| State     | Description                                                                                                                            |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------|
| Ready     | KubeRay sets the state to `Ready` once all the Pods in the cluster are ready. The `State` remains `Ready` until KubeRay suspends the cluster. |
| Suspended | KubeRay sets the state to `Suspended` when it sets `Spec.Suspend` to true and deletes all Pods in the cluster.                                     |

#### RayCluster `.Status.Conditions`

Although `Status.State` can represent the cluster situation, it's still only a single field. By enabling the feature gate `RayClusterStatusConditions` on the KubeRay v1.2.1, you can access to new `Status.Conditions` for more detailed cluster history and states.

:::{warning}
`RayClusterStatusConditions` is still an alpha feature and may change in the future.
:::

If you deployed KubeRay with Helm, then enable the `RayClusterStatusConditions` gate in the `featureGates` of your Helm values.

```bash
helm upgrade kuberay-operator kuberay/kuberay-operator --version 1.2.2 \
  --set featureGates\[0\].name=RayClusterStatusConditions \
  --set featureGates\[0\].enabled=true
```

Or, just make your KubeRay Operator executable run with `--feature-gates=RayClusterStatusConditions=true` argument.

| Type                     | Status | Reason                         | Description                                                                                                          |
|--------------------------|--------|--------------------------------|----------------------------------------------------------------------------------------------------------------------|
| RayClusterProvisioned    | True   | AllPodRunningAndReadyFirstTime | When all Pods in the cluster become ready, the system marks the condition as `True`. Even if some Pods fail later, the system maintains this `True` state. |
|                          | False  | RayClusterPodsProvisioning     |                                                                                                                      |
| RayClusterReplicaFailure | True   | FailedDeleteAllPods            | KubeRay sets this condition to `True` when there's a reconciliation error, otherwise KubeRay clears the condition.            |
|                          | True   | FailedDeleteHeadPod            | See the `Reason` and the `Message` of the condition for more detailed debugging information.                 |
|                          | True   | FailedCreateHeadPod            |                                                                                                                      |
|                          | True   | FailedDeleteWorkerPod          |                                                                                                                      |
|                          | True   | FailedCreateWorkerPod          |                                                                                                                      |
| HeadPodReady             | True   | HeadPodRunningAndReady         | This condition is `True` only if the HeadPod is currently ready; otherwise, it's `False`.                |
|                          | False  | HeadPodNotFound                |                                                                                                                      |

#### RayService `.Status.Conditions`

From KubeRay v1.3.0, RayService also supports the `Status.Conditions` field.

* `Ready`: If `Ready` is true, the RayService is ready to serve requests.
* `UpgradeInProgress`: If `UpgradeInProgress` is true, the RayService is currently in the upgrade process and both active and pending RayCluster exist.

```sh
kubectl describe rayservices.ray.io rayservice-sample

# [Example output]
# Conditions:
#   Last Transition Time:  2025-02-08T06:45:20Z
#   Message:               Number of serve endpoints is greater than 0
#   Observed Generation:   1
#   Reason:                NonZeroServeEndpoints
#   Status:                True
#   Type:                  Ready
#   Last Transition Time:  2025-02-08T06:44:28Z
#   Message:               Active Ray cluster exists and no pending Ray cluster
#   Observed Generation:   1
#   Reason:                NoPendingCluster
#   Status:                False
#   Type:                  UpgradeInProgress
```


#### Kubernetes Events

KubeRay creates Kubernetes events for every interaction between the KubeRay operator and the Kubernetes API server, such as creating a Kubernetes service, updating a RayCluster, and deleting a RayCluster.
In addition, if the validation of the custom resource fails, KubeRay also creates a Kubernetes event.

```sh
# Example:
kubectl describe rayclusters.ray.io raycluster-kuberay

# Events:
#   Type    Reason            Age   From                   Message
#   ----    ------            ----  ----                   -------
#   Normal  CreatedService    37m   raycluster-controller  Created service default/raycluster-kuberay-head-svc
#   Normal  CreatedHeadPod    37m   raycluster-controller  Created head Pod default/raycluster-kuberay-head-l7v7q
#   Normal  CreatedWorkerPod  ...
```

## Ray Observability

### Ray Dashboard

* To view the [Ray dashboard](observability-getting-started) running on the head Pod, follow [these instructions](kuberay-port-forward-dashboard).
* To integrate the Ray dashboard with Prometheus and Grafana, see [Using Prometheus and Grafana](kuberay-prometheus-grafana) for more details.
* To enable the "CPU Flame Graph" and "Stack Trace" features, see [Profiling with py-spy](kuberay-pyspy-integration).

### Check logs of Ray Pods

Check the Ray logs directly by accessing the log files on the Pods. See [Ray Logging](configure-logging) for more details.

```bash
kubectl exec -it $RAY_POD -n $YOUR_NAMESPACE -- bash
# Check the logs under /tmp/ray/session_latest/logs/
```

(kuberay-port-forward-dashboard)=
### Check Dashboard

```bash
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl port-forward $HEAD_POD -n $YOUR_NAMESPACE 8265:8265
# Check $YOUR_IP:8265 in your browser to access the dashboard.
# For most cases, 127.0.0.1:8265 or localhost:8265 should work.
```

### Ray State CLI

You can use the [Ray State CLI](state-api-cli-ref) on the head Pod to check the status of Ray Serve applications.

```bash
# Log into the head Pod
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- ray summary actors

# [Example output]:
# ======== Actors Summary: 2023-07-11 17:58:24.625032 ========
# Stats:
# ------------------------------------
# total_actors: 14


# Table (group by class):
# ------------------------------------
#     CLASS_NAME                          STATE_COUNTS
# 0   ...                                 ALIVE: 1
# 1   ...                                 ALIVE: 1
# 2   ...                                 ALIVE: 3
# 3   ...                                 ALIVE: 1
# 4   ...                                 ALIVE: 1
# 5   ...                                 ALIVE: 1
# 6   ...                                 ALIVE: 1
# 7   ...                                 ALIVE: 1
# 8   ...                                 ALIVE: 1
# 9   ...                                 ALIVE: 1
# 10  ...                                 ALIVE: 1
# 11  ...                                 ALIVE: 1
```
