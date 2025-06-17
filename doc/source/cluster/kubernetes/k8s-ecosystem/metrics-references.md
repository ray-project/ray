(kuberay-metrics-references)=

# KubeRay metrics references

## `controller-runtime` metrics
KubeRay exposes metrics provided by [kubernetes-sigs/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime), including information about reconciliation, work queues, and more, to help users operate the KubeRay operator in production environments.

For more details about the default metrics provided by [kubernetes-sigs/controller-runtime](https://github.com/kubernetes-sigs/controller-runtime), see [Default Exported Metrics References](https://book.kubebuilder.io/reference/metrics-reference).

## KubeRay custom metrics

Starting with KubeRay 1.4.0, KubeRay provides metrics for its custom resources to help users better understand Ray clusters and Ray applications.

You can view these metrics by following the instructions below:
```sh
# Forward a local port to the KubeRay operator service.
kubectl port-forward service/kuberay-operator 8080

# View the metrics.
curl localhost:8080/metrics

# You should see metrics like the following if a RayCluster already exists:  
# kuberay_cluster_info{name="raycluster-kuberay",namespace="default",owner_kind="None"} 1
```

### RayCluster metrics


| Metric name                                      | Type  | Description                                                                                                                | Labels                                                               |
|--------------------------------------------------|-------|----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `kuberay_cluster_info`                           | Gauge | Metadata information about RayCluster custom resources.                                                                     | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;<br/> `owner_kind`: &lt;RayJob\|RayService\|None&gt; |
| `kuberay_cluster_condition_provisioned`          | Gauge | Indicates whether the RayCluster is provisioned. See [RayClusterProvisioned](https://github.com/ray-project/kuberay/blob/7c6aedff5b4106281f50e87a7e9e177bf1237ec7/ray-operator/apis/ray/v1/raycluster_types.go#L214) for more information.                                      | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;<br/> `condition`: &lt;true\|false&gt;               |
| `kuberay_cluster_provisioned_duration_seconds`   | Gauge | The time, in seconds, when a RayCluster's `RayClusterProvisioned` status transitions from false (or unset) to true.         | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;                                              |

### RayService metrics

| Metric name                                       | Type  | Description                                                | Labels                                                               |
|--------------------------------------------------|-------|------------------------------------------------------------|--------------------------------------------------------------------|
| `kuberay_service_info`                           | Gauge | Metadata information about RayService custom resources.     | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                               |
| `kuberay_service_condition_ready`                | Gauge | Describes whether the RayService is ready. Ready means users can send requests to the underlying cluster and the number of serve endpoints is greater than 0. See [RayServiceReady](https://github.com/ray-project/kuberay/blob/33ee6724ca2a429c77cb7ff5821ba9a3d63f7c34/ray-operator/apis/ray/v1/rayservice_types.go#L135) for more information.                                           | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                             |
| `kuberay_service_condition_upgrade_in_progress`  | Gauge | Describes whether the RayService is performing a zero-downtime upgrade. See [UpgradeInProgress](https://github.com/ray-project/kuberay/blob/33ee6724ca2a429c77cb7ff5821ba9a3d63f7c34/ray-operator/apis/ray/v1/rayservice_types.go#L137) for more information.                                         | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                              |

### RayJob metrics

| Metric name                                       | Type  | Description                                                | Labels                                                                   |
|--------------------------------------------------|-------|------------------------------------------------------------|---------------------------------------------------------------------------|
| `kuberay_job_info`                               | Gauge | Metadata information about RayJob custom resources.         | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;                                                   |
| `kuberay_job_deployment_status`                  | Gauge | The RayJob's current deployment status.                      | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;<br/> `deployment_status`: &lt;New\|Initializing\|Running\|Complete\|Failed\|Suspending\|Suspended\|Retrying\|Waiting&gt;                          |
| `kuberay_job_execution_duration_seconds`         | Gauge | Duration of the RayJob CR’s JobDeploymentStatus transition from `Initializing` to either the `Retrying` state or a terminal state, such as `Complete` or `Failed`. The `Retrying` state indicates that the CR previously failed and that spec.backoffLimit is enabled.   | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;<br/> `job_deployment_status`: &lt;Complete\|Failed&gt;<br/> `retry_count`: &lt;count&gt; |


