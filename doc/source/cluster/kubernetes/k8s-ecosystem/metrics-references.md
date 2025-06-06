(kuberay-metrics-references)=

# KubeRay Metrics References

## Controller Runtime Metrics
KubeRay is built with Controller Runtime, which natively exposes metrics that are included in KubeRay’s metrics. These include:
- Reconciliation counts (success, error, requeue)
- Length of the reconcile queue
- Reconciliation latency
- CPU, memory, and file descriptor usage
- Go runtime stats (Goroutines, GC duration)

For more information, refer to [metrics-reference](https://book.kubebuilder.io/reference/metrics-reference).

## KubeRay Custom Metrics

Starting with KubeRay 1.4.0, new metrics have been added for KubeRay's three custom resources: RayCluster, RayService, and RayJob

You can view these metrics by following the instructions below:
```sh
# Forward a local port to the KubeRay operator service.
kubectl port-forward service/kuberay-operator 8080

# View the metrics
curl localhost:8080/metrics

# You should see metric like this if a RayCluster already exists,  
# kuberay_cluster_info{name="raycluster-kuberay",namespace="default",owner_kind="None"} 1
```

### Ray Cluster


| Metric Name                                      | Type  | Description                                                                                                                | Labels                                                               |
|--------------------------------------------------|-------|----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `kuberay_cluster_info`                           | Gauge | Metadata information about RayCluster custom resources                                                                     | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;<br/> `owner_kind`: &lt;RayJob\|RayService\|None&gt; |
| `kuberay_cluster_condition_provisioned`          | Gauge | Indicates whether the RayCluster is provisioned. Refer to [RayClusterProvisioned](https://github.com/ray-project/kuberay/blob/7c6aedff5b4106281f50e87a7e9e177bf1237ec7/ray-operator/apis/ray/v1/raycluster_types.go#L214) for more information                                      | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;<br/> `condition`: &lt;true\|false&gt;               |
| `kuberay_cluster_provisioned_duration_seconds`   | Gauge | The time, in seconds, when a RayCluster's `RayClusterProvisioned` status transitions from false (or unset) to true         | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;                                              |

### Ray Service

| Metric Name                                       | Type  | Description                                                | Labels                                                               |
|--------------------------------------------------|-------|------------------------------------------------------------|--------------------------------------------------------------------|
| `kuberay_service_info`                           | Gauge | Metadata information about RayService custom resources     | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                               |
| `kuberay_service_condition_ready`                | Gauge | RayServiceReady means users can send requests to the underlying cluster and the number of serve endpoints is greater than 0. Refer to [RayServiceReady](https://github.com/ray-project/kuberay/blob/33ee6724ca2a429c77cb7ff5821ba9a3d63f7c34/ray-operator/apis/ray/v1/rayservice_types.go#L135) for more information                                           | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                             |
| `kuberay_service_condition_upgrade_in_progress`  | Gauge | UpgradeInProgress means the RayService is currently performing a zero-downtime upgrade. Refer to [UpgradeInProgress](https://github.com/ray-project/kuberay/blob/33ee6724ca2a429c77cb7ff5821ba9a3d63f7c34/ray-operator/apis/ray/v1/rayservice_types.go#L137) for more information                                         | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                              |

### Ray Job

| Metric Name                                       | Type  | Description                                                | Labels                                                                   |
|--------------------------------------------------|-------|------------------------------------------------------------|---------------------------------------------------------------------------|
| `kuberay_job_info`                               | Gauge | Metadata information about RayJob custom resources         | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;                                                   |
| `kuberay_job_deployment_status`                  | Gauge | The RayJobs current deployment status                      | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;<br/> `deployment_status`: &lt;New\|Initializing\|Running\|Complete\|Failed\|Suspending\|Suspended\|Retrying\|Waiting&gt;                          |
| `kuberay_job_execution_duration_seconds`         | Gauge | Duration from RayJob CR initialization to terminal state   | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;<br/> `result`: &lt;Complete\|Failed&gt;<br/> `retry_count`: &lt;count&gt; |


