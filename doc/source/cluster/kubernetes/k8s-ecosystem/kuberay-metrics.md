(kuberay-metrics-references)=

# KubeRay Metrics References

## Controller Runtime Metrics
KubeRay uses Controller Runtime to build controllers, and Controller Runtime provides some metrics by default that are aggregated in KubeRay metrics.
Controller runtime provides these metrics natively:
- Total number of reconciliations (success, error, requeue, etc.)
- Length of reconcile queue
- Reconciliation latency
- Total CPU spent time, resident, virtual memory size, file descriptor usage
- Go runtime metrics such as number of Goroutines, GC duration

For more information, refer to [metrics-reference](https://book.kubebuilder.io/reference/metrics-reference).

## KubeRay Custom Metrics

Starting from KubeRay 1.4.0, KubeRay introduces new metrics for KubeRay's three Custom Resources, RayCluster, RayService, and RayJob.

You can check them by following the instruction:
```sh
# forward the metrics port(8080) from KubeRay operator
kubectl port-forward service/kuberay-operator 8080

# check the metrics
curl localhost:8080/metrics

# something like this should appear if you have existed RayCluster 
# kuberay_cluster_info{name="raycluster-kuberay",namespace="default",owner_kind="None"} 1
```

### Ray Cluster CR


| Metric Name                                      | Type  | Description                                                                                                                | Labels                                                               |
|--------------------------------------------------|-------|----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `kuberay_cluster_info`                           | Gauge | Metadata information about RayCluster custom resources                                                                     | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;<br/> `owner_kind`: &lt;RayJob\|RayService\|None&gt; |
| `kuberay_cluster_condition_provisioned`          | Gauge | Describes whether all ray pods are ready for the first time, refer to [RayClusterProvisioned](https://github.com/ray-project/kuberay/blob/7c6aedff5b4106281f50e87a7e9e177bf1237ec7/ray-operator/apis/ray/v1/raycluster_types.go#L214) for more information                                      | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;<br/> `condition`: &lt;true\|false&gt;               |
| `kuberay_cluster_provisioned_duration_seconds`   | Gauge | The time, in seconds, when a RayCluster's `RayClusterProvisioned` status transitions from false (or unset) to true         | `namespace`: &lt;RayCluster-namespace&gt;<br/> `name`: &lt;RayCluster-name&gt;                                              |

### Ray Service CR

| Metric Name                                       | Type  | Description                                                | Labels                                                               |
|--------------------------------------------------|-------|------------------------------------------------------------|--------------------------------------------------------------------|
| `kuberay_service_info`                           | Gauge | Metadata information about RayService custom resources     | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                               |
| `kuberay_service_condition_ready`                | Gauge | Shows whether RayService is in condition of [RayServiceReady](https://github.com/ray-project/kuberay/blob/33ee6724ca2a429c77cb7ff5821ba9a3d63f7c34/ray-operator/apis/ray/v1/rayservice_types.go#L135)                                           | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                             |
| `kuberay_service_condition_upgrade_in_progress`  | Gauge | Shows whether RayService is in condition of [UpgradeInProgress](https://github.com/ray-project/kuberay/blob/33ee6724ca2a429c77cb7ff5821ba9a3d63f7c34/ray-operator/apis/ray/v1/rayservice_types.go#L137)                                         | `namespace`: &lt;RayService-namespace&gt;<br/> `name`: &lt;RayService-name&gt;                                              |

### Ray Job CR

| Metric Name                                       | Type  | Description                                                | Labels                                                                   |
|--------------------------------------------------|-------|------------------------------------------------------------|---------------------------------------------------------------------------|
| `kuberay_job_info`                               | Gauge | Metadata information about RayJob custom resources         | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;                                                   |
| `kuberay_job_deployment_status`                  | Gauge | The RayJobs current deployment status                      | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;<br/> `deployment_status`: &lt;New\|Initializing\|Running\|Complete\|Failed\|Suspending\|Suspended\|Retrying\|Waiting&gt;                          |
| `kuberay_job_execution_duration_seconds`         | Gauge | Duration from RayJob CR initialization to terminal state   | `namespace`: &lt;RayJob-namespace&gt;<br/> `name`: &lt;RayJob-name&gt;<br/> `result`: &lt;Complete\|Failed&gt;<br/> `retry_count`: &lt;count&gt; |


