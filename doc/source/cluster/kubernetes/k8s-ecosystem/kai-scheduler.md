(kuberay-kai-scheduler)=
# Gang scheduling, queue priority, and GPU sharing for RayClusters using KAI Scheduler

This guide demonstrates how to use KAI Scheduler for setting up hierarchical queues with quotas, gang scheduling, and GPU sharing using RayClusters.


## KAI Scheduler

[KAI Scheduler](https://github.com/NVIDIA/KAI-Scheduler) is a high-performance, scalable Kubernetes scheduler built for AI/ML workloads. Designed to orchestrate GPU clusters at massive scale, KAI optimizes GPU allocation and supports the full AI lifecycle - from interactive development to large distributed training and inference. Some of the key features are:
- **Bin packing and spread scheduling**: Optimize node usage either by minimizing fragmentation (bin packing) or increasing resiliency and load balancing (spread scheduling)
- **GPU sharing**: Allow KAI to pack multiple Ray workloads from across teams on the same GPU, letting your organization fit more work onto your existing hardware and reducing idle GPU time.
- **Workload autoscaling**: Scale Ray replicas or workers within min/max while respecting gang constraints
- **Cluster autoscaling**: Compatible with dynamic cloud infrastructures (including auto-scalers like Karpenter)
- **Workload priorities**: Prioritize Ray workloads effectively within queues
- **Hierarchical queues and fairness**: Two-level queues with quotas, over-quota weights, limits, and equitable resource distribution between queues using DRF
and many more.
For more details and key features, see [the documentation](https://github.com/NVIDIA/KAI-Scheduler?tab=readme-ov-file#key-features).

### Core components

1. **PodGroups**: PodGroups are atomic units for scheduling and represent one or more interdependent pods that the scheduler execute as a single unit, also known as gang scheduling. They are vital for distributed workloads. KAI Scheduler includes a **PodGrouper** that handles gang scheduling automatically.

**How PodGrouper works:**
```
RayCluster "distributed-training":
├── Head Pod: 1 GPU
└── Worker Group: 4 × 0.5 GPU = 2 GPUs
Total Group Requirement: 3 GPUs

PodGrouper schedules all 5 pods (1 head + 4 workers) together or none at all.
```

2. **Queues**: Queues enforce fairness in resource distribution using:

- Quota: The baseline amount of resources guaranteed to the queue. The scheduler allocates quotas first to ensure fairness.
- Queue priority: Determines the order in which queues receive resources beyond their quota. The scheduler serves the higher-priority queues first.
- Over-quota weight: Controls how the scheduler divides surplus resources among queues within the same priority level. Queues with higher weights receive a larger share of the extra resources.
- Limit: Defines the maximum resources that the queue can consume.

You can arrange queues hierarchically for organizations with multiple teams, for example, departments with multiple teams.

## [Prerequisites](https://github.com/NVIDIA/KAI-Scheduler?tab=readme-ov-file#prerequisites)

* Kubernetes cluster with GPU nodes
* NVIDIA GPU Operator 
* kubectl configured to access your cluster
* Install KAI Scheduler with gpu-sharing enabled. Choose the desired release version from [KAI Scheduler releases](https://github.com/NVIDIA/KAI-Scheduler/releases) and replace the `<KAI_SCHEDULER_VERSION>` in the following command. It's recommended to choose v0.10.0 or higher version.

```bash
# Install KAI Scheduler
helm upgrade -i kai-scheduler oci://ghcr.io/nvidia/kai-scheduler/kai-scheduler -n kai-scheduler --create-namespace --version <KAI_SCHEDULER_VERSION> --set "global.gpuSharing=true"
```

## Step 1: Install the KubeRay operator with KAI Scheduler as the batch scheduler

Follow the official KubeRay operator [installation documentation](https://docs.ray.io/en/master/cluster/kubernetes/getting-started/kuberay-operator-installation.html#kuberay-operator-installation) and add the following configuration to enable KAI Scheduler integration:

```bash
--set batchScheduler.name=kai-scheduler
```

## Step 2: Create KAI Scheduler Queues

Create a basic queue structure for department-1 and its child team-a. For demo reasons, this example doesn't enforce any quota, overQuotaWeight, or limit. You can configure these parameters depending on your needs: 

```yaml
apiVersion: scheduling.run.ai/v2
kind: Queue
metadata:
  name: department-1
spec:
  #priority: 100 (optional)
  resources:
    cpu:
      quota: -1
      limit: -1
      overQuotaWeight: 1
    gpu:
      quota: -1
      limit: -1
      overQuotaWeight: 1
    memory:
      quota: -1
      limit: -1
      overQuotaWeight: 1
---
apiVersion: scheduling.run.ai/v2
kind: Queue
metadata:
  name: team-a
spec:
  #priority: 200 (optional)
  parentQueue: department-1
  resources:
    cpu:
      quota: -1
      limit: -1
      overQuotaWeight: 1
    gpu:
      quota: -1
      limit: -1
      overQuotaWeight: 1
    memory:
      quota: -1
      limit: -1
      overQuotaWeight: 1

```

Note: To make this demo easier to follow, we combined these queue definitions with the RayCluster example in the next step. You can use the single combined YAML file and apply both queues and workloads at once.

## Step 3: Gang scheduling with KAI Scheduler

The key pattern is to add the queue label to your RayCluster. [Here's a basic example](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/ray-cluster.kai-scheduler.yaml) from the KubeRay repository:

```yaml
metadata:
  name: raycluster-sample
  labels:
    kai.scheduler/queue: team-a    # This is the essential configuration.
```

Apply this RayCluster with queues:

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.kai-scheduler.yaml

kubectl apply -f ray-cluster.kai-scheduler.yaml

#Verify queues are created
kubectl get queues
# NAME           PRIORITY   PARENT         CHILDREN     DISPLAYNAME
# department-1                             ["team-a"]   
# team-a                    department-1                

# Watch the pods get scheduled
kubectl get pods -w
# NAME                                    READY   STATUS              RESTARTS   AGE
# kuberay-operator-7d86f4f46b-dq22x       1/1     Running             0          50s
# raycluster-sample-head-rvrkz            0/1     ContainerCreating   0          13s
# raycluster-sample-worker-worker-mlvtz   0/1     Init:0/1            0          13s
# raycluster-sample-worker-worker-rcb54   0/1     Init:0/1            0          13s
# raycluster-sample-worker-worker-mlvtz   0/1     Init:0/1            0          40s
# raycluster-sample-worker-worker-rcb54   0/1     Init:0/1            0          41s
# raycluster-sample-head-rvrkz            0/1     Running             0          42s
# raycluster-sample-head-rvrkz            1/1     Running             0          54s
# raycluster-sample-worker-worker-rcb54   0/1     PodInitializing     0          59s
# raycluster-sample-worker-worker-mlvtz   0/1     PodInitializing     0          59s
# raycluster-sample-worker-worker-rcb54   0/1     Running             0          60s
# raycluster-sample-worker-worker-mlvtz   0/1     Running             0          60s
# raycluster-sample-worker-worker-rcb54   1/1     Running             0          71s
# raycluster-sample-worker-worker-mlvtz   1/1     Running             0          71s
```

## Set priorities for workloads

In Kubernetes, assigning different priorities to workloads ensures efficient resource management, minimizes service disruption, and supports better scaling. By prioritizing workloads, KAI Scheduler schedules jobs according to their assigned priority. When sufficient resources aren't available for a workload, the scheduler can preempt lower-priority workloads to free up resources for higher-priority ones. This approach ensures the scheduler always prioritizes that mission-critical services in resource allocation.

KAI scheduler deployment comes with several predefined priority classes:

- train (50) - use for preemptible training workloads
- build-preemptible (75) - use for preemptible build/interactive workloads
- build (100) - use for build/interactive workloads (non-preemptible)
- inference (125) - use for inference workloads (non-preemptible)

You can submit the same workload above with a specific priority. Modify the above example into a build class workload:

```yaml
  labels:
    kai.scheduler/queue: team-a    # This is the essential configuration.
    priorityClassName: build       # Here you can specify the priority class in metadata.labels (optional)
```
See the [documentation](https://github.com/NVIDIA/KAI-Scheduler/tree/main/docs/priority) for more information.

## Step 4: Submitting Ray workers with GPU sharing 

This example creates two workers that share a single GPU (0.5 each, with time-slicing) within a RayCluster. See the [YAML file](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/ray-cluster.kai-gpu-sharing.yaml)):

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.kai-gpu-sharing.yaml

kubectl apply -f ray-cluster.kai-gpu-sharing.yaml

# Watch the pods get scheduled
kubectl get pods -w
# NAME                                          READY   STATUS    RESTARTS   AGE
# kuberay-operator-7d86f4f46b-dq22x             1/1     Running   0          4m9s
# raycluster-half-gpu-head-9rtxf                0/1     Running   0          4s
# raycluster-half-gpu-shared-gpu-worker-5l7cn   0/1     Pending   0          4s
# raycluster-half-gpu-shared-gpu-worker-98tzh   0/1     Pending   0          4s
# ... (skip for brevity)
# raycluster-half-gpu-shared-gpu-worker-5l7cn   0/1     Init:0/1   0          6s
# raycluster-half-gpu-shared-gpu-worker-5l7cn   0/1     Init:0/1   0          7s
# raycluster-half-gpu-shared-gpu-worker-98tzh   0/1     Init:0/1   0          8s
# raycluster-half-gpu-head-9rtxf                1/1     Running    0          19s
# raycluster-half-gpu-shared-gpu-worker-5l7cn   0/1     PodInitializing   0          19s
# raycluster-half-gpu-shared-gpu-worker-98tzh   0/1     PodInitializing   0          19s
# raycluster-half-gpu-shared-gpu-worker-5l7cn   0/1     Running           0          20s
# raycluster-half-gpu-shared-gpu-worker-98tzh   0/1     Running           0          20s
# raycluster-half-gpu-shared-gpu-worker-5l7cn   1/1     Running           0          31s
# raycluster-half-gpu-shared-gpu-worker-98tzh   1/1     Running           0          31s
```

Note: GPU sharing with time slicing in this example occurs only at the Kubernetes layer, allowing multiple pods to share a single GPU device. The scheduler doesn't enforce memory isolation, so applications must manage their own usage to prevent interference. For other GPU sharing approaches (e.g., MPS), see the [the KAI documentation](https://github.com/NVIDIA/KAI-Scheduler/tree/main/docs/gpu-sharing).

### Verify GPU sharing is working

To confirm that GPU sharing is working correctly, use these commands:

```bash
# 1. Check GPU fraction annotations and shared GPU groups
kubectl get pods -l ray.io/cluster=raycluster-half-gpu -o custom-columns="NAME:.metadata.name,NODE:.spec.nodeName,GPU-FRACTION:.metadata.annotations.gpu-fraction,GPU-GROUP:.metadata.labels.runai-gpu-group"
```

You should see both worker pods on the same node with `GPU-FRACTION: 0.5` and the same `GPU-GROUP` ID:

```bash
NAME                                          NODE               GPU-FRACTION   GPU-GROUP
raycluster-half-gpu-head                      ip-xxx-xx-xx-xxx   <none>         <none>
raycluster-half-gpu-shared-gpu-worker-67tvw   ip-xxx-xx-xx-xxx   0.5            3e456911-a6ea-4b1a-8f55-e90fba89ad76
raycluster-half-gpu-shared-gpu-worker-v5tpp   ip-xxx-xx-xx-xxx   0.5            3e456911-a6ea-4b1a-8f55-e90fba89ad76
```

This shows that both workers have the same `NVIDIA_VISIBLE_DEVICES` (same physical GPU) and `GPU-FRACTION: 0.50`.

## Troubleshooting

### Check for missing queue labels

If pods remain in `Pending` state, the most common issue is missing queue labels.

Check operator logs for KAI Scheduler errors and look for error messages like:

```bash
"Queue label missing from RayCluster; pods will remain pending"
```
**Solution**: Ensure your RayCluster has the queue label that exists in the cluster:

```yaml
metadata:
  labels:
    kai.scheduler/queue: default  # Add this label
```
