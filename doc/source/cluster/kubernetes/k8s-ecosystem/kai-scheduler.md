(kuberay-kai-scheduler)=
# Gang Scheduling, Queue Priority, and GPU Sharing for Ray Clusters using KAI Scheduler

This guide demonstrates how to use KAI Scheduler for setting up hierarchical queues with quotas, gang scheduling and GPU sharing using RayCluster. KAI Scheduler enables multiple Ray workers to share GPU resources through fractional allocation, maximizing GPU utilization and reducing costs.


## KAI Scheduler

[KAI Scheduler](https://github.com/NVIDIA/KAI-Scheduler) is a high-performance, scalable Kubernetes scheduler built for AI/ML workloads. Designed to manage large-scale GPU clusters, including thousands of nodes, and high-throughput of workloads, makes the KAI Scheduler ideal for extensive and demanding environments. It optimizes GPU resource allocation and supports the whole AI lifecycle — from interactive development sessions to large-scale distributed training and inference.

For more details and key features, please refer to [the documentation](https://github.com/NVIDIA/KAI-Scheduler?tab=readme-ov-file#key-features).


### Core Components

1. **PodGroups**: Pogroups are atomic units for scheduling and represent one or more interdependent pods that must be executed as a single unit, also known as gang scheduling. They are vital for distributed workloads. KAI Scheduler includes an **PodGrouper** that handles gang scheduling automatically.

**How PodGrouper works:**
```
RayCluster "distributed-training":
├── Head Pod: 1 GPU
└── Worker Group: 4 × 0.5 GPU = 2 GPUs
Total Group Requirement: 3 GPUs

PodGrouper ensures all 5 pods (1 head + 4 workers) are scheduled 
together or none at all.
```

2. **Queues**: Queues enforce fairness in resource distribution using:

- Quota: The baseline amount of resources guaranteed to the queue. Quotas are allocated first to ensure fairness.
- Queue Priority: Determines the order in which queues receive resources beyond their quota. Higher-priority queues are served first.
- Over-Quota Weight: Controls how surplus resources are shared among queues within the same priority level. Queues with higher weights receive a larger share of the extra resources.
- Limit: Defines the maximum resources that the queue can consume.

Queues can be arranged hierarchically for organizations with multiple teams (e.g. departments with multiple teams).

## Prerequisites

* Kubernetes cluster with GPU nodes
* NVIDIA GPU Operator 
* kubectl configured to access your cluster

## Step 1: Install KAI Scheduler

Install KAI Scheduler with gpu-sharing enabled:

```bash
# Install KAI Scheduler
helm upgrade -i kai-scheduler oci://ghcr.io/nvidia/kai-scheduler/kai-scheduler -n kai-scheduler --create-namespace --version <KAI_SCHEDULER_VERSION> --set "global.gpuSharing=true"
```

## Step 2: Install the KubeRay operator with KAI Scheduler as the batch scheduler

helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator --version <KUBERAY_OPERATOR_VERSION> --set batchScheduler.name=kai-scheduler

See the [official documentation of KubeRay operator](https://docs.ray.io/en/master/cluster/kubernetes/getting-started/kuberay-operator-installation.html#kuberay-operator-installation)for more information.

## Step 3: Create KAI Scheduler Queues

Create a basic queue structure for department-1 and its child team-a (for demo reasons, we did not enforce any quota, overQuotaWeight and limit. Users can setup these parameters depending on their needs): 

```bash
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

# Verify queues are created
kubectl get queues
```

**How Scheduling Logic Works with Queues**

1. Priority-based ordering: Higher priority queues scheduled first
2. Quota distribution: Resources distributed according to each queue's deserved quota
3. Over-quota handling: Queues can exceed quota if resources available
4. Preemption: Higher priority workloads can preempt lower priority jobs that are running over-quota

## Step 4: Gang-Scheduling with KAI Scheduler

The key pattern simply adding the queue label to your RayCluster. [Here's a basic example](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/ray-cluster.kai-scheduler.yaml), which you can also find in kuberay repository:

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-sample
  labels:
    kai.scheduler/queue: team-a    # This is the essential configuration!
spec:
  headGroupSpec:
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.41.0
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
  workerGroupSpecs:
  - groupName: worker
    replicas: 2
    minReplicas: 2
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.41.0
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"

```

Apply this RayCluster:

```bash
kubectl apply -f ray-cluster.kai-scheduler.yaml

# Watch the pods get scheduled
kubectl get pods -w
```


## Setting Priorities for Workloads

In Kubernetes, assigning different priorities to workloads ensures efficient resource management, minimizes service disruption, and supports better scaling. By prioritizing workloads, KAI Scheduler schedules jobs according to their assigned priority. When sufficient resources aren't available for a workload, the scheduler can preempt lower-priority workloads to free up resources for higher-priority ones. This approach ensures that mission-critical services are always prioritized in resource allocation.

KAI scheduler deployment comes with several predefined priority classes:

- train (50) - can be used for preemptible training workloads
- build-preemptible (75) - can be used for preemptible build/interactive workloads
- build (100) - can be used for build/interactive workloads (non-preemptible)
- inference (125) - can be used for inference workloads (non-preemptible)

You can submit the same workload above with a specific priority. Here is an example how to turn the above example into a build class workload

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-sample
  labels:
    kai.scheduler/queue: team-a    # This is the essential configuration!
    priorityClassName: build       # Here you can specify the priority class
spec:
  headGroupSpec:
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.41.0
          resources:
            requests:
              cpu: "1"
              memory: "2Gi"
  workerGroupSpecs:
  - groupName: worker
    replicas: 2
    minReplicas: 2
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.41.0
          resources:
            requests:
              cpu: "1"
              memory: "1Gi"

```

Please refer to documentation [here](https://github.com/NVIDIA/KAI-Scheduler/tree/main/docs/priority)for more information.


## Step 5: Submitting Ray workers with GPU sharing 

This example creates two workers that share a single GPU (0.5 each) within a RayCluster (find the yaml file [here](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/ray-cluster.kai-gpu-sharing.yaml)):

```bash
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-half-gpu
  labels:
    kai.scheduler/queue: team-a
spec:
  headGroupSpec:
    template:
      spec:
        containers:
        - name: head
          image: rayproject/ray:2.46.0
          resources:
            limits:
              cpu: "1"
              memory: "2Gi"

  # ---- Two workers share one GPU (0.5 each) ----
  workerGroupSpecs:
  - groupName: shared-gpu
    replicas: 2
    minReplicas: 2
    template:
      metadata:
        annotations:
          gpu-fraction: "0.5"   
      spec:
        containers:
        - name: worker
          image: rayproject/ray:2.46.0
          resources:
            limits:
              cpu: "1"
              memory: "2Gi"
```

```bash
kubectl apply -f ray-cluster.kai-scheduler.yaml

# Watch the pods get scheduled
kubectl get pods -w
```

### Verify GPU Sharing is Working

To confirm that GPU sharing is working correctly, use these commands:

```bash
# 1. Check GPU fraction annotations and shared GPU groups
kubectl get pods -l ray.io/cluster=raycluster-half-gpu -o custom-columns="NAME:.metadata.name,NODE:.spec.nodeName,GPU-FRACTION:.metadata.annotations.gpu-fraction,GPU-GROUP:.metadata.labels.runai-gpu-group"
```

You should see both worker pods on the same node with `GPU-FRACTION: 0.5` and the same `GPU-GROUP` ID:

NAME                                          NODE               GPU-FRACTION   GPU-GROUP
raycluster-half-gpu-head                      ip-xxx-xx-xx-xxx   <none>         <none>
raycluster-half-gpu-shared-gpu-worker-67tvw   ip-xxx-xx-xx-xxx   0.5            3e456911-a6ea-4b1a-8f55-e90fba89ad76
raycluster-half-gpu-shared-gpu-worker-v5tpp   ip-xxx-xx-xx-xxx   0.5            3e456911-a6ea-4b1a-8f55-e90fba89ad76

This shows that both workers have the same `NVIDIA_VISIBLE_DEVICES` (same physical GPU) and `GPU-FRACTION: 0.50`.

## Troubleshooting

### Check for missing queue labels

If pods remain in `Pending` state, the most common issue is missing queue labels:

Please check operator logs for KAI Scheduler errors and look for error messages like:

```bash
"Queue label missing from RayCluster; pods will remain pending"
```
**Solution**: Ensure your RayCluster has the queue label that exists in the cluster:

```yaml
metadata:
  labels:
    kai.scheduler/queue: default  # Add this label
```
