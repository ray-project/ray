(kuberay-volcano)=
# KubeRay integration with Volcano

[Volcano](https://github.com/volcano-sh/volcano) is a batch scheduling system built on Kubernetes. It provides a suite of mechanisms (gang scheduling, job queues, fair scheduling policies) currently missing from Kubernetes that are commonly required by many classes of batch and elastic workloads. KubeRay's Volcano integration enables more efficient scheduling of Ray pods in multi-tenant Kubernetes environments.

## Setup

### Step 1: Create a Kubernetes cluster with KinD
```shell
kind create cluster
```

### Step 2: Install Volcano

Volcano needs to be successfully installed in your Kubernetes cluster before enabling Volcano integration with KubeRay.
Refer to the [Quick Start Guide](https://github.com/volcano-sh/volcano#quick-start-guide) for Volcano installation instructions.

### Step 3: Install KubeRay Operator with Batch Scheduling

Deploy the KubeRay Operator with the `--enable-batch-scheduler` flag to enable Volcano batch scheduling support.

When installing KubeRay Operator via Helm, you should either set `batchScheduler.enabled` to `true` in your
[`values.yaml`](https://github.com/ray-project/kuberay/blob/753dc05dbed5f6fe61db3a43b34a1b350f26324c/helm-chart/kuberay-operator/values.yaml#L48)
file:
```shell
# values.yaml file
batchScheduler:
    enabled: true
```

**or** pass `--set batchScheduler.enabled=true` flag when running on the command line:
```shell
# Install Helm chart with --enable-batch-scheduler flag set to true 
helm install kuberay-operator kuberay/kuberay-operator --version 1.0.0-rc.0 --set batchScheduler.enabled=true
```

### Step 4: Install a RayCluster with Volcano scheduler

RayCluster custom resource must include label `ray.io/scheduler-name: volcano` to submit the cluster Pods to Volcano for scheduling.

```shell
# Path: kuberay/ray-operator/config/samples
# Includes label `ray.io/scheduler-name: volcano` in the metadata.labels
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.volcano-scheduler.yaml
kubectl apply -f ray-cluster.volcano-scheduler.yaml

# Check RayCluster
kubectl get pod -l ray.io/cluster=test-cluster-0
# NAME                                 READY   STATUS    RESTARTS   AGE
# test-cluster-0-head-jj9bg            1/1     Running   0          36s
```

In addition, the following labels can also be provided in the RayCluster metadata:

- `ray.io/priority-class-name`: the cluster priority class as defined by Kubernetes [here](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#priorityclass).
  - This label only works after the creation of a `PriorityClass` resource
  - ```shell
    labels:
      ray.io/scheduler-name: volcano
      ray.io/priority-class-name: <replace with correct PriorityClass resource name>
    ```
- `volcano.sh/queue-name`: the Volcano [queue](https://volcano.sh/en/docs/queue/) name the cluster will be submitted to.
  - This label only works after the creation of a `Queue` resource
  - ```shell
    labels:
      ray.io/scheduler-name: volcano
      volcano.sh/queue-name: <replace with correct Queue resource name>
    ```

If autoscaling is enabled, `minReplicas` will be used for gang scheduling, otherwise the desired `replicas` will be used.

### Step 5: Use Volcano for batch scheduling

If you need some guidance, check out [examples](https://github.com/volcano-sh/volcano/tree/master/example) available.

## Example

Before going through the example, remove any ray clusters running to ensure successful run through of the example below. 
```shell
kubectl delete raycluster --all
```

### Gang scheduling

In this example, we'll walk through how gang scheduling works with Volcano and KubeRay.

First, let's create a queue with a capacity of 4 CPUs and 6Gi of RAM:

```shell
kubectl create -f - <<EOF
apiVersion: scheduling.volcano.sh/v1beta1
kind: Queue
metadata:
  name: kuberay-test-queue
spec:
  weight: 1
  capability:
    cpu: 4
    memory: 6Gi
EOF
```

The **weight** in the definition above indicates the relative weight of a queue in cluster resource division. This is useful in cases where the total **capability** of all the queues in your cluster exceeds the total available resources, forcing the queues to share among themselves. Queues with higher weight will be allocated a proportionally larger share of the total resources.

The **capability** is a hard constraint on the maximum resources the queue will support at any given time. It can be updated as needed to allow more or fewer workloads to run at a time.

Next we'll create a RayCluster with a head node (1 CPU + 2Gi of RAM) and two workers (1 CPU + 1Gi of RAM each), for a total of 3 CPU and 4Gi of RAM:

```shell
# Path: kuberay/ray-operator/config/samples
# Includes labels `ray.io/scheduler-name: volcano` and `volcano.sh/queue-name: kuberay-test-queue` in the metadata.labels
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.volcano-scheduler-queue.yaml
kubectl apply -f ray-cluster.volcano-scheduler-queue.yaml
```

Because our queue has a capacity of 4 CPU and 6Gi of RAM, this resource should schedule successfully without any issues. We can verify this by checking the status of our cluster's Volcano PodGroup to see that the phase is `Running` and the last status is `Scheduled`:

```shell
kubectl get podgroup ray-test-cluster-0-pg -o yaml

# apiVersion: scheduling.volcano.sh/v1beta1
# kind: PodGroup
# metadata:
#   creationTimestamp: "2022-12-01T04:43:30Z"
#   generation: 2
#   name: ray-test-cluster-0-pg
#   namespace: test
#   ownerReferences:
#   - apiVersion: ray.io/v1alpha1
#     blockOwnerDeletion: true
#     controller: true
#     kind: RayCluster
#     name: test-cluster-0
#     uid: 7979b169-f0b0-42b7-8031-daef522d25cf
#   resourceVersion: "4427347"
#   uid: 78902d3d-b490-47eb-ba12-d6f8b721a579
# spec:
#   minMember: 3
#   minResources:
#     cpu: "3"
#     memory: 4Gi
#   queue: kuberay-test-queue
# status:
#   conditions:
#   - lastTransitionTime: "2022-12-01T04:43:31Z"
#     reason: tasks in gang are ready to be scheduled
#     status: "True"
#     transitionID: f89f3062-ebd7-486b-8763-18ccdba1d585
#     type: Scheduled
#   phase: Running
```

And checking the status of our queue to see that we have 1 running job:

```shell
kubectl get queue kuberay-test-queue -o yaml

# apiVersion: scheduling.volcano.sh/v1beta1
# kind: Queue
# metadata:
#   creationTimestamp: "2022-12-01T04:43:21Z"
#   generation: 1
#   name: kuberay-test-queue
#   resourceVersion: "4427348"
#   uid: a6c4f9df-d58c-4da8-8a58-e01c93eca45a
# spec:
#   capability:
#     cpu: 4
#     memory: 6Gi
#   reclaimable: true
#   weight: 1
# status:
#   reservation: {}
#   running: 1
#   state: Open
```

Next, we'll add an additional RayCluster with the same configuration of head / worker nodes, but a different name:

```shell
# Path: kuberay/ray-operator/config/samples
# Includes labels `ray.io/scheduler-name: volcano` and `volcano.sh/queue-name: kuberay-test-queue` in the metadata.labels
# Replaces the name to test-cluster-1
sed 's/test-cluster-0/test-cluster-1/' ray-cluster.volcano-scheduler-queue.yaml | kubectl apply -f-
```

Now check the status of its PodGroup to see that its phase is `Pending` and the last status is `Unschedulable`:

```shell
kubectl get podgroup ray-test-cluster-1-pg -o yaml

# apiVersion: scheduling.volcano.sh/v1beta1
# kind: PodGroup
# metadata:
#   creationTimestamp: "2022-12-01T04:48:18Z"
#   generation: 2
#   name: ray-test-cluster-1-pg
#   namespace: test
#   ownerReferences:
#   - apiVersion: ray.io/v1alpha1
#     blockOwnerDeletion: true
#     controller: true
#     kind: RayCluster
#     name: test-cluster-1
#     uid: b3cf83dc-ef3a-4bb1-9c42-7d2a39c53358
#   resourceVersion: "4427976"
#   uid: 9087dd08-8f48-4592-a62e-21e9345b0872
# spec:
#   minMember: 3
#   minResources:
#     cpu: "3"
#     memory: 4Gi
#   queue: kuberay-test-queue
# status:
#   conditions:
#   - lastTransitionTime: "2022-12-01T04:48:19Z"
#     message: '3/3 tasks in gang unschedulable: pod group is not ready, 3 Pending,
#       3 minAvailable; Pending: 3 Undetermined'
#     reason: NotEnoughResources
#     status: "True"
#     transitionID: 3956b64f-fc52-4779-831e-d379648eecfc
#     type: Unschedulable
#   phase: Pending
```

Because our new cluster requires more CPU and RAM than our queue will allow, even though we could fit one of the pods with the remaining 1 CPU and 2Gi of RAM, none of the cluster's pods will be placed until there is enough room for all the pods. Without using Volcano for gang scheduling in this way, one of the pods would ordinarily be placed, leading to the cluster being partially allocated, and some jobs (like [Horovod](https://github.com/horovod/horovod) training) getting stuck waiting for resources to become available.

We can see the effect this has on scheduling the pods for our new RayCluster, which are listed as `Pending`:

```shell
kubectl get pods

# NAME                                            READY   STATUS         RESTARTS   AGE
# test-cluster-0-worker-worker-ddfbz              1/1     Running        0          7m
# test-cluster-0-head-vst5j                       1/1     Running        0          7m
# test-cluster-0-worker-worker-57pc7              1/1     Running        0          6m59s
# test-cluster-1-worker-worker-6tzf7              0/1     Pending        0          2m12s
# test-cluster-1-head-6668q                       0/1     Pending        0          2m12s
# test-cluster-1-worker-worker-n5g8k              0/1     Pending        0          2m12s
```

If we dig into the pod details, we'll see that this is indeed because Volcano cannot schedule the gang:

```shell
kubectl describe pod test-cluster-1-head-6668q | tail -n 3

# Type     Reason            Age   From     Message
# ----     ------            ----  ----     -------
# Warning  FailedScheduling  4m5s  volcano  3/3 tasks in gang unschedulable: pod group is not ready, 3 Pending, 3 minAvailable; Pending: 3 Undetermined
```

Let's go ahead and delete the first RayCluster to clear up space in the queue:

```shell
kubectl delete raycluster test-cluster-0
```

The PodGroup for the second cluster has moved to the `Running` state, as there are now enough resources available to schedule the entire set of pods:

```shell
kubectl get podgroup ray-test-cluster-1-pg -o yaml

# apiVersion: scheduling.volcano.sh/v1beta1
# kind: PodGroup
# metadata:
#   creationTimestamp: "2022-12-01T04:48:18Z"
#   generation: 9
#   name: ray-test-cluster-1-pg
#   namespace: test
#   ownerReferences:
#   - apiVersion: ray.io/v1alpha1
#     blockOwnerDeletion: true
#     controller: true
#     kind: RayCluster
#     name: test-cluster-1
#     uid: b3cf83dc-ef3a-4bb1-9c42-7d2a39c53358
#   resourceVersion: "4428864"
#   uid: 9087dd08-8f48-4592-a62e-21e9345b0872
# spec:
#   minMember: 3
#   minResources:
#     cpu: "3"
#     memory: 4Gi
#   queue: kuberay-test-queue
# status:
#   conditions:
#   - lastTransitionTime: "2022-12-01T04:54:04Z"
#     message: '3/3 tasks in gang unschedulable: pod group is not ready, 3 Pending,
#       3 minAvailable; Pending: 3 Undetermined'
#     reason: NotEnoughResources
#     status: "True"
#     transitionID: db90bbf0-6845-441b-8992-d0e85f78db77
#     type: Unschedulable
#   - lastTransitionTime: "2022-12-01T04:55:10Z"
#     reason: tasks in gang are ready to be scheduled
#     status: "True"
#     transitionID: 72bbf1b3-d501-4528-a59d-479504f3eaf5
#     type: Scheduled
#   phase: Running
#   running: 3
```

Checking the pods again, we see that the second cluster is now up and running:

```shell
kubectl get pods

# NAME                                            READY   STATUS         RESTARTS   AGE
# test-cluster-1-worker-worker-n5g8k              1/1     Running        0          9m4s
# test-cluster-1-head-6668q                       1/1     Running        0          9m4s
# test-cluster-1-worker-worker-6tzf7              1/1     Running        0          9m4s
```

Finally, we'll clean up the remaining cluster and queue:

```shell
kubectl delete raycluster test-cluster-1
kubectl delete queue kuberay-test-queue
```
