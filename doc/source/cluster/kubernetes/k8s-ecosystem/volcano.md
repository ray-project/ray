(kuberay-volcano)=
# KubeRay integration with Volcano

[Volcano](https://github.com/volcano-sh/volcano) is a batch scheduling system built on Kubernetes. It provides a suite of mechanisms (gang scheduling, job queues, fair scheduling policies) currently missing from Kubernetes that are commonly required by many classes of batch and elastic workloads. KubeRay's Volcano integration enables more efficient scheduling of Ray pods in multi-tenant Kubernetes environments.

## Setup

### Step 1: Create a Kubernetes cluster with KinD
Run the following command in a terminal:

```shell
kind create cluster
```

### Step 2: Install Volcano

You need to successfully install Volcano on your Kubernetes cluster before enabling Volcano integration with KubeRay.
See [Quick Start Guide](https://github.com/volcano-sh/volcano#quick-start-guide) for Volcano installation instructions.

### Step 3: Install the KubeRay Operator with batch scheduling

Deploy the KubeRay Operator with the `--batch-scheduler=volcano` flag to enable Volcano batch scheduling support.

When installing KubeRay Operator using Helm, you should use one of these two options:

* Set `batchScheduler.name` to `volcano` in your
[`values.yaml`](https://github.com/ray-project/kuberay/blob/753dc05dbed5f6fe61db3a43b34a1b350f26324c/helm-chart/kuberay-operator/values.yaml#L48)
file:
```shell
# values.yaml file
batchScheduler:
    name: volcano
```

* Pass the `--set batchScheduler.name=volcano` flag when running on the command line:
```shell
# Install the Helm chart with the --batch-scheduler=volcano flag
helm install kuberay-operator kuberay/kuberay-operator --version 1.5.0 --set batchScheduler.name=volcano
```

### Step 4: Install a RayCluster with the Volcano scheduler

The RayCluster custom resource must include the `ray.io/scheduler-name: volcano` label to submit the cluster Pods to Volcano for scheduling.

```shell
# Path: kuberay/ray-operator/config/samples
# Includes label `ray.io/scheduler-name: volcano` in the metadata.labels
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.volcano-scheduler.yaml
kubectl apply -f ray-cluster.volcano-scheduler.yaml

# Check the RayCluster
kubectl get pod -l ray.io/cluster=test-cluster-0
# NAME                                 READY   STATUS    RESTARTS   AGE
# test-cluster-0-head-jj9bg            1/1     Running   0          36s
```

You can also be provide the following labels in the RayCluster metadata:

- `ray.io/priority-class-name`: The cluster priority class as defined by [Kubernetes](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#priorityclass)
  - This label only works after you create a `PriorityClass` resource
  - ```shell
    labels:
      ray.io/scheduler-name: volcano
      ray.io/priority-class-name: <replace with correct PriorityClass resource name>
    ```
- `volcano.sh/queue-name`: The Volcano [queue](https://volcano.sh/en/docs/queue/) name the cluster submits to.
  - This label only works after you create a `Queue` resource
  - ```shell
    labels:
      ray.io/scheduler-name: volcano
      volcano.sh/queue-name: <replace with correct Queue resource name>
    ```

If autoscaling is enabled, `minReplicas` is used for gang scheduling, otherwise the desired `replicas` is used.

### Step 5: Use Volcano for batch scheduling

For guidance, see [examples](https://github.com/volcano-sh/volcano/tree/master/example).

## Example

Before going through the example, remove any running Ray Clusters to ensure a successful run through of the example below.
```shell
kubectl delete raycluster --all
```

### Gang scheduling

This example walks through how gang scheduling works with Volcano and KubeRay.

First, create a queue with a capacity of 4 CPUs and 6Gi of RAM:

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

The **weight** in the definition above indicates the relative weight of a queue in a cluster resource division. Use this parameter in cases where the total **capability** of all the queues in your cluster exceeds the total available resources, forcing the queues to share among themselves. Queues with higher weight are allocated a proportionally larger share of the total resources.

The **capability** is a hard constraint on the maximum resources the queue supports at any given time. You can update it as needed to allow more or fewer workloads to run at a time.

Next, create a RayCluster with a head node (1 CPU + 2Gi of RAM) and two workers (1 CPU + 1Gi of RAM each), for a total of 3 CPU and 4Gi of RAM:

```shell
# Path: kuberay/ray-operator/config/samples
# Includes  the `ray.io/scheduler-name: volcano` and `volcano.sh/queue-name: kuberay-test-queue` labels in the metadata.labels
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.volcano-scheduler-queue.yaml
kubectl apply -f ray-cluster.volcano-scheduler-queue.yaml
```

Because the queue has a capacity of 4 CPU and 6Gi of RAM, this resource should schedule successfully without any issues. You can verify this by checking the status of the cluster's Volcano PodGroup to see that the phase is `Running` and the last status is `Scheduled`:

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
#     reason: tasks in the gang are ready to be scheduled
#     status: "True"
#     transitionID: f89f3062-ebd7-486b-8763-18ccdba1d585
#     type: Scheduled
#   phase: Running
```

Check the status of the queue to see allocated resources:

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
#   allocated:
#     cpu: "3"
#     memory: 4Gi 
#     pods: "3"
#   reservation: {}
#   state: Open
```

Next, add an additional RayCluster with the same configuration of head and worker nodes, but with a different name:

```shell
# Path: kuberay/ray-operator/config/samples
# Includes the `ray.io/scheduler-name: volcano` and `volcano.sh/queue-name: kuberay-test-queue` labels in the metadata.labels
# Replaces the name to test-cluster-1
sed 's/test-cluster-0/test-cluster-1/' ray-cluster.volcano-scheduler-queue.yaml | kubectl apply -f-
```

Check the status of its PodGroup to see that its phase is `Pending` and the last status is `Unschedulable`:

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

Because the new cluster requires more CPU and RAM than our queue allows, even though one of the pods would fit in the remaining 1 CPU and 2Gi of RAM, none of the cluster's pods are placed until there is enough room for all the pods. Without using Volcano for gang scheduling in this way, one of the pods would ordinarily be placed, leading to the cluster being partially allocated, and some jobs (like [Horovod](https://github.com/horovod/horovod) training) being stuck waiting for resources to become available.

See the effect this has on scheduling the pods for our new RayCluster, which are listed as `Pending`:

```shell
kubectl get pods

# NAME                                            READY   STATUS         RESTARTS   AGE
# test-cluster-0-worker-worker-ddfbz              1/1     Running        0          7m
# test-cluster-0-head                             1/1     Running        0          7m
# test-cluster-0-worker-worker-57pc7              1/1     Running        0          6m59s
# test-cluster-1-worker-worker-6tzf7              0/1     Pending        0          2m12s
# test-cluster-1-head                             0/1     Pending        0          2m12s
# test-cluster-1-worker-worker-n5g8k              0/1     Pending        0          2m12s
```

Look at the pod details to see that Volcano cannot schedule the gang:

```shell
kubectl describe pod test-cluster-1-head-6668q | tail -n 3

# Type     Reason            Age   From     Message
# ----     ------            ----  ----     -------
# Warning  FailedScheduling  4m5s  volcano  3/3 tasks in gang unschedulable: pod group is not ready, 3 Pending, 3 minAvailable; Pending: 3 Undetermined
```

Delete the first RayCluster to make space in the queue:

```shell
kubectl delete raycluster test-cluster-0
```

The PodGroup for the second cluster changed to the `Running` state, because enough resources are now available to schedule the entire set of pods:

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
#     reason: tasks in the gang are ready to be scheduled
#     status: "True"
#     transitionID: 72bbf1b3-d501-4528-a59d-479504f3eaf5
#     type: Scheduled
#   phase: Running
#   running: 3
```

Check the pods again to see that the second cluster is now up and running:

```shell
kubectl get pods

# NAME                                            READY   STATUS         RESTARTS   AGE
# test-cluster-1-worker-worker-n5g8k              1/1     Running        0          9m4s
# test-cluster-1-head                             1/1     Running        0          9m4s
# test-cluster-1-worker-worker-6tzf7              1/1     Running        0          9m4s
```

Finally, clean up the remaining cluster and queue:

```shell
kubectl delete raycluster test-cluster-1
kubectl delete queue kuberay-test-queue
```

### Use Volcano for RayJob gang scheduling

Starting with KubeRay 1.5.0, KubeRay supports gang scheduling for RayJob custom resources.

First, create a queue with a capacity of 4 CPUs and 6Gi of RAM and RayJob a with a head node (1 CPU + 2Gi of RAM), two workers (1 CPU + 1Gi of RAM each) and a submitter pod (0.5 CPU + 200Mi of RAM), for a total of 3500m CPU and 4296Mi of RAM

```shell
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-job.volcano-scheduler-queue.yaml
kubectl apply -f ray-job.volcano-scheduler-queue.yaml
```

Wait until all pods are in the Running state.
```shell
kubectl get pod

# NAME                                             READY   STATUS      RESTARTS   AGE
# rayjob-sample-0-k449j-head-rlgxj                 1/1     Running     0          93s
# rayjob-sample-0-k449j-small-group-worker-c6dt8   1/1     Running     0          93s
# rayjob-sample-0-k449j-small-group-worker-cq6xn   1/1     Running     0          93s
# rayjob-sample-0-qmm8s                            0/1     Completed   0          32s
```

Add an additional RayJob with the same configuration but with a different name
```shell
sed 's/rayjob-sample-0/rayjob-sample-1/' ray-job.volcano-scheduler-queue.yaml | kubectl apply -f-
```

All the pods stuck on pending for new RayJob
```shell
# NAME                                             READY   STATUS      RESTARTS   AGE
# rayjob-sample-0-k449j-head-rlgxj                 1/1     Running     0          3m27s
# rayjob-sample-0-k449j-small-group-worker-c6dt8   1/1     Running     0          3m27s
# rayjob-sample-0-k449j-small-group-worker-cq6xn   1/1     Running     0          3m27s
# rayjob-sample-0-qmm8s                            0/1     Completed   0          2m26s
# rayjob-sample-1-mvgqf-head-qb7wm                 0/1     Pending     0          21s
# rayjob-sample-1-mvgqf-small-group-worker-jfzt5   0/1     Pending     0          21s
# rayjob-sample-1-mvgqf-small-group-worker-ng765   0/1     Pending     0          21s
```

Check the status of its PodGroup to see that its phase is `Pending` and the last status is `Unschedulable`:
```shell
kubectl get podgroup ray-rayjob-sample-1-pg  -o yaml

# apiVersion: scheduling.volcano.sh/v1beta1
# kind: PodGroup
# metadata:
#   creationTimestamp: "2025-10-30T17:10:18Z"
#   generation: 2
#   name: ray-rayjob-sample-1-pg
#   namespace: default
#   ownerReferences:
#   - apiVersion: ray.io/v1
#     blockOwnerDeletion: true
#     controller: true
#     kind: RayJob
#     name: rayjob-sample-1
#     uid: 5835c896-c75d-4692-b10a-2871a79f141a
#   resourceVersion: "3226"
#   uid: 9fd55cbd-ba69-456d-b305-f61ffd6d935d
# spec:
#   minMember: 3
#   minResources:
#     cpu: 3500m
#     memory: 4296Mi
#   queue: kuberay-test-queue
# status:
#   conditions:
#   - lastTransitionTime: "2025-10-30T17:10:18Z"
#     message: '3/3 tasks in gang unschedulable: pod group is not ready, 3 Pending,
#       3 minAvailable; Pending: 3 Unschedulable'
#     reason: NotEnoughResources
#     status: "True"
#     transitionID: 7866f533-6590-4a4d-83cf-8f1db0214609
#     type: Unschedulable
#   phase: Pending
```

Delete the first RayJob to make space in the queue.
```shell
kubectl delete rayjob rayjob-sample-0
```

The PodGroup for the second cluster changed to the Running state, because enough resources are now available to schedule the entire set of pods.
```shell
kubectl get podgroup ray-rayjob-sample-1-pg  -o yaml

# apiVersion: scheduling.volcano.sh/v1beta1
# kind: PodGroup
# metadata:
#   creationTimestamp: "2025-10-30T17:10:18Z"
#   generation: 7
#   name: ray-rayjob-sample-1-pg
#   namespace: default
#   ownerReferences:
#   - apiVersion: ray.io/v1
#     blockOwnerDeletion: true
#     controller: true
#     kind: RayJob
#     name: rayjob-sample-1
#     uid: 5835c896-c75d-4692-b10a-2871a79f141a
#   resourceVersion: "3724"
#   uid: 9fd55cbd-ba69-456d-b305-f61ffd6d935d
# spec:
#   minMember: 3
#   minResources:
#     cpu: 3500m
#     memory: 4296Mi
#   queue: kuberay-test-queue
# status:
#   conditions:
#   - lastTransitionTime: "2025-10-30T17:10:18Z"
#     message: '3/3 tasks in gang unschedulable: pod group is not ready, 3 Pending,
#       3 minAvailable; Pending: 3 Unschedulable'
#     reason: NotEnoughResources
#     status: "True"
#     transitionID: 7866f533-6590-4a4d-83cf-8f1db0214609
#     type: Unschedulable
#   - lastTransitionTime: "2025-10-30T17:14:44Z"
#     reason: tasks in gang are ready to be scheduled
#     status: "True"
#     transitionID: 36e0222d-eee3-444a-9889-5b9c255f41af
#     type: Scheduled
#   phase: Running
#   running: 4
```

Check the pods again to see that the second RayJob is now up and running:
```shell
kubectl get pod
# NAME                                             READY   STATUS      RESTARTS   AGE
# rayjob-sample-1-mvgqf-head-qb7wm                 1/1     Running     0          5m47s
# rayjob-sample-1-mvgqf-small-group-worker-jfzt5   1/1     Running     0          5m47s
# rayjob-sample-1-mvgqf-small-group-worker-ng765   1/1     Running     0          5m47s
# rayjob-sample-1-tcd4m                            0/1     Completed   0          84s
```

Finally, clean up the remaining rayjob, queue and configmap:
```
kubectl delete rayjob rayjob-sample-1
kubectl delete queue kuberay-test-queue
kubectl delete configmap ray-job-code-sample
```
