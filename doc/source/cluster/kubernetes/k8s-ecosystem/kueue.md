(kuberay-kueue)=
# Gang scheduling and priority scheduling for RayJob with Kueue

This guide demonstrates the integration of KubeRay and Kueue for gang and priority scheduling using RayJob on a local Kind cluster.
Refer to [Priority Scheduling with RayJob and Kueue](kuberay-kueue-priority-scheduling-example) and [Gang Scheduling with RayJob and Kueue](kuberay-kueue-gang-scheduling-example) for real-world use cases.

## Kueue

[Kueue](https://kueue.sigs.k8s.io/) is a Kubernetes-native job queueing system that manages quotas and how jobs consume them. Kueue decides when:
* To make a job wait.
* To admit a job to start, which triggers Kubernetes to create pods.
* To preempt a job, which triggers Kubernetes to delete active pods.

Kueue has native support for some KubeRay APIs. Specifically, you can use Kueue to manage resources consumed by RayJob and RayCluster. 
See the [Kueue documentation](https://kueue.sigs.k8s.io/docs/overview/) to learn more.

## Step 0: Create a Kind cluster

```bash
kind create cluster
```

## Step 1: Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Step 2: Install Kueue

```bash
VERSION=v0.6.0
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/$VERSION/manifests.yaml
```

See [Kueue Installation](https://kueue.sigs.k8s.io/docs/installation/#install-a-released-version) for more details on installing Kueue.
Some limitations exist between Kueue and RayJob. See the [limitations of Kueue](https://kueue.sigs.k8s.io/docs/tasks/run_rayjobs/#c-limitations) for more details.

## Step 3: Create Kueue resources

```yaml
# kueue-resources.yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: "default-flavor"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: "cluster-queue"
spec:
  preemption:
    withinClusterQueue: LowerPriority
  namespaceSelector: {} # Match all namespaces.
  resourceGroups:
  - coveredResources: ["cpu", "memory"]
    flavors:
    - name: "default-flavor"
      resources:
      - name: "cpu"
        nominalQuota: 3
      - name: "memory"
        nominalQuota: 6G
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  namespace: "default"
  name: "user-queue"
spec:
  clusterQueue: "cluster-queue"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: prod-priority
value: 1000
description: "Priority class for prod jobs"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: WorkloadPriorityClass
metadata:
  name: dev-priority
value: 100
description: "Priority class for development jobs"
```

The YAML manifest configures:

* **ResourceFlavor**
  * The ResourceFlavor `default-flavor` is an empty ResourceFlavor because the compute resources in the Kubernetes cluster are homogeneous. In other words, users can request 1 CPU without considering whether it's an ARM chip or an x86 chip.
* **ClusterQueue**
    * The ClusterQueue `cluster-queue` only has 1 ResourceFlavor `default-flavor` with quotas for 3 CPUs and 6G memory.
    * The ClusterQueue `cluster-queue` has a preemption policy `withinClusterQueue: LowerPriority`. This policy allows the pending RayJob that doesn’t fit within the nominal quota for its ClusterQueue to preempt active RayJob custom resources in the ClusterQueue that have lower priority.
* **LocalQueue**
  * The LocalQueue `user-queue` is a namespaced object in the `default` namespace which belongs to a ClusterQueue. A typical practice is to assign a namespace to a tenant, team, or user of an organization. Users submit jobs to a LocalQueue, instead of to a ClusterQueue directly.
* **WorkloadPriorityClass**
  * The WorkloadPriorityClass `prod-priority` has a higher value than the WorkloadPriorityClass `dev-priority`. RayJob custom resources with the `prod-priority` priority class take precedence over RayJob custom resources with the `dev-priority` priority class.  

Create the Kueue resources:
```bash
kubectl apply -f kueue-resources.yaml
```

## Step 4: Gang scheduling with Kueue

Kueue always admits workloads in “gang” mode.
Kueue admits workloads on an “all or nothing” basis, ensuring that Kubernetes never partially provisions a RayJob or RayCluster.
Use gang scheduling strategy to avoid wasting compute resources caused by inefficient scheduling of workloads.

Download the RayJob YAML manifest from the KubeRay repository.

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-job.kueue-toy-sample.yaml
```

Before creating the RayJob, modify the RayJob metadata with the following:

```yaml
metadata:
  generateName: rayjob-sample-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: dev-priority
```

Create two RayJob custom resources with the same priority `dev-priority`.
Note these important points for RayJob custom resources:
* The RayJob custom resource includes 1 head Pod and 1 worker Pod, with each Pod requesting 1 CPU and 2G of memory.
* The RayJob runs a simple Python script that demonstrates a loop running 600 iterations, printing the iteration number and sleeping for 1 second per iteration. Hence, the RayJob runs for about 600 seconds after the submitted Kubernetes Job starts.
* Set `shutdownAfterJobFinishes` to true for RayJob to enable automatic cleanup. This setting triggers KubeRay to delete the RayCluster after the RayJob finishes.
  * Kueue doesn't handle the RayJob custom resource with the `shutdownAfterJobFinishes` set to false. See the [limitations of Kueue](https://kueue.sigs.k8s.io/docs/tasks/run_rayjobs/#c-limitations) for more details.

```yaml
kubectl create -f ray-job.kueue-toy-sample.yaml
kubectl create -f ray-job.kueue-toy-sample.yaml
```

Each RayJob custom resource requests 2 CPUs and 4G of memory in total.
However, the ClusterQueue only has 3 CPUs and 6G of memory in total.
Therefore, the second RayJob custom resource remains pending, and KubeRay doesn't create Pods from the pending RayJob, even though the remaining resources are sufficient for a Pod.
You can also inspect the `ClusterQueue` to see available and used quotas:

```bash
$ kubectl get clusterqueues.kueue.x-k8s.io
NAME            COHORT   PENDING WORKLOADS
cluster-queue            1

$ kubectl get clusterqueues.kueue.x-k8s.io cluster-queue -o yaml
Status:
  Admitted Workloads:  1 # Workloads admitted by queue.
  Conditions:
    Last Transition Time:  2024-02-28T22:41:28Z
    Message:               Can admit new workloads
    Reason:                Ready
    Status:                True
    Type:                  Active
  Flavors Reservation:
    Name:  default-flavor
    Resources:
      Borrowed:  0
      Name:      cpu
      Total:     2
      Borrowed:  0
      Name:      memory
      Total:     4Gi
  Flavors Usage:
    Name:  default-flavor
    Resources:
      Borrowed:         0
      Name:             cpu
      Total:            2
      Borrowed:         0
      Name:             memory
      Total:            4Gi
  Pending Workloads:    1
  Reserving Workloads:  1
```

Kueue admits the pending RayJob custom resource when the first RayJob custom resource finishes.
Check the status of the RayJob custom resources and delete them after they finish:

```bash
$ kubectl get rayjobs.ray.io
NAME                  JOB STATUS   DEPLOYMENT STATUS   START TIME             END TIME               AGE
rayjob-sample-ckvq4   SUCCEEDED    Complete            xxxxx                  xxxxx                  xxx
rayjob-sample-p5msp   SUCCEEDED    Complete            xxxxx                  xxxxx                  xxx

$ kubectl delete rayjob rayjob-sample-ckvq4
$ kubectl delete rayjob rayjob-sample-p5msp
```

## Step 5: Priority scheduling with Kueue

This step creates a RayJob with a lower priority class `dev-priority` first and a RayJob with a higher priority class `prod-priority` later.
The RayJob with higher priority class `prod-priority` takes precedence over the RayJob with lower priority class `dev-priority`.
Kueue preempts the RayJob with a lower priority to admit the RayJob with a higher priority.

If you followed the previous step, the RayJob YAML manifest `ray-job.kueue-toy-sample.yaml` should already be set to the `dev-priority` priority class.
Create a RayJob with the lower priority class `dev-priority`:

```bash
kubectl create -f ray-job.kueue-toy-sample.yaml
```

Before creating the RayJob with the higher priority class `prod-priority`, modify the RayJob metadata with the following:

```yaml
metadata:
  generateName: rayjob-sample-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: prod-priority
```

Create a RayJob with the higher priority class `prod-priority`:

```bash
kubectl create -f ray-job.kueue-toy-sample.yaml
```

You can see that KubeRay operator deletes the Pods belonging to the RayJob with the lower priority class `dev-priority` and creates the Pods belonging to the RayJob with the higher priority class `prod-priority`.
