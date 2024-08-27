(kuberay-kueue-priority-scheduling-example)=

# Priority Scheduling with RayJob and Kueue

This guide shows how to run [Fine-tune a PyTorch Lightning Text Classifier with Ray Data](https://docs.ray.io/en/master/train/examples/lightning/lightning_cola_advanced.html) example as a RayJob and leverage Kueue to orchestrate priority scheduling and quota management.

## What's Kueue?

[Kueue](https://kueue.sigs.k8s.io/) is a Kubernetes-native job queueing system that manages quotas
and how jobs consume them. Kueue decides when:
* To make a job wait
* To admit a job to start, meaning that Kubernetes creates pods.
* To preempt a job, meaning that Kubernetes deletes active pods.

Kueue has native support for some KubeRay APIs. Specifically, you can use Kueue
to manage resources consumed by RayJob and RayCluster. See the
[Kueue documentation](https://kueue.sigs.k8s.io/docs/overview/) to learn more.

## Step 0: Create a Kubernetes cluster on GKE (Optional)

If you already have a Kubernetes cluster with GPUs, you can skip this step.
Otherwise, follow [Start Google Cloud GKE Cluster with GPUs for KubeRay](kuberay-gke-gpu-cluster-setup) to set up a Kubernetes cluster on GKE.

## Step 1: Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
The KubeRay operator Pod must be on the CPU node if you set up the taint for the GPU node pool correctly.

## Step 2: Install Kueue

```bash
VERSION=v0.6.0
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/$VERSION/manifests.yaml
```

See [Kueue Installation](https://kueue.sigs.k8s.io/docs/installation/#install-a-released-version) for more details on installing Kueue.

## Step 3: Configure Kueue with priority scheduling

To understand this tutorial, it's important to understand the following Kueue concepts:
* [ResourceFlavor](https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/)
* [ClusterQueue](https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/)
* [LocalQueue](https://kueue.sigs.k8s.io/docs/concepts/local_queue/)
* [WorkloadPriorityClass](https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/)

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
  - coveredResources: ["cpu", "memory", "nvidia.com/gpu"]
    flavors:
    - name: "default-flavor"
      resources:
      - name: "cpu"
        nominalQuota: 2
      - name: "memory"
        nominalQuota: 8G
      - name: "nvidia.com/gpu" # ClusterQueue only has quota for a single GPU.
        nominalQuota: 1
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
  * The ResourceFlavor `default-flavor` is an empty ResourceFlavor because the compute resources in the Kubernetes cluster are homogeneous. In other words, users can request 1 GPU without considering whether it's an NVIDIA A100 or a T4 GPU.
* **ClusterQueue**
    * The ClusterQueue `cluster-queue` only has 1 ResourceFlavor `default-flavor` with quotas for 2 CPUs, 8G memory, and 1 GPU. It exactly matches the resources requested by 1 RayJob custom resource. ***Hence, only 1 RayJob can run at a time.***
    * The ClusterQueue `cluster-queue` has a preemption policy `withinClusterQueue: LowerPriority`. This policy allows the pending RayJob that doesn’t fit within the nominal quota for its ClusterQueue to preempt active RayJob custom resources in the ClusterQueue that have lower priority.
* **LocalQueue**
  * The LocalQueue `user-queue` is a namespaced object in the `default` namespace which belongs to a ClusterQueue. A typical practice is to assign a namespace to a tenant, team or user, of an organization. Users submit jobs to a LocalQueue, instead of to a ClusterQueue directly.
* **WorkloadPriorityClass**
  * The WorkloadPriorityClass `prod-priority` has a higher value than the WorkloadPriorityClass `dev-priority`. This means that RayJob custom resources with the `prod-priority` priority class take precedence over RayJob custom resources with the `dev-priority` priority class.  

Create the Kueue resources:
```bash
kubectl apply -f kueue-resources.yaml
```

## Step 4: Deploy a RayJob

Download the RayJob that executes all the steps documented in [Fine-tune a PyTorch Lightning Text Classifier](https://docs.ray.io/en/master/train/examples/lightning/lightning_cola_advanced.html). The [source code](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/pytorch-text-classifier) is also in the KubeRay repository.

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-text-classifier/ray-job.pytorch-distributed-training.yaml
```

Before creating the RayJob, modify the RayJob metadata with:

```yaml
metadata:
  generateName: dev-pytorch-text-classifier-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: dev-priority
```

* `kueue.x-k8s.io/queue-name: user-queue`: As the previous step mentioned, users submit jobs to a LocalQueue instead of directly to a ClusterQueue.
* `kueue.x-k8s.io/priority-class: dev-priority`: Assign the RayJob with the `dev-priority` WorkloadPriorityClass.
* A modified name to indicate that this job is for development.

Also note the resources required for this RayJob by looking at the resources that the Ray head Pod requests:
```yaml
resources:
  limits:
    memory: "8G"
    nvidia.com/gpu: "1"
  requests:
    cpu: "2"
    memory: "8G"
    nvidia.com/gpu: "1"
```

Now deploy the RayJob:
```bash
$ kubectl create -f ray-job.pytorch-distributed-training.yaml
rayjob.ray.io/dev-pytorch-text-classifier-r6d4p created
```

Verify that the RayCluster and the submitter Kubernetes Job are running:
```bash
$ kubectl get pod
NAME                                                      READY   STATUS    RESTARTS   AGE
dev-pytorch-text-classifier-r6d4p-4nczg                   1/1     Running   0          4s  # Submitter Kubernetes Job
torch-text-classifier-r6d4p-raycluster-br45j-head-8bbwt   1/1     Running   0          34s # Ray head Pod
```

Delete the RayJob after verifying that the job has completed successfully.
```bash
$ kubectl get rayjobs.ray.io dev-pytorch-text-classifier-r6d4p -o jsonpath='{.status.jobStatus}'
SUCCEEDED
$ kubectl get rayjobs.ray.io dev-pytorch-text-classifier-r6d4p -o jsonpath='{.status.jobDeploymentStatus}'
Complete
$ kubectl delete rayjob dev-pytorch-text-classifier-r6d4p
rayjob.ray.io "dev-pytorch-text-classifier-r6d4p" deleted
```

## Step 5: Queuing multiple RayJob resources

Create 3 RayJob custom resources to see how Kueue interacts with KubeRay to implement job queueing.

```bash
$ kubectl create -f ray-job.pytorch-distributed-training.yaml
rayjob.ray.io/dev-pytorch-text-classifier-8vg2c created
$ kubectl create -f ray-job.pytorch-distributed-training.yaml
rayjob.ray.io/dev-pytorch-text-classifier-n5k89 created
$ kubectl create -f ray-job.pytorch-distributed-training.yaml
rayjob.ray.io/dev-pytorch-text-classifier-ftcs9 created
```

Because each RayJob requests 1 GPU and the ClusterQueue has quotas for only 1 GPU,
Kueue automatically suspends new RayJob resources until GPU quotas become available.

You can also inspect the `ClusterQueue` to see available and used quotas:
```bash
$ kubectl get clusterqueue
NAME            COHORT   PENDING WORKLOADS
cluster-queue            2
$ kubectl get clusterqueue cluster-queue -o yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
...
...
...
status:
  admittedWorkloads: 1  # Workloads admitted by queue.
  flavorsReservation:
  - name: default-flavor
    resources:
    - borrowed: "0"
      name: cpu
      total: "8"
    - borrowed: "0"
      name: memory
      total: 19531250Ki
    - borrowed: "0"
      name: nvidia.com/gpu
      total: "2"
  flavorsUsage:
  - name: default-flavor
    resources:
    - borrowed: "0"
      name: cpu
      total: "8"
    - borrowed: "0"
      name: memory
      total: 19531250Ki
    - borrowed: "0"
      name: nvidia.com/gpu
      total: "2"
  pendingWorkloads: 2   # Queued workloads waiting for quotas.
  reservingWorkloads: 1 # Running workloads that are using quotas.
```

## Step 6: Deploy a RayJob with higher priority

At this point there are multiple RayJob custom resources queued up but only enough quota to run a single RayJob.
Now you can create a new RayJob with higher priority to preempt the already queued RayJob resources.
Modify the RayJob with:

```yaml
metadata:
  generateName: prod-pytorch-text-classifier-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: prod-priority
```

* `kueue.x-k8s.io/queue-name: user-queue`: As the previous step mentioned, users submit jobs to a LocalQueue instead of directly to a ClusterQueue.
* `kueue.x-k8s.io/priority-class: dev-priority`: Assign the RayJob with the `prod-priority` WorkloadPriorityClass.
* A modified name to indicate that this job is for production.

Create the new RayJob:
```sh
$ kubectl create -f ray-job.pytorch-distributed-training.yaml
rayjob.ray.io/prod-pytorch-text-classifier-gkp9b created
```

Note that higher priority jobs preempt lower priority jobs when there aren't enough quotas for both:
```bash
$ kubectl get pods
NAME                                                      READY   STATUS    RESTARTS   AGE
prod-pytorch-text-classifier-gkp9b-r9k5r                  1/1     Running   0          5s
torch-text-classifier-gkp9b-raycluster-s2f65-head-hfvht   1/1     Running   0          35s
```
