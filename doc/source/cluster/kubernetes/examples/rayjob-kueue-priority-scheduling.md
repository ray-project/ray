(kuberay-kueue-priority-scheduling-example)=

# Priority Scheduling with RayJob and Kueue

This guide shows how to run [Fine-tune a PyTorch Lightning Text Classifier with Ray Data](https://docs.ray.io/en/master/train/examples/lightning/lightning_cola_advanced.html) example as a RayJob and leverage Kueue to orchestrate priority scheduling and quota management.

## What's Kueue?

[Kueue](https://kueue.sigs.k8s.io/) is a Kubernetes-native system that manages quotas
and how jobs consume them. Kueue decides when:
* To make a job wait
* To admit a job to start, meaning that Kubernetes creates pods.
* To preempt a job, meaning that Kubernetes deletes active pods.

Kueue has native support for some KubeRay APIs. Specifically, you can use Kueue
to manage resources consumed by RayJob and RayCluster. See the
[Kueue documentation](https://kueue.sigs.k8s.io/docs/overview/) to learn more.

## Create a Kubernetes cluster on GKE

If you already have a Kubernetes cluster with GPUs, you can skip this step.

Create a GKE cluster with autoscaling enabled:
```bash
gcloud container clusters create kuberay-gpu-cluster \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-west1-b --machine-type e2-standard-4
```

Create a GPU node pool:
```bash
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-l4,count=1,gpu-driver-version=latest \
  --zone us-west1-b \
  --cluster kuberay-gpu-cluster \
  --num-nodes 1 \
  --min-nodes 1 \
  --max-nodes 10 \
  --enable-autoscaling \
  --machine-type g2-standard-4
```

Configure `kubectl` to connect with your cluster:
```bash
gcloud container clusters get-credentials kuberay-gpu-cluster --zone us-west1-b
``` 

See [Start Google Cloud GKE Cluster with GPUs for KubeRay](kuberay-gke-gpu-cluster-setup) for more details on setting up a Kubernetes cluster on GKE.

## Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
The KubeRay operator Pod must be on the CPU node if you set up the taint for the GPU node pool correctly.

## Install Kueue

```
VERSION=v0.6.0-rc.1
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/$VERSION/manifests.yaml
```

See [Kueue Installation](https://kueue.sigs.k8s.io/docs/installation/#install-a-released-version) for more details on installing Kueue.

## Configure Kueue with priority scheduling

Next, configure Kueue for a single fine-tuning RayJob to run at a time.
Use priority classes to allow RayJobs with higher priority to preempt those with lower priority.

### Create Kueue resources

This manifest creates the following resources:
* [ClusterQueue](https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/): defines quotas and fair sharing rules
* [LocalQueue](https://kueue.sigs.k8s.io/docs/concepts/local_queue/): a namespaced queue (belonging to a tenant) that references a ClusterQueue
* [ResourceFlavor](https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/): defines what resources are available in the cluster (typically from Nodes)
* [WorkloadPriorityClass](https://kueue.sigs.k8s.io/docs/concepts/workload_priority_class/): defines priority for workloads

This example configures Kueue with just enough quotas to run a single fine-tuning RayJob.
There are two priority classes `dev-priority` and `prod-priority`. RayJobs using the
`prod-priority` priority class should take precedence and preempt any running RayJobs from the `dev-priority`
priority class.

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
    reclaimWithinCohort: Any
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

Create the Kueue resources:
```bash
kubectl apply -f kueue-resources.yaml
```

## Deploy a RayJob

Download the RayJob that executes all the steps documented in [Fine-tune a PyTorch Lightning Text Classifier](https://docs.ray.io/en/master/train/examples/lightning/lightning_cola_advanced.html). The [source code](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/pytorch-text-classifier) is also in the KubeRay repository.

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-text-classifier/ray-job.pytorch-distributed-training.yaml
```

Before creating the RayJob, modify the RayJob metadata with:
* A label to assign the RayJob to the LocalQueue created earlier.
* A label to assign the RayJob with the `dev-priority` priority class.
* A modified name to indicate that this job is for development.

```yaml
metadata:
  generateName: dev-pytorch-text-classifier-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: dev-priority
```

Also note the resources required for this RayJob by looking at the resources requested by the nodes:
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
dev-pytorch-text-classifier-r6d4p-4nczg                   1/1     Running   0          4s
torch-text-classifier-r6d4p-raycluster-br45j-head-8bbwt   1/1     Running   0          34s
```

Delete the RayJob after verifying that the job has completed successfully:
```bash
$ kubectl get rayjobs.ray.io dev-pytorch-text-classifier-r6d4p -o jsonpath='{.status.jobStatus}'
SUCCEEDED
$ kubectl get rayjobs.ray.io dev-pytorch-text-classifier-r6d4p -o jsonpath='{.status.jobDeploymentStatus}'
Complete
$ kubectl delete rayjob dev-pytorch-text-classifier-r6d4p
rayjob.ray.io "dev-pytorch-text-classifier-r6d4p" deleted
```

## Queuing multiple RayJob resources

Create 3 RayJobs to see how Kueue interacts with KubeRay to implement job queueing.
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
$ kubectl get clusterqueue  cluster-queue -o yaml
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

## Deploy a RayJob with higher priority

At this point there are multiple RayJob custom resources queued up but only enough quota to run a single RayJob.
Now you can create a new RayJob with higher priority to preempt the already queued RayJob resources.
Modify the RayJob with:
* A label to assign the RayJob to the LocalQueue created earlier.
* A label to assign the RayJob with the `prod-priority` priority class.
* A modified name to indicate that this job is for production.

```yaml
metadata:
  generateName: prod-pytorch-text-classifier-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
    kueue.x-k8s.io/priority-class: prod-priority
```

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

## Increasing cluster quotas

As a final step, you can control the quotas available by modifying the ClusterQueue.
Increasing the quotas signals to Kueue that it can admit more RayJob resources.
If no resources are available to schedule new RayJobs, the Kubernetes node autoscaler,
such as GKE Autopilot, adds additional nodes if enabled. You must enable the autoscaler
to have this behavior.
```yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: "cluster-queue"
spec:
  preemption:
    reclaimWithinCohort: Any
    withinClusterQueue: LowerPriority
  namespaceSelector: {} # Match all namespaces.
  resourceGroups:
  - coveredResources: ["cpu", "memory", "nvidia.com/gpu"]
    flavors:
    - name: "default-flavor"
      resources:
      - name: "cpu"
        nominalQuota: 100
      - name: "memory"
        nominalQuota: 100Gi
      - name: "nvidia.com/gpu"
        nominalQuota: 10  # Add more GPU quota.
```

RayJob resources queued from the previous steps are now admitted by Kueue:
```bash
$ kubectl get pods
NAME                                                      READY   STATUS    RESTARTS   AGE
dev-pytorch-text-classifier-7494p-snvm9                   1/1     Running   0          35s
dev-pytorch-text-classifier-nv9h9-gnqvw                   1/1     Running   0          23s
dev-pytorch-text-classifier-r8xwd-ksscd                   1/1     Running   0          35s
dev-pytorch-text-classifier-vwnwk-qmgxg                   1/1     Running   0          35s
prod-pytorch-text-classifier-gkp9b-r9k5r                  1/1     Running   0          4m53s
torch-text-classifier-4vd7c-raycluster-jwh8p-head-l29kc   1/1     Running   0          66s
torch-text-classifier-7494p-raycluster-c9xcs-head-4jdqm   1/1     Running   0          66s
torch-text-classifier-gkp9b-raycluster-s2f65-head-hfvht   1/1     Running   0          5m23s
torch-text-classifier-nv9h9-raycluster-z6zk4-head-llwkr   1/1     Running   0          36s
torch-text-classifier-r8xwd-raycluster-l45f2-head-bvf6v   1/1     Running   0          66s
torch-text-classifier-vwnwk-raycluster-xxffj-head-gbslc   1/1     Running   0          66s
```
