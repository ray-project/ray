(kuberay-kueue-gang-scheduling-example)=

# Gang Scheduling with RayJob and Kueue

This guide demonstrates how to use Kueue for gang scheduling RayJob resources, taking advantage of dynamic resource provisioning and queueing on Kubernetes.
 To illustrate the concepts, this guide uses the [Fine-tune a PyTorch Lightning Text Classifier with Ray Data](https://docs.ray.io/en/master/train/examples/lightning/lightning_cola_advanced.html) example.

## Gang scheduling

Gang scheduling in Kubernetes ensures that a group of related Pods, such as those in a Ray cluster,
only start when all required resources are available. Having this requirement is crucial when working with expensive,
limited resources like GPUs.

## Kueue

[Kueue](https://kueue.sigs.k8s.io/) is a Kubernetes-native system that manages quotas
and how jobs consume them. Kueue decides when:
* To make a job wait.
* To admit a job to start, which triggers Kubernetes to create Pods.
* To preempt a job, which triggers Kubernetes to delete active Pods.

Kueue has native support for some KubeRay APIs. Specifically, you can use Kueue
to manage resources that RayJob and RayCluster consume. See the
[Kueue documentation](https://kueue.sigs.k8s.io/docs/overview/) to learn more.

## Why use gang scheduling

Gang scheduling is essential when working with expensive, limited hardware accelerators like GPUs.
It prevents RayJobs from partially provisioning Ray clusters and claiming but not using the GPUs.
Kueue suspends a RayJob until the Kubernetes cluster and the underlying cloud provider can guarantee
the capacity that the RayJob needs to execute. This approach greatly improves GPU utilization and
cost, especially when GPU availability is limited.

## Create a Kubernetes cluster on GKE

Create a GKE cluster with the `enable-autoscaling` option:
```bash
gcloud container clusters create kuberay-gpu-cluster \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-east4-c --machine-type e2-standard-4
```

Create a GPU node pool with the `enable-queued-provisioning` option enabled:
```bash
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-l4,count=1,gpu-driver-version=latest \
  --enable-queued-provisioning \
  --reservation-affinity=none  \
  --zone us-east4-c \
  --cluster kuberay-gpu-cluster \
  --num-nodes 0 \
  --min-nodes 0 \
  --max-nodes 10 \
  --enable-autoscaling \
  --machine-type g2-standard-4
```

This command creates a node pool, which initially has zero nodes.
The `--enable-queued-provisioning` flag enables "queued provisioning" in the Kubernetes node autoscaler using the ProvisioningRequest API. More details are below.
You need to use the `--reservation-affinity=none` flag because GKE doesn't support Node Reservations with ProvisioningRequest.


## Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.
The KubeRay operator Pod must be on the CPU node if you set up the taint for the GPU node pool correctly.

## Install Kueue

Install the latest released version of Kueue.
```
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.8.2/manifests.yaml
```

See [Kueue Installation](https://kueue.sigs.k8s.io/docs/installation/#install-a-released-version) for more details on installing Kueue.

## Configure Kueue for gang scheduling

Next, configure Kueue for gang scheduling. Kueue leverages the ProvisioningRequest API for two key tasks:
1. Dynamically adding new nodes to the cluster when a job needs more resources.
2. Blocking the admission of new jobs that are waiting for sufficient resources to become available.

See [How ProvisioningRequest works](https://cloud.google.com/kubernetes-engine/docs/how-to/provisioningrequest#how-provisioningrequest-works) for more details.

### Create Kueue resources

This manifest creates the following resources:
* [ClusterQueue](https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/): Defines quotas and fair sharing rules
* [LocalQueue](https://kueue.sigs.k8s.io/docs/concepts/local_queue/): A namespaced queue, belonging to a tenant, that references a ClusterQueue
* [ResourceFlavor](https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/): Defines what resources are available in the cluster, typically from Nodes
* [AdmissionCheck](https://kueue.sigs.k8s.io/docs/concepts/admission_check/): A mechanism allowing components to influence the timing of a workload admission

```yaml
# kueue-resources.yaml
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: "default-flavor"
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: AdmissionCheck
metadata:
  name: rayjob-gpu
spec:
  controllerName: kueue.x-k8s.io/provisioning-request
  parameters:
    apiGroup: kueue.x-k8s.io
    kind: ProvisioningRequestConfig
    name: rayjob-gpu-config
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ProvisioningRequestConfig
metadata:
  name: rayjob-gpu-config
spec:
  provisioningClassName: queued-provisioning.gke.io
  managedResources:
  - nvidia.com/gpu
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: "cluster-queue"
spec:
  namespaceSelector: {} # match all
  resourceGroups:
  - coveredResources: ["cpu", "memory", "nvidia.com/gpu"]
    flavors:
    - name: "default-flavor"
      resources:
      - name: "cpu"
        nominalQuota: 10000 # infinite quotas
      - name: "memory"
        nominalQuota: 10000Gi # infinite quotas
      - name: "nvidia.com/gpu"
        nominalQuota: 10000 # infinite quotas
  admissionChecks:
  - rayjob-gpu
---
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  namespace: "default"
  name: "user-queue"
spec:
  clusterQueue: "cluster-queue"
```

Create the Kueue resources:
```bash
kubectl apply -f kueue-resources.yaml
```

:::{note}
This example configures Kueue to orchestrate the gang scheduling of GPUs. However, you can use other resources such as CPU and memory.
:::

## Deploy a RayJob

Download the RayJob that executes all the steps documented in [Fine-tune a PyTorch Lightning Text Classifier](https://docs.ray.io/en/master/train/examples/lightning/lightning_cola_advanced.html). The [source code](https://github.com/ray-project/kuberay/tree/master/ray-operator/config/samples/pytorch-text-classifier) is also in the KubeRay repository.

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/pytorch-text-classifier/ray-job.pytorch-distributed-training.yaml
```

Before creating the RayJob, modify the RayJob metadata with a label
to assign the RayJob to the LocalQueue that you created earlier:
```yaml
metadata:
  generateName: pytorch-text-classifier-
  labels:
    kueue.x-k8s.io/queue-name: user-queue
```

Deploy the RayJob:
```bash
$ kubectl create -f ray-job.pytorch-distributed-training.yaml
rayjob.ray.io/dev-pytorch-text-classifier-r6d4p created
```

## Gang scheduling with RayJob

Following is the expected behavior when you deploy a GPU-requiring RayJob to a cluster that initially lacks GPUs:
* Kueue suspends the RayJob due to insufficient GPU resources in the cluster.
* Kueue creates a ProvisioningRequest, specifying the GPU requirements for the RayJob.
* The Kubernetes node autoscaler monitors ProvisioningRequests and adds nodes with GPUs as needed.
* Once the required GPU nodes are available, the ProvisioningRequest is satisfied.
* Kueue admits the RayJob, allowing Kubernetes to schedule the Ray nodes on the newly provisioned nodes, and the RayJob execution begins.

If GPUs are unavailable, Kueue keeps suspending the RayJob. In addition, the node autoscaler avoids
provisioning new nodes until it can fully satisfy the RayJob's GPU requirements.

Upon creating a RayJob, notice that the RayJob status is immediately `suspended` despite the ClusterQueue having GPU quotas available.
```bash
$ kubectl get rayjob pytorch-text-classifier-rj4sg -o yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: pytorch-text-classifier-rj4sg
  labels:
    kueue.x-k8s.io/queue-name: user-queue
...
...
...
status:
  jobDeploymentStatus: Suspended  # RayJob suspended
  jobId: pytorch-text-classifier-rj4sg-pj9hx
  jobStatus: PENDING
```

Kueue keeps suspending this RayJob until its corresponding ProvisioningRequest is satisfied.
List ProvisioningRequest resources and their status with this command:
```bash
$ kubectl get provisioningrequest
NAME                                                      ACCEPTED   PROVISIONED   FAILED   AGE
rayjob-pytorch-text-classifier-nv77q-e95ec-rayjob-gpu-1   True       False         False    22s
```

Note the two columns in the output: `ACCEPTED` and `PROVISIONED`.
`ACCEPTED=True` means that Kueue and the Kubernetes node autoscaler have acknowledged the request.
`PROVISIONED=True` means that the Kubernetes node autoscaler has completed provisioning nodes.
Once both of these conditions are true, the ProvisioningRequest is satisfied.
```bash
$ kubectl get provisioningrequest
NAME                                                      ACCEPTED   PROVISIONED   FAILED   AGE
rayjob-pytorch-text-classifier-nv77q-e95ec-rayjob-gpu-1   True       True          False    57s
```

Because the example RayJob requires 1 GPU for fine-tuning, the ProvisioningRequest is satisfied
by the addition of a single GPU node in the `gpu-node-pool` Node Pool.
```bash
$ kubectl get nodes
NAME                                                  STATUS   ROLES    AGE   VERSION
gke-kuberay-gpu-cluster-default-pool-8d883840-fd6d    Ready    <none>   14m   v1.29.0-gke.1381000
gke-kuberay-gpu-cluster-gpu-node-pool-b176212e-g3db   Ready    <none>   46s   v1.29.0-gke.1381000  # new node with GPUs
```

Once the ProvisioningRequest is satisfied, Kueue admits the RayJob.
The Kubernetes scheduler then immediately places the head and worker nodes onto the newly provisioned resources.
The ProvisioningRequest ensures a seamless Ray cluster start up, with no scheduling delays for any Pods.

```bash
$ kubectl get pods
NAME                                                      READY   STATUS    RESTARTS        AGE
pytorch-text-classifier-nv77q-g6z57                       1/1     Running   0               13s
torch-text-classifier-nv77q-raycluster-gstrk-head-phnfl   1/1     Running   0               6m43s
```
