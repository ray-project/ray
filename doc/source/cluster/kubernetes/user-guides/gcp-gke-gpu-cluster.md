(kuberay-gke-gpu-cluster-setup)=

# Start Google Cloud GKE Cluster with GPUs for KubeRay

## Step 1: Create a Kubernetes cluster on GKE

Run this command and all following commands on your local machine or on the [Google Cloud Shell](https://cloud.google.com/shell). If running from your local machine, you will need to install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install). The following command creates a Kubernetes cluster named `kuberay-gpu-cluster` with 1 CPU node in the `us-west1-b` zone. In this example, we use the `e2-standard-4` machine type, which has 4 vCPUs and 16 GB RAM.

```sh
gcloud container clusters create kuberay-gpu-cluster \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-west1-b --machine-type e2-standard-4
```

> Note: You can also create a cluster from the [Google Cloud Console](https://console.cloud.google.com/kubernetes/list).

## Step 2: Create a GPU node pool

Run the following command to create a GPU node pool for Ray GPU workers.
(You can also create it from the Google Cloud Console; see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/node-taints#create_a_node_pool_with_node_taints) for more details.)

```sh
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-l4-vws,count=1 \
  --zone us-west1-b \
  --cluster kuberay-gpu-cluster \
  --num-nodes 1 \
  --min-nodes 0 \
  --max-nodes 1 \
  --enable-autoscaling \
  --machine-type g2-standard-4 \
  --node-taints=ray.io/node-type=worker:NoSchedule 
```

The `--accelerator` flag specifies the type and number of GPUs for each node in the node pool. In this example, we use the [NVIDIA L4](https://cloud.google.com/compute/docs/gpus#l4-gpus) GPU. The machine type `g2-standard-4` has 1 GPU, 24 GB GPU Memory, 4 vCPUs and 16 GB RAM.

The taint `ray.io/node-type=worker:NoSchedule` prevents CPU-only Pods such as the Kuberay operator, Ray head, and CoreDNS Pods from being scheduled on this GPU node pool. This is because GPUs are expensive, so we want to use this node pool for Ray GPU workers only.

Concretely, any Pod that does not have the following toleration will not be scheduled on this GPU node pool:

```yaml
tolerations:
- key: ray.io/node-type
  operator: Equal
  value: worker
  effect: NoSchedule
```

For more on taints and tolerations, see the [Kubernetes documentation](https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/).

## Step 3: Configure `kubectl` to connect to the cluster

Run the following command to download Google Cloud credentials and configure the Kubernetes CLI to use them.

```sh
gcloud container clusters get-credentials kuberay-gpu-cluster --zone us-west1-b
```

For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).

## Step 4: Install NVIDIA GPU device drivers

This step is required for GPU support on GKE. See the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus#installing_drivers) for more details.

```sh
# Install NVIDIA GPU device driver
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded-latest.yaml

# Verify that your nodes have allocatable GPUs 
kubectl get nodes "-o=custom-columns=NAME:.metadata.name,GPU:.status.allocatable.nvidia\.com/gpu"

# Example output:
# NAME                                          GPU
# gke-kuberay-gpu-cluster-gpu-node-pool-xxxxx   1
# gke-kuberay-gpu-cluster-default-pool-xxxxx    <none>
```
