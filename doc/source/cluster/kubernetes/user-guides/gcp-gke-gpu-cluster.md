(kuberay-gke-gpu-cluster-setup)=

# Start Google Cloud GKE Cluster with GPUs for KubeRay

See <https://cloud.google.com/kubernetes-engine/docs/how-to/gpus> for full details, or continue reading for a quick start.

## Step 1: Create a Kubernetes cluster on GKE

Run this command and all following commands on your local machine or on the [Google Cloud Shell](https://cloud.google.com/shell). If running from your local machine, you will need to install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install). The following command creates a Kubernetes cluster named `kuberay-gpu-cluster` with 1 CPU node in the `us-west1-b` zone. In this example, we use the `e2-standard-4` machine type, which has 4 vCPUs and 16 GB RAM.

```sh
gcloud container clusters create kuberay-gpu-cluster \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --zone=us-west1-b --machine-type e2-standard-4
```

> Note: You can also create a cluster from the [Google Cloud Console](https://console.cloud.google.com/kubernetes/list).

## Step 2: Create a GPU node pool

Run the following command to create a GPU node pool for Ray GPU workers. You can also create it from the Google Cloud Console: <https://cloud.google.com/kubernetes-engine/docs/how-to/gpus#console>

```sh
gcloud container node-pools create gpu-node-pool \
  --accelerator type=nvidia-l4-vws,count=1,gpu-driver-version=default \
  --zone us-west1-b \
  --cluster kuberay-gpu-cluster \
  --num-nodes 1 \
  --min-nodes 0 \
  --max-nodes 1 \
  --enable-autoscaling \
  --machine-type g2-standard-4 \
```

The `--accelerator` flag specifies the type and number of GPUs for each node in the node pool. In this example, we use the [NVIDIA L4](https://cloud.google.com/compute/docs/gpus#l4-gpus) GPU. The machine type `g2-standard-4` has 1 GPU, 24 GB GPU Memory, 4 vCPUs and 16 GB RAM.

.. note::

    GKE will automatically install the GPU drivers for you.  For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus#create-gpu-pool-auto-drivers)

.. note::

    GKE will automatically configure taints and tolerations so that only GPU pods are scheduled on GPU nodes.  For more detais, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/gpus#create)

## Step 3: Configure `kubectl` to connect to the cluster

Run the following command to download Google Cloud credentials and configure the Kubernetes CLI to use them.

```sh
gcloud container clusters get-credentials kuberay-gpu-cluster --zone us-west1-b
```

For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).
