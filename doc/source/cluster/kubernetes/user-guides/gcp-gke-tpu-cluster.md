(kuberay-gke-tpu-cluster-setup)=

# Start Google Cloud GKE Cluster with TPUs for KubeRay

See <https://cloud.google.com/kubernetes-engine/docs/how-to/tpus> for full details, or continue reading for a quick start.

## Step 1: Create a Kubernetes cluster on GKE

Run this command and all following commands on your local machine or on the [Google Cloud Shell](https://cloud.google.com/shell). If running from your local machine, you need to install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install). The following command creates a Kubernetes cluster named `kuberay-tpu-cluster` with 1 CPU node in the `us-central2-b` zone. This example uses the `n2-standard-4` machine type, which has 4 vCPUs and 16 GB RAM.

```sh
gcloud container clusters create kuberay-tpu-cluster \
    --num-nodes=1 --min-nodes 0 --max-nodes 1 --enable-autoscaling \
    --region=us-central2-b --machine-type n2-standard-4
```

You can also create a cluster from the [Google Cloud Console](https://console.cloud.google.com/kubernetes/list).

## Step 2: Create a TPU node pool

Run the following command to create a TPU node pool for Ray TPU workers. You can also create it from the Google Cloud Console: <https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#console>

```sh
gcloud container node-pools create tpu-node-pool \
  --zone us-central2-b \
  --cluster kuberay-tpu-cluster \
  --num-nodes 2 \
  --min-nodes 0 \
  --max-nodes 2 \
  --enable-autoscaling \
  --machine-type ct4p-hightpu-4t \
  --tpu-topology 2x2x2
```

The `--tpu-topology` flag specifies the physical topology of the TPU PodSlice. This example uses a v4 TPU slice with a 2x2x2 topology, or 8 total TPU chips. v4 TPUs have 4 chips per VM host, so a 2x2x2 v4 slice will have 2 TPU hosts, each scheduled on their own node. TPU slices are treated as atomic units, and scaled using node pools rather than singular nodes. Therefore, the number of TPU hosts should always equal the number of nodes in the TPU node pool. For more information about selecting a TPU topology and accelerator, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/concepts/tpus).

GKE uses Kubernetes node selectors to ensure TPU workloads run on the desired TPU machine type and topology.
For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#workload_preparation).

## Step 3: Configure `kubectl` to connect to the cluster

Run the following command to download Google Cloud credentials and configure the Kubernetes CLI to use them.

```sh
gcloud container clusters get-credentials kuberay-tpu-cluster --zone us-central2-b
```

For more details, see [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).

## Step 4: Install the TPU initialization webhook

GKE provides a mutating webhook to handle TPU pod scheduling and bootstrap certain environment variables used for [JAX](https://github.com/google/jax) initialization. The Ray TPU webhook is installed once per cluster and requires a Kuberay operator version of at least v1.1.0. For instructions on installing the webhook, see the documentation [here](https://github.com/GoogleCloudPlatform/ai-on-gke/tree/main/ray-on-gke/guides/tpu#manually-installing-the-tpu-initialization-webhook).
