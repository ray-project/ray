(kuberay-gke-tpu-cluster-setup)=

# Start Google Cloud GKE Cluster with TPUs for KubeRay

See the [GKE documentation](<https://cloud.google.com/kubernetes-engine/docs/how-to/tpus>) for full details, or continue reading for a quick start.

## Step 1: Create a Kubernetes cluster on GKE

Run the following commands on your local machine or on the [Google Cloud Shell](https://cloud.google.com/shell). If running from your local machine, install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).

Create a Standard GKE cluster and enable the Ray Operator in the us-central2-b compute region:

```sh
gcloud container clusters create kuberay-tpu-cluster \
    --addons=RayOperator \
    --machine-type=n1-standard-8 \
    --cluster-version=1.30 \
    --location=us-central2-b
```

Run the following command to add a TPU node pool to the cluster. You can also create it from the [Google Cloud Console](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#console):

Create a node pool with a single-host TPU topology as follows:
```sh
gcloud container node-pools create tpu-pool \
  --zone us-central2-b \
  --cluster kuberay-tpu-cluster \
  --num-nodes 1 \
  --min-nodes 0 \
  --max-nodes 10 \
  --enable-autoscaling \
  --machine-type ct4p-hightpu-4t \
  --tpu-topology 2x2x1
```

Alternatively, create a multi-host node pool as follows:

```sh
gcloud container node-pools create tpu-pool \
  --zone us-central2-b \
  --cluster kuberay-tpu-cluster \
  --num-nodes 2 \
  --min-nodes 0 \
  --max-nodes 10 \
  --enable-autoscaling \
  --machine-type ct4p-hightpu-4t \
  --tpu-topology 2x2x2
```

The `--tpu-topology` flag specifies the physical topology of the TPU Pod slice. This example uses a v4 TPU slice with either a 2x2x1 or 2x2x2 topology. v4 TPUs have 4 chips per VM host, so a 2x2x2 v4 slice has 8 chips total and 2 TPU hosts, each scheduled on their own node. GKE treats multi-host TPU slices as atomic units, and scales them using node pools rather than singular nodes. Therefore, the number of TPU hosts should always equal the number of nodes in the TPU node pool. For more information about selecting a TPU topology and accelerator, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/concepts/tpus).

GKE uses Kubernetes node selectors to ensure TPU workloads run on the desired machine type and topology.
For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#workload_preparation).

## Step 2: Configure `kubectl` to connect to the cluster

Run the following command to download Google Cloud credentials and configure the Kubernetes CLI to use them.

```sh
gcloud container clusters get-credentials kuberay-tpu-cluster --zone us-central2-b
```

For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).

## Step 3: Install the TPU initialization webhook

GKE provides a [validating and mutating webhook](https://github.com/GoogleCloudPlatform/ai-on-gke/tree/main/ray-on-gke/tpu/kuberay-tpu-webhook) to handle TPU Pod scheduling and bootstrap certain environment variables used for [JAX](https://github.com/google/jax) initialization. The Ray TPU webhook requires a KubeRay operator version of at least v1.1.0. GKE automatically installs the Ray TPU webhook through the [Ray Operator Addon](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/how-to/enable-ray-on-gke) with GKE versions 1.30.0-gke.1747000 or later. When manually installing the Ray TPU webhook, you need [cert-manager](https://github.com/cert-manager/cert-manager) to handle TLS certificate injection. You can install cert-manager in both GKE Standard and Autopilot clusters using the following helm commands:

### [Optional] Manually install in a GKE cluster without the Ray Addon:
Install cert-manager:
```
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install --create-namespace --namespace cert-manager --set installCRDs=true --set global.leaderElection.namespace=cert-manager cert-manager jetstack/cert-manager
```

Deploy the Ray TPU initialization webhook:
1. `git clone https://github.com/GoogleCloudPlatform/ai-on-gke`
2. `cd ray-on-gke/tpu/kuberay-tpu-webhook`
3. `make deploy deploy-cert`
