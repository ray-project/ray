(kuberay-gke-tpu-cluster-setup)=

# Start Google Cloud GKE Cluster with TPUs for KubeRay

See the [GKE documentation](<https://cloud.google.com/kubernetes-engine/docs/how-to/tpus>) for full details, or continue reading for a quick start.

## Step 1: Create a Kubernetes cluster on GKE

First, set the following environment variables to be used for GKE cluster creation:
```sh
export CLUSTER_NAME=CLUSTER_NAME
export COMPUTE_ZONE=ZONE
export CLUSTER_VERSION=CLUSTER_VERSION
```
Replace the following:
- CLUSTER_NAME: The name of the GKE cluster to be created.
- ZONE: The zone with available TPU quota, for a list of TPU availability by zones, see the [GKE documentation](https://cloud.google.com/tpu/docs/regions-zones).
- CLUSTER_VERSION: The GKE version to use. TPU v6e is supported in GKE versions 1.31.2-gke.1115000 or later. See the [GKE documentation](https://cloud.google.com/tpu/docs/tpus-in-gke#tpu-machine-types) for TPU generations and their minimum supported version.

Run the following commands on your local machine or on the [Google Cloud Shell](https://cloud.google.com/shell). If running from your local machine, install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).

Create a Standard GKE cluster and enable the Ray Operator:

```sh
gcloud container clusters create $CLUSTER_NAME \
    --addons=RayOperator \
    --machine-type=n1-standard-16 \
    --cluster-version=$CLUSTER_VERSION \
    --location=$ZONE
```

Run the following command to add a TPU node pool to the cluster. You can also create it from the [Google Cloud Console](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#console):

Create a node pool with a single-host v4 TPU topology as follows:
```sh
gcloud container node-pools create v4-4 \
  --zone $ZONE \
  --cluster $CLUSTER_NAME \
  --num-nodes 1 \
  --min-nodes 0 \
  --max-nodes 10 \
  --enable-autoscaling \
  --machine-type ct4p-hightpu-4t \
  --tpu-topology 2x2x1
```
- For v4 TPUs, ZONE must be `us-central2-b`.

Alternatively, create a multi-host node pool as follows:

```sh
gcloud container node-pools create v4-8 \
  --zone $ZONE \
  --cluster $CLUSTER_NAME \
  --num-nodes 2 \
  --min-nodes 0 \
  --max-nodes 10 \
  --enable-autoscaling \
  --machine-type ct4p-hightpu-4t \
  --tpu-topology 2x2x2
```
- For v4 TPUs, ZONE must be `us-central2-b`.

The `--tpu-topology` flag specifies the physical topology of the TPU Pod slice. This example uses a v4 TPU slice with either a 2x2x1 or 2x2x2 topology. v4 TPUs have 4 chips per VM host, so a 2x2x2 v4 slice has 8 chips total and 2 TPU hosts, each scheduled on their own node. GKE treats multi-host TPU slices as atomic units, and scales them using node pools rather than singular nodes. Therefore, the number of TPU hosts should always equal the number of nodes in the TPU node pool. For more information about selecting a TPU topology and accelerator, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/concepts/tpus).

GKE uses Kubernetes node selectors to ensure TPU workloads run on the desired machine type and topology.
For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#workload_preparation).

## Step 2: Connect to the GKE cluster

Run the following command to download Google Cloud credentials and configure the Kubernetes CLI to use them.

```sh
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE
```

The remote GKE cluster is now reachable through `kubectl`. For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).


### [Optional] Manually install KubeRay and the TPU webhook in a GKE cluster without the Ray Operator Addon:

In a cluster without the Ray Operator Addon enabled, KubeRay can be manually installed using [helm](https://ray-project.github.io/kuberay/deploy/helm/) with the following commands:

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

# Install both CRDs and KubeRay operator v1.5.0.
helm install kuberay-operator kuberay/kuberay-operator --version 1.5.0
```

GKE provides a [validating and mutating webhook](https://github.com/ai-on-gke/kuberay-tpu-webhook) to handle TPU Pod scheduling and bootstrap certain environment variables used for [JAX](https://github.com/google/jax) initialization. The Ray TPU webhook requires a KubeRay operator version of at least v1.1.0. GKE automatically installs the Ray TPU webhook through the [Ray Operator Addon](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/how-to/enable-ray-on-gke) with GKE versions 1.30.0-gke.1747000 or later.

When manually installing the webhook, [cert-manager](https://github.com/cert-manager/cert-manager) is required to handle TLS certificate injection. You can install cert-manager in both GKE Standard and Autopilot clusters using the following helm commands:

Install cert-manager:
```
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install --create-namespace --namespace cert-manager --set installCRDs=true --set global.leaderElection.namespace=cert-manager cert-manager jetstack/cert-manager
```

Next, deploy the Ray TPU initialization webhook:
1. `git clone https://github.com/GoogleCloudPlatform/ai-on-gke`
2. `cd ray-on-gke/tpu/kuberay-tpu-webhook`
3. `make deploy deploy-cert`
