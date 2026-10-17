---
myst:
  html_meta:
    description: "Create a GKE cluster with TPU node pools for KubeRay, including manual install of the TPU webhook."
---

(kuberay-gke-tpu-cluster-setup)=

# Start Google Cloud GKE Cluster with TPUs for KubeRay

See the [GKE documentation](<https://cloud.google.com/kubernetes-engine/docs/how-to/tpus>) for full details, or continue reading for a quick start.

## Step 1: Create a Kubernetes cluster on GKE

First, set the following environment variables to be used for GKE cluster creation:
```sh
export CLUSTER_NAME=CLUSTER_NAME
export ZONE=ZONE
export CLUSTER_VERSION=CLUSTER_VERSION
```
Replace the following:
- CLUSTER_NAME: The name of the GKE cluster to be created.
- ZONE: The zone with available TPU quota, for a list of TPU availability by zones, see the [GKE documentation](https://cloud.google.com/tpu/docs/regions-zones).
- CLUSTER_VERSION: The GKE version to use. TPU v6e is supported in GKE versions 1.31.2-gke.1115000 or later, and TPU7x is supported in GKE versions 1.34.1-gke.2541000 or later. See the [GKE documentation](https://cloud.google.com/tpu/docs/tpus-in-gke#tpu-machine-types) for TPU generations and their minimum supported version.

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

::::{tab-set}

:::{tab-item} TPU v6e single-host (2x4)

Create a single-host TPU v6e node pool using `ct6e-standard-8t`, which has eight chips in a `2x4` topology on one host:

```sh
gcloud container node-pools create tpu-v6e-singlehost \
    --location=$ZONE \
    --cluster=$CLUSTER_NAME \
    --machine-type=ct6e-standard-8t \
    --num-nodes=1 \
    --min-nodes=0 \
    --max-nodes=4 \
    --enable-autoscaling
```

:::

:::{tab-item} TPU v6e multi-host (4x4)

Create a multi-host TPU v6e node pool with a `4x4` topology across four `ct6e-standard-4t` hosts:

```sh
gcloud container node-pools create tpu-v6e-multihost \
    --location=$ZONE \
    --cluster=$CLUSTER_NAME \
    --machine-type=ct6e-standard-4t \
    --tpu-topology=4x4 \
    --num-nodes=4 \
    --min-nodes=0 \
    --max-nodes=4 \
    --enable-autoscaling
```

:::

:::{tab-item} TPU7x multi-host (2x2x2)

[TPU7x](https://cloud.google.com/kubernetes-engine/docs/concepts/tpu-ironwood) multi-host node pools use a `HIGH_THROUGHPUT` Compute Engine workload policy to define the 3D torus topology. The following command creates a `2x2x2` slice with eight dual-chiplet chips across two hosts:

```sh
export REGION=${ZONE%-*}

gcloud compute resource-policies create workload-policy tpu7x-2x2x2-policy \
    --type=HIGH_THROUGHPUT \
    --accelerator-topology=2x2x2 \
    --region=$REGION

gcloud container node-pools create tpu7x-multihost \
    --location=$ZONE \
    --cluster=$CLUSTER_NAME \
    --node-locations=$ZONE \
    --machine-type=tpu7x-standard-4t \
    --placement-policy=tpu7x-2x2x2-policy \
    --num-nodes=2 \
    --min-nodes=0 \
    --max-nodes=2 \
    --enable-autoscaling
```

:::

::::

The `--tpu-topology` flag, or `--accelerator-topology` on a workload policy, specifies the physical topology of the TPU Pod slice. For example, each `ct6e-standard-4t` host has 4 chips, so a `4x4` v6e slice has 16 chips across 4 TPU hosts, each scheduled on its own node. GKE treats multi-host TPU slices as atomic units, and scales them using node pools rather than singular nodes. Therefore, the number of TPU hosts should always equal the number of nodes in the TPU node pool. To provision multiple multi-host slices, create one node pool per slice or use [ClusterAutoscaler custom compute classes](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#custom-compute-class). For more information about selecting a TPU topology and accelerator, see [About TPUs in GKE](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/tpus).

GKE uses Kubernetes node selectors to ensure TPU workloads run on the desired machine type and topology. For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#workload_preparation).

### Recommended GKE node pool options

Configure the following node pool options based on your workload:

- **Consumption options:** Add `--spot` for Spot VMs, `--flex-start` with `--num-nodes=0 --enable-autoscaling` for Dynamic Workload Scheduler Flex-Start, or `--reservation-affinity=specific --reservation=<reservation-name>` for reserved capacity. See [Choose a TPU consumption option](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#tpu_reservation).
- **Multi-NIC networking:** Multi-host TPU VMs include multiple network interfaces for high-bandwidth data center network traffic alongside the optical inter-chip interconnect. For multi-slice training or disaggregated RL weight transfer, enable GKE Dataplane V2 with `--enable-dataplane-v2` when creating the cluster. Then pass `--accelerator-network-profile=auto` and `--node-labels=cloud.google.com/gke-networking-dra-driver=true` when creating the node pool. See [Configure automated multi-network support for TPUs](https://cloud.google.com/kubernetes-engine/docs/how-to/config-auto-net-for-accelerators).

## Step 2: Connect to the GKE cluster

Run the following command to download Google Cloud credentials and configure the Kubernetes CLI to use them.

```sh
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE
```

The remote GKE cluster is now reachable through `kubectl`. For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).


### [Optional] Manually install KubeRay and the TPU webhook in a GKE cluster without the Ray Operator Addon:

In a cluster without the Ray Operator Addon enabled, KubeRay can be manually installed using Helm with the following commands. See {ref}`KubeRay Operator Installation <kuberay-operator-deploy>` for the full installation guide.

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

# Install both CRDs and KubeRay operator v1.7.0.
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.0
```

GKE provides a [validating and mutating webhook](https://github.com/ai-on-gke/kuberay-tpu-webhook) to handle TPU Pod scheduling and bootstrap environment variables used for [JAX](https://github.com/google/jax) and TorchTPU initialization. The Ray TPU webhook requires a KubeRay operator version of at least v1.1.0. GKE automatically installs the Ray TPU webhook through the [Ray Operator Addon](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/how-to/enable-ray-on-gke) with GKE versions 1.30.0-gke.1747000 or later.

When manually installing the webhook, [cert-manager](https://github.com/cert-manager/cert-manager) is required to handle TLS certificate injection. You can install cert-manager in both GKE Standard and Autopilot clusters using the following helm commands:

Install cert-manager:
```
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install --create-namespace --namespace cert-manager --set installCRDs=true --set global.leaderElection.namespace=cert-manager cert-manager jetstack/cert-manager
```

Next, deploy the Ray TPU initialization webhook:
1. `git clone https://github.com/ai-on-gke/kuberay-tpu-webhook.git`
2. `cd kuberay-tpu-webhook`
3. `make deploy deploy-cert`
