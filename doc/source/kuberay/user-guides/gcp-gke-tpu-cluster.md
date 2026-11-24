---
myst:
  html_meta:
    description: "Create a GKE cluster with TPU node pools for KubeRay, including manual install of the TPU webhook."
---

(kuberay-gke-tpu-cluster-setup)=

# Start a Google Cloud GKE cluster with TPUs for KubeRay

This page shows how to create a Google Kubernetes Engine (GKE) cluster with Tensor Processing Unit (TPU) node pools for KubeRay. For full GKE TPU documentation, see [Deploy TPU workloads in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus).

## Step 1: Create a Kubernetes cluster on GKE

First, set the following environment variables for GKE cluster creation:

```bash
export CLUSTER_NAME=<cluster-name>
export ZONE=<zone>
```

Replace the following:

- `<cluster-name>`: The name of the GKE cluster to create.
- `<zone>`: The zone with available TPU quota. For a list of TPU availability by zone, see [TPU regions and zones](https://cloud.google.com/tpu/docs/regions-zones).

Run the following commands on your local machine or in [Google Cloud Shell](https://cloud.google.com/shell). If you run them from your local machine, install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install).

For supported GKE versions by TPU generation, see [Plan TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#choose-tpu-version). Create a Standard GKE cluster and enable the Ray Operator add-on:

```bash
gcloud container clusters create $CLUSTER_NAME \
    --location=$ZONE \
    --addons=RayOperator \
    --machine-type=e2-standard-16 \
    --num-nodes=1
```

To add a TPU node pool to the cluster, run the commands in one of the following tabs. You can also create the node pool from the [Google Cloud console](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#console).

::::{tab-set}

:::{tab-item} TPU v6e single-host (2x4)

Create a single-host TPU v6e node pool with the `ct6e-standard-8t` machine type, which has eight chips in a `2x4` topology on one host:

```bash
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

```bash
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

[TPU7x](https://cloud.google.com/kubernetes-engine/docs/concepts/tpu-ironwood) multi-host node pools use a `HIGH_THROUGHPUT` Compute Engine workload policy to define the 3D torus topology. The following commands create a `2x2x2` slice with eight dual-chiplet chips across two hosts:

```bash
export REGION=${ZONE%-*}

gcloud compute resource-policies create workload-policy tpu7x-2x2x2-policy \
    --type=HIGH_THROUGHPUT \
    --accelerator-topology=2x2x2 \
    --region=$REGION

gcloud container node-pools create tpu7x-multihost \
    --location=$ZONE \
    --cluster=$CLUSTER_NAME \
    --machine-type=tpu7x-standard-4t \
    --placement-policy=tpu7x-2x2x2-policy \
    --num-nodes=2 \
    --min-nodes=0 \
    --max-nodes=2 \
    --enable-autoscaling
```

:::

::::

The `--tpu-topology` flag, or `--accelerator-topology` on a workload policy, specifies the physical topology of the TPU Pod slice. For example, each `ct6e-standard-4t` host has four chips, so a `4x4` v6e slice has 16 chips across four TPU hosts, each scheduled on its own node. GKE treats a multi-host TPU slice as an atomic unit and scales it as a node pool rather than as individual nodes. The number of TPU hosts must therefore equal the number of nodes in the TPU node pool. To provision multiple multi-host slices, create one node pool per slice or use [custom compute classes](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#custom-compute-classes). For more information about selecting a TPU topology and accelerator, see [About TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/concepts/tpus).

GKE uses Kubernetes node selectors to ensure TPU workloads run on the desired machine type and topology. For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#workload_preparation).

### Recommended GKE node pool options

Configure the following node pool options based on your workload:

- **Consumption options**: Add `--spot` for Spot VMs, `--flex-start` with `--num-nodes=0 --enable-autoscaling` for Dynamic Workload Scheduler Flex-Start, or `--reservation-affinity=specific --reservation=<reservation-name>` for reserved capacity. See [Choose a TPU consumption option](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#tpu_reservation).
- **Multi-NIC networking**: Multi-host TPU VMs include multiple network interfaces for high-bandwidth data center network traffic alongside the inter-chip interconnect. For multi-slice training or disaggregated reinforcement learning weight transfer, enable GKE Dataplane V2 with `--enable-dataplane-v2` when you create the cluster. Then pass `--accelerator-network-profile=auto` and `--node-labels=cloud.google.com/gke-networking-dra-driver=true` when you create the node pool. See [Configure automated multi-network support for TPUs](https://cloud.google.com/kubernetes-engine/docs/how-to/config-auto-net-for-accelerators).

## Step 2: Connect to the GKE cluster

Run the following command to download Google Cloud credentials and configure `kubectl` to use them:

```bash
gcloud container clusters get-credentials $CLUSTER_NAME --location=$ZONE
```

You can then reach the remote GKE cluster through `kubectl`. For more details, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).

### Optional: Manually install KubeRay and the TPU webhook

If your cluster doesn't have the Ray Operator add-on enabled, install KubeRay with Helm by running the following commands. For the full installation guide, see {ref}`KubeRay operator installation <kuberay-operator-deploy>`.

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

# Install both CRDs and KubeRay operator v1.7.0.
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.0
```

GKE provides a [validating and mutating webhook](https://github.com/ai-on-gke/kuberay-tpu-webhook) to handle TPU Pod scheduling and bootstrap environment variables for [JAX](https://github.com/google/jax) and [TorchTPU](https://torchtpu.readthedocs.io/en/latest/) initialization. GKE automatically installs the Ray TPU webhook through the [Ray Operator add-on](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/how-to/enable-ray-on-gke).

When you install the webhook manually, [cert-manager](https://github.com/cert-manager/cert-manager) handles TLS certificate injection. Install cert-manager in a GKE Standard or Autopilot cluster with the following Helm commands:

```bash
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install --create-namespace --namespace cert-manager --set installCRDs=true --set global.leaderElection.namespace=cert-manager cert-manager jetstack/cert-manager
```

Next, authenticate with Artifact Registry and install the Ray TPU initialization webhook chart with Helm:

```bash
gcloud auth configure-docker us-docker.pkg.dev
helm install kuberay-tpu-webhook oci://us-docker.pkg.dev/ai-on-gke/kuberay-tpu-webhook-helm/kuberay-tpu-webhook
```
