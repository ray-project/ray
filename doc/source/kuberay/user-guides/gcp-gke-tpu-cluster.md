---
myst:
  html_meta:
    description: "Create a GKE cluster with TPU node pools for KubeRay, including manual install of the TPU webhook."
---

(kuberay-gke-tpu-cluster-setup)=

# Set up a Google Cloud GKE cluster with TPUs for KubeRay

This guide shows how to create a Google Kubernetes Engine (GKE) Standard cluster with TPU slice node pools for KubeRay. For full details, see [Deploy TPU workloads in GKE Standard](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus).

## Step 1: Create a GKE cluster and TPU node pool

Set the environment variables for your cluster:

```sh
export CLUSTER_NAME=<cluster-name>
export REGION=<region>
export ZONE=<zone>
export CLUSTER_VERSION=<cluster-version>
```

Replace the following placeholders:

- `<cluster-name>`: The name of the GKE cluster to create.
- `<region>` and `<zone>`: The Google Cloud region and zone with TPU quota (for example, `us-central1` and `us-central1-b`). For TPU availability by zone, see [TPU regions and zones](https://cloud.google.com/tpu/docs/regions-zones).
- `<cluster-version>`: The GKE version. [TPU v6e](https://cloud.google.com/tpu/docs/v6e) requires GKE `1.31.2-gke.1115000` or later, and [TPU7x](https://cloud.google.com/kubernetes-engine/docs/concepts/tpu-ironwood) requires GKE `1.34.1-gke.2541000` or later. See [TPUs in GKE](https://cloud.google.com/tpu/docs/tpus-in-gke#tpu-machine-types) for minimum supported versions.

Run the following commands on your local machine (with the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installed) or in [Google Cloud Shell](https://cloud.google.com/shell).

Create a Standard GKE cluster with the [GKE Ray Operator add-on](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/how-to/enable-ray-on-gke) enabled:

```sh
gcloud container clusters create $CLUSTER_NAME \
    --location=$ZONE \
    --cluster-version=$CLUSTER_VERSION \
    --machine-type=e2-standard-16 \
    --num-nodes=1 \
    --addons=RayOperator
```

Add a TPU slice node pool to the cluster (or create one from the [Google Cloud Console](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#console)):

::::{tab-set}

:::{tab-item} TPU v6e single-host (2x2)

Create a single-host TPU v6e node pool (`ct6e-standard-4t`, four chips per node). Do not set `--tpu-topology` for single-host node pools; GKE automatically labels each node with `cloud.google.com/gke-tpu-topology: 2x2` and scales individual TPU nodes between `--min-nodes` and `--max-nodes`:

```sh
gcloud container node-pools create tpu-v6e-singlehost \
  --location=$ZONE \
  --cluster=$CLUSTER_NAME \
  --machine-type=ct6e-standard-4t \
  --num-nodes=1 \
  --min-nodes=0 \
  --max-nodes=10 \
  --enable-autoscaling
```

:::

:::{tab-item} TPU v6e multi-host (4x4)

Create a multi-host TPU v6e node pool with a `4x4` topology (16 chips across four hosts). GKE scales multi-host node pools atomically as a complete slice between `0` and `--max-nodes=4`:

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

For multi-host TPU7x node pools, create a `HIGH_THROUGHPUT` workload policy that defines the `2x2x2` topology (eight dual-chiplet chips, or 16 logical TPU devices, across two hosts) and attach it with `--placement-policy`:

```sh
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

### Recommended GKE node pool options

Configure the following node pool options based on your workload:

- **Single-host versus multi-host scaling:** Single-host node pools scale node by node. Multi-host node pools represent a single ICI-connected TPU slice and scale atomically between `0` and the slice size (`numOfHosts`). To provision multiple multi-host slices, create one node pool per slice or use [ClusterAutoscaler custom compute classes](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus#custom-compute-class).
- **Consumption options:** Add `--spot` for Spot VMs, `--flex-start` (with `--num-nodes=0 --enable-autoscaling`) for Dynamic Workload Scheduler Flex-Start, or `--reservation-affinity=specific --reservation=<reservation-name>` for reserved capacity. See [Choose a TPU consumption option](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#tpu_reservation).
- **Multi-NIC networking (DRANET):** Multi-chip TPU VMs (such as `ct6e-standard-4t` and `tpu7x-standard-4t`) have multiple network interfaces (NICs), but Pods use only the primary NIC (`eth0`) by default. For data center network (DCN)-intensive workloads such as multi-slice training or disaggregated RL weight transfer, create the cluster with `--enable-dataplane-v2` and pass `--accelerator-network-profile=auto` and `--node-labels=cloud.google.com/gke-networking-dra-driver=true` during node pool creation. These flags provision secondary VPC networks and enable [GKE managed DRANET](https://cloud.google.com/kubernetes-engine/docs/how-to/allocate-network-resources-dra) so Ray worker Pods can claim secondary NICs (`netdev.google.com`) through a Kubernetes `ResourceClaimTemplate`. See [Configure automated networking for accelerator VMs](https://cloud.google.com/kubernetes-engine/docs/how-to/config-auto-net-for-accelerators).

## Step 2: Connect to the GKE cluster

Install [`gke-gcloud-auth-plugin`](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#install_plugin) if needed, then configure `kubectl` and verify the cluster nodes:

```sh
gcloud components install gke-gcloud-auth-plugin
gcloud container clusters get-credentials $CLUSTER_NAME --location=$ZONE
kubectl get nodes
```

For more details, see [Configure cluster access for kubectl](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl).

### Optional: Manually install KubeRay and the TPU webhook

When you enable the [GKE Ray Operator add-on](https://cloud.google.com/kubernetes-engine/docs/add-on/ray-on-gke/how-to/enable-ray-on-gke) (`--addons=RayOperator`), GKE automatically manages KubeRay and the TPU initialization webhook.

If you manage your own KubeRay installation instead of using the GKE add-on, complete the following steps:

1. Install the KubeRay operator (`v1.6.0` or later recommended). See {ref}`KubeRay Operator Installation <kuberay-operator-deploy>`.
2. Install the [KubeRay TPU initialization webhook](https://github.com/ai-on-gke/kuberay-tpu-webhook) (`v1.4.0` or later) by following the [installation instructions in the `kuberay-tpu-webhook` README](https://github.com/ai-on-gke/kuberay-tpu-webhook#installation).
