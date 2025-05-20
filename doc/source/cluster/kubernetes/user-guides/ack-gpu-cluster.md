(kuberay-ack-gpu-cluster-setup)=

# Start an Aliyun ACK cluster with GPUs for KubeRay

This guide provides step-by-step instructions for creating an ACK cluster with GPU nodes specifically configured for KubeRay.
The configuration outlined here can be applied to most KubeRay examples found in the documentation.

## Step 1: Create a Kubernetes cluster on Aliyun ACK

See [Create a cluster](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/create-an-ack-managed-cluster-2) to create a Aliyun ACK cluster and see [Connect to clusters](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/access-clusters) to configure your computer to communicate with the cluster.

## Step 2: Create node pools for the Aliyun ACK cluster

See [Create a node pool](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/create-a-node-pool) to create node pools.

### Manage node labels and taints

If you need to set taints for nodes, see [Create and manage node labels](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/manage-taints-and-tolerations) and [Create and manage node taints](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/manage-taints-and-tolerations). For example, you can add a taint to GPU node pools so that Ray won't schedule head pods on these nodes.

### Upgrade drivers on the nodes

If you need to upgrade the drivers on the nodes, see [Step 2: Create a node pool and specify an NVIDIA driver version](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/customize-the-gpu-driver-version-of-the-node-by-specifying-the-version-number) to upgrade drivers.

## Step 3: Install KubeRay addon in the cluster

See [Step 2: Install KubeRay-Operator](https://www.alibabacloud.com/help/en/ack/cloud-native-ai-suite/use-cases/efficient-deployment-and-optimization-practice-of-ray-in-ack-cluster?) to deploy KubeRay in ACK.

