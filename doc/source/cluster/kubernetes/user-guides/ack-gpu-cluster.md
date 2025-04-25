(kuberay-ack-gpu-cluster-setup)=

# Start Aliyun ACK Cluster with GPUs for KubeRay

This guide walks you through the steps to create an ACK cluster with GPU nodes specifically for KubeRay.
The configuration outlined here can be applied to most KubeRay examples found in the documentation.

## Step 1: Create a Kubernetes cluster on Aliyun ACK

Follow the first two steps in [this Aliyun documentation](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/create-an-ack-managed-cluster-2) to:
(1) create your Aliyun ACK cluster and (2) configure your computer to communicate with your cluster.

## Step 2: Create node pools for the Aliyun ACK cluster

Follow "Create a node pool" in [this Aliyun documentation](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/create-a-node-pool) to create node pools.

### Manage node labels and taints

If you need to set taints for nodes, follow "Create and manage node labels" and "Create and manage node taints" in [this Aliyun documentation](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/manage-taints-and-tolerations). For example, you can add a taint to gpu node pools so that ray head pods won't be scheduled to these nodes.

### Upgrade drivers on the nodes

If you need to upgrade the drivers on the nodes, follow "Step 2: Create a node pool and specify an NVIDIA driver version" in [this Aliyun documentation](https://www.alibabacloud.com/help/en/ack/ack-managed-and-ack-dedicated/user-guide/customize-the-gpu-driver-version-of-the-node-by-specifying-the-version-number) to upgrade drivers.

## Step 3: Install KubeRay addon in the cluster

Follow "Step 2: Install KubeRay-Operator" in [this Aliyun documentation](https://www.alibabacloud.com/help/en/ack/cloud-native-ai-suite/use-cases/efficient-deployment-and-optimization-practice-of-ray-in-ack-cluster?) to in KubeRay in ACK.

