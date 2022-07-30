(kuberay-ml-example)=

# XGBoost on Ray, on Kubernetes

In this guide, we show you how to run a sample Ray machine learning
workload on Kubernetes infrastructure.

We will run Ray's XGBoost benchmark with a 100 gigabyte training set.

## Autoscaling: Pros and Cons

This guide will show how to run XGBoost workload with and without optional Ray Autoscaler support.
Here are some considerations when choosing whether or not to use autoscaling functionality.

### Pros of autoscaling

#### Deal with unknown resource requirements.
If you don't know how much compute your Ray workload will require,
autoscaling will adjust your Ray cluster to the right size.

#### Cost-savings.
Idle compute is automatically scaled down, potentially leading to cost savings.

### Cons of autoscaling:

#### Less predictable when resource requirements are known.
If you already know exactly how much compute your workload requires, it may make
sense to provision a statically-sized Ray cluster.
In this guide's example, we know that we need 1 Ray head and 9 Ray workers,
so autoscaling is not strictly required.

#### Longer end-to-end runtime.
Autoscaling entails provisioning compute for Ray workers while the Ray application
is running. Pre-provisioning static compute resources allows all required compute
to be provisioned in parallel before the application executes, potentially reducing the application's runtime.

## Kubernetes infrastructure setup.

For the workload in this guide, it is recommended to use a pool (group) of Kubernetes nodes
with the following properties:
- 10 nodes total
- A capacity of 16 CPU and 64 Gi memory per node.
- Each node should be configured with 1000 gigabytes of disk space (to store the training set).

### Kubernetes infrastructure and autoscaling

**If you would like to try running the workload with autoscaling enabled**, use an autoscaling
node group or pool with
- 1 node minimum
- 10 nodes maximum
The 1 static node will be used to run the Ray head pod. This node may also host the KubeRay
operator and Kubernetes system components. After the workload is submitted, 9 additional nodes will
scale up to accommodate Ray worker pods. These nodes will scale back down after the workload is complete.

### Learn more about node pools

To learn about node group or node pool setup with managed Kubernetes services,
refer to the cloud provider documentation.
- [EKS (Amazon Web Services)](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html)
- [AKS (Azure)](https://docs.microsoft.com/en-us/azure/aks/use-multiple-node-pools)
- [GKE (Google Cloud)](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pools)

## Deploying the KubeRay operator

## Deploying a Ray cluster

## Running the workload

### Connect to the cluster.

### Submit the workload.

### Observe progress.

#### Job logs

#### Cluster state

#### Ray status
