(kuberay-aks-gpu-cluster-setup)=

# Start Azure AKS Cluster with GPUs for KubeRay

This guide walks you through the steps to create an Azure AKS cluster with GPU nodes specifically for KubeRay.
The configuration outlined here can be applied to most KubeRay examples found in the documentation.

You can find the landing page for AKS [here](https://azure.microsoft.com/en-us/services/kubernetes-service/).
If you have an account set up, you can immediately start experimenting with Kubernetes clusters in the provider's console. Alternatively, check out the [documentation](https://docs.microsoft.com/en-us/azure/aks/) and [quickstart guides](https://docs.microsoft.com/en-us/azure/aks/learn/quick-kubernetes-deploy-portal?tabs=azure-cli).
To successfully deploy Ray on Kubernetes, you will need to use node pools following the guidance [here](https://docs.microsoft.com/en-us/azure/aks/use-multiple-node-pools).

## Step 1: Create a Resource Group

To create a resource group in a particular region:

```
az group create -l eastus -n kuberay-rg
```

## Step 2: Create AKS Cluster

To create an AKS cluster with system nodepool:
```
az aks create \
   -g kuberay-rg \
   -n kuberay-gpu-cluster \
   --nodepool-name system \
   --node-vm-size Standard_D8s_v3 \
   --node-count 3
```

## Step 3: Add a GPU node group

To add a GPU nodepool with autoscaling:
```
az aks nodepool add \
   -g kuberay-rg \
   --cluster-name kuberay-gpu-cluster \
   --nodepool-name gpupool \
   --node-vm-size Standard_NC6s_v3 \
   --node-taints nvidia.com/gpu=present:NoSchedule \
   --min-count 0 \
   --max-count 3 \
   --enable-cluster-autoscaler
```
To use NVIDIA GPU operator alternatively, follow instructions [here](https://learn.microsoft.com/en-us/azure/aks/gpu-cluster?tabs=add-ubuntu-gpu-node-pool#skip-gpu-driver-installation-preview)

## Step 4: Get kubeconfig

To get kubeconfig:
```
az aks get-credentials --resource-group kuberay-rg \
    --name kuberay-gpu-cluster \
    --overwrite-existing
```