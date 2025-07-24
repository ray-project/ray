(kuberay-k8s-setup)=

# Managed Kubernetes services

```{toctree}
:hidden:

aws-eks-gpu-cluster
gcp-gke-gpu-cluster
gcp-gke-tpu-cluster
azure-aks-gpu-cluster
ack-gpu-cluster
```

Most KubeRay documentation examples only require a local Kubernetes cluster such as [Kind](https://kind.sigs.k8s.io/).
Some KubeRay examples require GPU nodes, which can be provided by a managed Kubernetes service.
We collect a few helpful links for users who are getting started with a managed Kubernetes service to launch a Kubernetes cluster equipped with GPUs.

(gke-setup)=
# Set up a GKE cluster (Google Cloud)

- {ref}`kuberay-gke-gpu-cluster-setup`
- {ref}`kuberay-gke-tpu-cluster-setup`

(eks-setup)=
# Set up an EKS cluster (AWS)

- {ref}`kuberay-eks-gpu-cluster-setup`

(aks-setup)=
# Set up an AKS cluster (Microsoft Azure)
- {ref}`kuberay-aks-gpu-cluster-setup`

# Set up an ACK cluster (Alibaba Cloud)
- {ref}`kuberay-ack-gpu-cluster-setup`
