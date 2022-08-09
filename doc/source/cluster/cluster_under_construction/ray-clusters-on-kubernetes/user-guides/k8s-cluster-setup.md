(kuberay-k8s-setup)=

# Managed Kubernetes services

The KubeRay operator and Ray can run on any cloud or on-prem Kubernetes cluster.
The simplest way to provision a remote Kubernetes cluster is to use a cloud-based managed service.
We collect a few helpful links for users who are getting started with a managed Kubernetes service.

(gke-setup)=
# Setting up a GKE cluster (Google Cloud)
You can find the landing page for GKE [here](https://cloud.google.com/kubernetes-engine).
If you have an account set up, you can immediately start experimenting with Kubernetes clusters in the provider's console.
Alternatively, check out the [documentation](https://cloud.google.com/kubernetes-engine/docs/) and
[quickstart guides](https://cloud.google.com/kubernetes-engine/docs/deploy-app-cluster). To successfully deploy Ray on Kubernetes,
you will need to configure pools of Kubernetes nodes;
find guidance [here](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pools).

(eks-setup)=
# Setting up an EKS cluster (AWS)
You can find the landing page for EKS [here](https://aws.amazon.com/eks/).
If you have an account set up, you can immediately start experimenting with Kubernetes clusters in the provider's console.
Alternatively, check out the [documentation](https://docs.aws.amazon.com/eks/latest/userguide/) and
[quickstart guides](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html). To successfully deploy Ray on Kubernetes,
you will need to configure groups of Kubernetes nodes;
find guidance [here](https://docs.aws.amazon.com/eks/latest/userguide/managed-node-groups.html).

(aks-setup)=
# Setting up an AKS (Microsoft Azure)
You can find the landing page for AKS [here](https://azure.microsoft.com/en-us/services/kubernetes-service/).
If you have an account set up, you can immediately start experimenting with Kubernetes clusters in the provider's console.
Alternatively, check out the [documentation](https://docs.microsoft.com/en-us/azure/aks/) and
[quickstart guides](https://docs.microsoft.com/en-us/azure/aks/learn/quick-kubernetes-deploy-portal?tabs=azure-cli). To successfully deploy Ray on Kubernetes,
you will need to configure pools of Kubernetes nodes;
find guidance [here](https://docs.microsoft.com/en-us/azure/aks/use-multiple-node-pools).
