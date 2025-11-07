(kuberay-operator-deploy)=

# KubeRay Operator Installation

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/). If you already have a Kubernetes cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install KubeRay operator

### Method 1: Helm (Recommended)

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install both CRDs and KubeRay operator v1.5.0.
helm install kuberay-operator kuberay/kuberay-operator --version 1.5.0
```

### Method 2: Kustomize

```sh
# Install CRD and KubeRay operator.
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v1.5.0"
```

## Step 3: Validate Installation

Confirm that the operator is running in the namespace `default`.

```sh
kubectl get pods
```

```text
NAME                                READY   STATUS    RESTARTS   AGE
kuberay-operator-6bc45dd644-gwtqv   1/1     Running   0          24s
```
