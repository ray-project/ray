(kuberay-operator-deploy)=

# KubeRay Operator Installation

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/). If you already have a Kubernetes cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install KubeRay operator

### Method 1: Helm (Recommended)

Install the operator into a dedicated `ray-system` namespace rather than `default` to isolate the operator's service account from workload pods.

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
kubectl create namespace ray-system
helm install kuberay-operator kuberay/kuberay-operator --version 1.7.0 -n ray-system
```

### Method 2: Kustomize

```sh
# Install CRD and KubeRay operator into the ray-system namespace.
kubectl create namespace ray-system
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v1.7.0" -n ray-system
```

## Step 3: Validate Installation

Confirm that the operator is running. If you installed into `ray-system`, pass `-n ray-system`:

```sh
kubectl get pods -n ray-system
```

```text
NAME                                READY   STATUS    RESTARTS   AGE
kuberay-operator-6bc45dd644-gwtqv   1/1     Running   0          24s
```
