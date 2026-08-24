---
myst:
  html_meta:
    description: "Install the KubeRay operator with Helm or Kustomize and validate the installation on a Kind or existing cluster."
---

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
helm install kuberay-operator kuberay/kuberay-operator --version 1.6.0 -n ray-system
```

### Method 2: Kustomize

```sh
# Install CRD and KubeRay operator into the ray-system namespace.
kubectl create namespace ray-system
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v1.6.0" -n ray-system
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

(kuberay-images)=

## Container images

KubeRay publishes its own component images to [Quay.io](https://quay.io/organization/kuberay), under `quay.io/kuberay/`. Published components include `operator`, `apiserver`, `dashboard`, `historyserver`, and `collector`. These are KubeRay's own components, separate from the Ray runtime images that your Ray clusters run, such as `rayproject/ray`, which Ray distributes on Docker Hub.

Always pull KubeRay images from Quay.io. The `kuberay` organization on Docker Hub is a legacy mirror that stopped receiving updates in early 2024.

KubeRay tags each image three ways:

* **Version tags** such as `quay.io/kuberay/operator:v1.6.0` identify a stable release. Use a version tag for anything you depend on. The operator's version tag matches the KubeRay release, so chart version `1.6.0` installs `operator:v1.6.0`.
* **Commit tags** such as `quay.io/kuberay/operator:feeaf72` pin an image to an exact `master` commit. The tag is the short Git commit hash.
* **The `nightly` tag** tracks the most recent `master` build. It moves with every merge, so it isn't a stable target. Use it only to try unreleased changes.
