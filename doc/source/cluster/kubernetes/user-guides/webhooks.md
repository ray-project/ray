(kuberay-webhooks)=

# Webhooks

This guide explains how to enable KubeRay's [K8s admission webhooks]. KubeRay supports the
following webhooks.

* RayCluster validating admission webhook

## Quickstart

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.24.0
```

## Step 2: Deploy KubeRay operator

Deploy KubeRay operator by following {ref}`Getting Started guide <kuberay-operator-deploy>`.

## Step 3: Provision certificates for the webhook server

We recommend using [cert-manager], but other solutions should also work as long as they put the
certificates in the desired location.

### Step 4: Install the webhooks

Check out the KubeRay repository and install the webhooks with the following commands.

* `git clone https://github.com/ray-project/kuberay`
* `git checkout VERSION` where `VERSION` is the version of KubeRay you are using, e.g. `v1.0.0
* `cd ray-operator`
* `make deploy-with-webhooks IMG=kuberay/operator:v1.0.0`

### Step 5: Check the webhook is working

Check the validating webhook is working by trying to create an invalid RayCluster that has duplicate
worker group names. Save the following RayCluster YAML as `invalid-raycluster.yaml` and apply it with
`kubectl apply -f invalid-raycluster.yaml`.

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: invalid-raycluster
spec:
  headGroupSpec:
    rayStartParams:
      dashboard-host: '0.0.0.0'
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.7.0
  workerGroupSpecs:
  - replicas: 1
    minReplicas: 1
    maxReplicas: 10
    groupName: group1
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.7.0
  - replicas: 1
    minReplicas: 1
    maxReplicas: 10
    groupName: group1
    rayStartParams: {}
    template:
      spec:
        containers:
        - name: ray-worker
          image: rayproject/ray:2.7.0
```

### Step 6: Check error message

You should see an error message stating `The RayCluster "invalid-raycluster" is invalid ... worker
group names must be unique`.

[K8s admission webhooks]: https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/
[cert-manager]: https://cert-manager.io/docs/installation/
