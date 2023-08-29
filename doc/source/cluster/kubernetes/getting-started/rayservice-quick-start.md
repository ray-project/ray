(kuberay-rayservice-quickstart)=

# RayService Quickstart

> Note: This guide is a quickstart for RayService, so it does not cover advanced topics such as in-place update, zero downtime upgrade, and high-availability.
For the full list of RayService features, please refer to [RayService](kuberay-rayservice). 
## Prerequisites

This guide focuses solely on the Ray Serve multi-application API, which is available starting from Ray version 2.4.0.

* Ray 2.4.0 or newer.
* KubeRay 0.6.0, KubeRay nightly, or newer.

## What's a RayService?

A RayService manages 2 things:

* **RayCluster**: Manages resources in a Kubernetes cluster.
* **Ray Serve Applications**: Manages users' applications.

## What does the RayService provide?

* **Kubernetes-native support for Ray clusters and Ray Serve applications:** After using a Kubernetes config to define a Ray cluster and its Ray Serve applications, you can use `kubectl` to create the cluster and its applications.
* **In-place update for Ray Serve applications:** Check [RayService](kuberay-rayservice) for more details.
* **Zero downtime upgrade for Ray clusters:** Check [RayService](kuberay-rayservice) for more details.
* **High-availabilable services:** Check [RayService](kuberay-rayservice) for more details.

## Example: Serve two simple Ray Serve applications using RayService

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.23.0
```

## Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.
Please note that the YAML file in this example uses `serveConfigV2` to specify a multi-application Serve config, which is supported starting from KubeRay v0.6.0.

## Step 3: Install a RayService

```sh
# Step 3.1: Download `ray_v1alpha1_rayservice.yaml`
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray_v1alpha1_rayservice.yaml

# Step 3.2: Create a RayService
kubectl apply -f ray_v1alpha1_rayservice.yaml
```

## Step 4: Verify the Kubernetes cluster status 

```sh
# Step 4.1: List all RayService custom resources in the `default` namespace.
kubectl get rayservice

# [Example output]
# NAME                AGE
# rayservice-sample   2m42s

# Step 4.2: List all RayCluster custom resources in the `default` namespace.
kubectl get raycluster

# [Example output]
# NAME                                 DESIRED WORKERS   AVAILABLE WORKERS   STATUS   AGE
# rayservice-sample-raycluster-6mj28   1                 1                   ready    2m27s

# Step 4.3: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# ervice-sample-raycluster-6mj28-worker-small-group-kg4v5   1/1     Running   0          3m52s
# rayservice-sample-raycluster-6mj28-head-x77h4             1/1     Running   0          3m52s

# Step 4.4: List services in the `default` namespace.
kubectl get services

# NAME                                          TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                                   AGE
# ...
# rayservice-sample-head-svc                    ClusterIP   10.96.34.90     <none>        10001/TCP,8265/TCP,52365/TCP,6379/TCP,8080/TCP,8000/TCP   4m58s
# rayservice-sample-raycluster-6mj28-head-svc   ClusterIP   10.96.171.184   <none>        10001/TCP,8265/TCP,52365/TCP,6379/TCP,8080/TCP,8000/TCP   6m21s
# rayservice-sample-serve-svc                   ClusterIP   10.96.161.84    <none>        8000/TCP                                                  4m58s
```

After the Ray Serve applications are healthy and ready, KubeRay will create a head service and a serve service for the RayService custom resource - for example, `rayservice-sample-head-svc` and `rayservice-sample-serve-svc` in Step 4.4.
## Step 5: Verify the status of the Serve applications

```sh
# (1) Forward the dashboard port to localhost.
# (2) Check the Serve page in the Ray dashboard at http://localhost:8265/#/serve.
kubectl port-forward svc/rayservice-sample-head-svc --address 0.0.0.0 8265:8265
```

* Refer to [rayservice-troubleshooting.md](https://github.com/ray-project/kuberay/blob/master/docs/guidance/rayservice-troubleshooting.md#observability) for more details on RayService observability.
Below is a screenshot example of the Serve page in the Ray dashboard.
  ![Ray Serve Dashboard](../images/dashboard_serve.png)

## Step 6: Send requests to the Serve applications via the Kubernetes serve service

```sh
# Step 6.1: Run a curl Pod.
# If you already have a curl Pod, you can use `kubectl exec -it <curl-pod> -- sh` to access the Pod.
kubectl run curl --image=radial/busyboxplus:curl -i --tty

# Step 6.2: Send a request to the fruit stand app.
curl -X POST -H 'Content-Type: application/json' rayservice-sample-serve-svc:8000/fruit/ -d '["MANGO", 2]'
# [Expected output]: 6

# Step 6.3: Send a request to the calculator app.
curl -X POST -H 'Content-Type: application/json' rayservice-sample-serve-svc:8000/calc/ -d '["MUL", 3]'
# [Expected output]: "15 pizzas please!"
```

## Step 7: Clean up the Kubernetes cluster

```sh
# Delete the RayService.
kubectl delete -f ray_v1alpha1_rayservice.yaml

# Uninstall the KubeRay operator.
helm uninstall kuberay-operator

# Delete the curl Pod.
kubectl delete pod curl
```

## Next steps

* Check the [RayService](kuberay-rayservice) document for the full list of RayService features.
* Check the [RayService troubleshooting guide](kuberay-raysvc-troubleshoot) if you encounter any issues.
* Check [Examples](kuberay-examples) for more RayService examples.
The [MobileNet example](kuberay-mobilenet-rayservice-example) is a good example to start with because it does not require GPUs and is easy to run on a local machine.
