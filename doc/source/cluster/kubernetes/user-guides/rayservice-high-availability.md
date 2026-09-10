---
myst:
  html_meta:
    description: "Configure RayService high availability with GCS fault tolerance so Serve keeps handling requests when the head pod fails."
---

(kuberay-rayservice-ha)=
# RayService high availability

[RayService](kuberay-rayservice) provides high availability to ensure services continue serving requests when the Ray head Pod fails.

## Prerequisites

* Use RayService with KubeRay 1.3.0 or later.
* Enable GCS fault tolerance in the RayService.

## Quickstart

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

### Step 3: Install a RayService with GCS fault tolerance

```sh
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml
```

The [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml) file has several Kubernetes objects:

* Redis: Redis is necessary to make GCS fault tolerant. See {ref}`GCS fault tolerance <kuberay-gcs-ft>` for more details.
* RayService: This RayService custom resource includes a 3-node RayCluster and a simple [Ray Serve application](https://github.com/ray-project/test_dag).
* `ray-pod`: This Pod sends requests to the RayService.

### Step 4: Verify the Kubernetes Serve service

Check the output of the following command to verify that you successfully started the Kubernetes Serve service:
```sh
# Step 4.1: Wait until the RayService is ready to serve requests.
kubectl describe rayservices.ray.io rayservice-ha

# [Example output]
#   Conditions:
#     Last Transition Time:  2025-02-13T21:36:18Z
#     Message:               Number of serve endpoints is greater than 0
#     Observed Generation:   1
#     Reason:                NonZeroServeEndpoints
#     Status:                True
#     Type:                  Ready 

# Step 4.2: `rayservice-ha-serve-svc` should have 3 endpoints, including the Ray head and two Ray workers.
kubectl describe svc rayservice-ha-serve-svc

# [Example output]
# Endpoints:         10.244.0.29:8000,10.244.0.30:8000,10.244.0.32:8000
```

### Step 5: Verify the Serve applications

In the [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml) file, the `serveConfigV2` parameter specifies `num_replicas: 2` and `max_replicas_per_node: 1` for each Ray Serve deployment. In addition, the YAML sets the `rayStartParams` parameter to `num-cpus: "0"` to ensure that the system doesn't schedule any Ray Serve replicas on the Ray head Pod.

In total, each Ray Serve deployment has two replicas, and each Ray node can have at most one of those two Ray Serve replicas. Additionally, Ray Serve replicas can't schedule on the Ray head Pod. As a result, each worker node should have exactly one Ray Serve replica for each Ray Serve deployment.

For Ray Serve, the Ray head always has a HTTPProxyActor whether it has a Ray Serve replica or not. The Ray worker nodes only have HTTPProxyActors when they have Ray Serve replicas. Thus, the `rayservice-ha-serve-svc` service in the previous step has 3 endpoints.

```sh
# Port forward the Ray Dashboard.
kubectl port-forward svc/rayservice-ha-head-svc 8265:8265
# Visit ${YOUR_IP}:8265 in your browser for the Dashboard (e.g. 127.0.0.1:8265)
# Check:
# (1) Both head and worker nodes have HTTPProxyActors.
# (2) Only worker nodes have Ray Serve replicas.
# (3) Each worker node has one Ray Serve replica for each Ray Serve deployment.
```

### Step 6: Send requests to the RayService

```sh
# Log into the separate client Pod.
kubectl exec -it ray-pod -- bash

# Send requests to the RayService.
python3 samples/query.py

# This script sends the same request to the RayService consecutively, ensuring at most one in-flight request at a time.
# The request is equivalent to `curl -X POST -H 'Content-Type: application/json' localhost:8000/fruit/ -d '["PEAR", 12]'`.

# [Example output]
# req_index : 2197, num_fail: 0
# response: 12
# req_index : 2198, num_fail: 0
# response: 12
# req_index : 2199, num_fail: 0
```

### Step 7: Delete the Ray head Pod

```sh
# Step 7.1: Delete the Ray head Pod.
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl delete pod $HEAD_POD
```

In this example, `query.py` ensures that at most one request is in-flight at any given time. Furthermore, the Ray head Pod has doesn't have any Ray Serve replicas. Requests may fail only when a request is in the HTTPProxyActor on the Ray head Pod. Therefore, failures are highly unlikely to occur during the deletion and recovery of the Ray head Pod. You can implement retry logic in Ray scripts to handle the failures.

```sh
# [Expected output]: The `num_fail` is highly likely to be 0.
req_index : 32503, num_fail: 0
response: 12
req_index : 32504, num_fail: 0
response: 12
```

### Step 8: Cleanup

```sh
kind delete cluster
```

(kuberay-rayservice-ha-upgrades)=
## GCS fault tolerance and zero-downtime upgrades

GCS fault tolerance and zero-downtime upgrades work together with no extra configuration. Leave `gcsFaultToleranceOptions.externalStorageNamespace` unset, as [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml) does.

When you don't set `externalStorageNamespace`, KubeRay derives the Redis storage namespace from the unique identifier (`metadata.uid`) that Kubernetes assigns to the RayCluster. That single default gives you both behaviors:

* Within one RayCluster, that identifier doesn't change when the head Pod restarts or moves to another node, so the new head recovers the cluster metadata from Redis. Step 7 demonstrates this recovery.
* Across a zero-downtime upgrade, KubeRay creates a second RayCluster, and Kubernetes assigns it a different identifier, so the new cluster gets its own namespace and can't read the old cluster's metadata. The operator waits for the new cluster to become ready before it switches traffic.

Setting `externalStorageNamespace` yourself replaces both behaviors with one fixed value. A pinned namespace helps only when you delete a RayCluster and recreate it, and you want the replacement to adopt the previous metadata across the resulting change of identifier. The two clusters overlap during a RayService upgrade, so a shared namespace lets the new head read the old cluster's Serve metadata. The operator then treats those applications as the new cluster's own and can switch traffic before the new cluster is ready. See {ref}`Issue 10 <kuberay-raysvc-issue10>`.

A zero-downtime upgrade also replaces the worker Pods, because KubeRay creates a new RayCluster with its own head and worker Pods. Head Pod recovery keeps the existing worker Pods, but an upgrade doesn't.
