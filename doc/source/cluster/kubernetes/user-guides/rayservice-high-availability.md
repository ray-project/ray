(kuberay-rayservice-ha)=
# RayService high availability

[RayService](kuberay-rayservice) provides high availability (HA) for services to continue serving requests when the Ray head Pod fails.

## Prerequisites

* Use RayService with KubeRay 1.0.0 or later.
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
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml
kubectl apply -f ray-service.high-availability.yaml
```

The [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml) file has several Kubernetes objects:

* Redis: Redis is required to make GCS fault tolerant. See {ref}`GCS fault tolerance <kuberay-gcs-ft>` for more details.
* RayService: This RayService custom resource includes a 3-node RayCluster and a simple [Ray Serve application](https://github.com/ray-project/test_dag).
* Ray Pod: This Pod sends requests to the RayService.

### Step 4: Verify the Kubernetes Serve service

Check the output of the following command to verify that you successfully started the Kubernetes Serve service:
```sh
# Step 4.1: KubeRay creates the K8s service `rayservice-ha-serve-svc` after the Ray Serve applications are ready.
kubectl describe svc rayservice-ha-serve-svc

# Step 4.2: `rayservice-ha-serve-svc` should have 3 endpoints, including the Ray head and two Ray workers.
# Endpoints:         10.244.0.29:8000,10.244.0.30:8000,10.244.0.32:8000
```

### Step 5: Verify the Serve applications

In the [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml) file, the `serveConfigV2` parameter specifies `num_replicas: 2` and `max_replicas_per_node: 1` for each Ray Serve deployment.
In addition, the YAML sets the `rayStartParams` parameter to `num-cpus: "0"` to ensure that the system doesn't schedule any Ray Serve replicas on the Ray head Pod.

In total, each Ray Serve deployment has two replicas, and each Ray node can have at most one of those two Ray Serve replicas. Additionally, Ray Serve replicas can't schedule on the Ray head Pod. As a result, each worker node should have exactly one Ray Serve replica for each Ray Serve deployment.

For Ray Serve, the Ray head always has a HTTPProxyActor whether it has a Ray Serve replica or not.
The Ray worker nodes only have HTTPProxyActors when they have Ray Serve replicas.
Thus, the `rayservice-ha-serve-svc` service in the previous step has 3 endpoints.

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
# Log into the separate Ray Pod.
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

In this example, `query.py` ensures that at most one request is in-flight at any given time.
Furthermore, the Ray head Pod has doesn't have any Ray Serve replicas.
Requests may fail only when a request is in the HTTPProxyActor on the Ray head Pod.
Therefore, failures are highly unlikely to occur during the deletion and recovery of the Ray head Pod.
You can implement retry logic in Ray scripts to handle the failures.

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
