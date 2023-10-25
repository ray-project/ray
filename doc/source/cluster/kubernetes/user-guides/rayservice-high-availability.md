(kuberay-rayservice-ha)=
# RayService high-availability

[RayService](kuberay-rayservice) provides high-availability so that the service can still serve requests when the Ray head Pod fails.

## Prerequisites

* KubeRay 1.0.0 or later
* Enable GCS fault tolerance in the RayService.

## Quickstart

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.23.0
```

### Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator by Helm repository.

### Step 3: Install a RayService with GCS fault tolerance enabled

```sh
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.high-availability.yaml
kubectl apply -f ray-service.high-availability.yaml
```

The [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/v1.0.0/ray-operator/config/samples/ray-service.high-availability.yaml) file has several Kubernetes objects:

* Redis: Redis is for GCS fault tolerance. See {ref}`GCS fault tolerance <kuberay-gcs-ft>` for more details.
* RayService: This RayService custom resource includes a 3-node RayCluster and a simple [Ray Serve application](https://github.com/ray-project/test_dag).
* Ray Pod: Sends requests to the RayService.

### Step 4: Verify the Kubernetes serve service

```sh
# Step 4.1: The K8s service `rayservice-ha-serve-svc` will be created after the Serve applications are ready.
kubectl describe svc rayservice-ha-serve-svc

# Step 4.2: `rayservice-ha-serve-svc` should have 3 endpoints, including the Ray head and two Ray workers.
# Endpoints:         10.244.0.29:8000,10.244.0.30:8000,10.244.0.32:8000
```

### Step 5: Verify the Serve applications

In [ray-service.high-availability.yaml](https://raw.githubusercontent.com/ray-project/kuberay/v1.0.0/ray-operator/config/samples/ray-service.high-availability.yaml), the `serveConfigV2` specifies `num_replicas: 2` and `max_replicas_per_node: 1` for each Ray Serve deployment.
In addition, the YAML sets `num-cpus: "0"` in `rayStartParams` to ensure the system doesn't schedule any Ray Serve replicas on the Ray head Pod.

In conclusion, each Ray Serve deployment has two replicas, and each Ray node can have at most one identical Ray Serve replica. Additionally, Ray Serve replicas can't schedule on the Ray head Pod. As a result, each worker node should have one Ray Serve replica for each Ray Serve deployment.

For Ray Serve, Ray head always has a HTTPProxyActor no matter whether it has a Ray Serve replica or not.
Ray workers only have HTTPProxyActors when they have Ray Serve replicas.
Thus, there are 3 endpoints for the `rayservice-ha-serve-svc` service in the previous step.

```sh
# Port forward the Ray dashboard
kubectl port-forward --address 0.0.0.0 svc/rayservice-ha-head-svc 8265:8265
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

In this example, `query.py` ensures there's at most one in-flight request at any given time.
Furthermore, there are no Ray Serve replicas on the Ray head Pod.
The request may only fail when a request is in the HTTPProxyActor on the Ray head Pod.
Therefore, it's highly probable that no failures occur during the deletion and recovery of the Ray head Pod.
Users can implement their own retry logic in their Ray scripts to handle the failures.

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
