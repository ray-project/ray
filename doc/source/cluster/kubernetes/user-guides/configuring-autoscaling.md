(kuberay-autoscaling)=

# KubeRay Autoscaling

This guide explains how to configure the Ray Autoscaler on Kubernetes.
The Ray Autoscaler is a Ray cluster process that automatically scales a cluster up and down based on resource demand.
The Autoscaler does this by adjusting the number of nodes (Ray Pods) in the cluster based on the resources required by tasks, actors, or placement groups.

The Autoscaler utilizes logical resource requests, indicated in `@ray.remote` and shown in `ray status`, not the physical machine utilization, to scale.
If you launch an actor, task, or placement group and resources are insufficient, the Autoscaler queues the request.
It adjusts the number of nodes to meet queue demands and removes idle nodes that have no tasks, actors, or objects over time.

<!-- TODO(ekl): probably should change the default kuberay examples to not use autoscaling -->
```{admonition} When to use Autoscaling?
Autoscaling can reduce workload costs, but adds node launch overheads and can be tricky to configure.
We recommend starting with non-autoscaling clusters if you're new to Ray.
```

## Overview

The following diagram illustrates the integration of the Ray Autoscaler with the KubeRay operator.
Although depicted as a separate entity for clarity, the Ray Autoscaler is actually a sidecar container within the Ray head Pod in the actual implementation.

```{eval-rst}
.. image:: ../images/AutoscalerOperator.svg
    :align: center
..
    Find the source document here (https://docs.google.com/drawings/d/1LdOg9JQuN5AOII-vDpSaFBsTeg0JGWcsbyNNLP1yovg/edit)
```

```{admonition} 3 levels of autoscaling in KubeRay
  * **Ray actor/task**: Some Ray libraries, like Ray Serve, can automatically adjust the number of Serve replicas (i.e., Ray actors) based on the incoming request volume.
  * **Ray node**: Ray Autoscaler automatically adjusts the number of Ray nodes (i.e., Ray Pods) based on the resource demand of Ray actors/tasks.
  * **Kubernetes node**: If the Kubernetes cluster lacks sufficient resources for the new Ray Pods that the Ray Autoscaler creates, the Kubernetes Autoscaler can provision a new Kubernetes node. ***You must configure the Kubernetes Autoscaler yourself.***
```

* The Autoscaler scales up the cluster through the following sequence of events:
  1. A user submits a Ray workload.
  2. The Ray head container aggregates the workload resource requirements and communicates them to the Ray Autoscaler sidecar.
  3. The Autoscaler decides to add a Ray worker Pod to satisfy the workload's resource requirement.
  4. The Autoscaler requests an additional worker Pod by incrementing the RayCluster CR's `replicas` field.
  5. The KubeRay operator creates a Ray worker Pod to match the new `replicas` specification.
  6. The Ray scheduler places the user's workload on the new worker Pod.

* The Autoscaler also scales down the cluster by removing idle worker Pods.
If it finds an idle worker Pod, it reduces the count in the RayCluster CR's `replicas` field and adds the identified Pods to the CR's `workersToDelete` field.
Then, the KubeRay operator deletes the Pods in the `workersToDelete` field.

## Quickstart

### Step 1: Create a Kubernetes cluster with Kind

```bash
kind create cluster --image=kindest/node:v1.23.0
```

### Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.

### Step 3: Create a RayCluster custom resource with autoscaling enabled

```bash
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.autoscaler.yaml
kubectl apply -f ray-cluster.autoscaler.yaml
```

### Step 4: Verify the Kubernetes cluster status

```bash
# Step 4.1: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                               READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head-6zc2t   2/2     Running   0          107s

# Step 4.2: Check the ConfigMap in the `default` namespace.
kubectl get configmaps

# [Example output]
# NAME                  DATA   AGE
# ray-example           2      21s
# ...
```

The RayCluster has one head Pod and zero worker Pods. The head Pod has two containers: a Ray head container and a Ray Autoscaler sidecar container.
Additionally, the [ray-cluster.autoscaler.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.autoscaler.yaml) includes a ConfigMap named `ray-example` that houses two Python scripts: `detached_actor.py` and `terminate_detached_actor`.py.

* `detached_actor.py` is a Python script that creates a detached actor which requires 1 CPU.
  ```py
  import ray
  import sys

  @ray.remote(num_cpus=1)
  class Actor:
    pass

  ray.init(namespace="default_namespace")
  Actor.options(name=sys.argv[1], lifetime="detached").remote()
  ```

* `terminate_detached_actor.py` is a Python script that terminates a detached actor.
  ```py
  import ray
  import sys

  ray.init(namespace="default_namespace")
  detached_actor = ray.get_actor(sys.argv[1])
  ray.kill(detached_actor)
  ```

### Step 5: Trigger RayCluster scale-up by creating detached actors

```bash
# Step 5.1: Create a detached actor "actor1" which requires 1 CPU.
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/detached_actor.py actor1

# Step 5.2: The Ray Autoscaler creates a new worker Pod.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                             READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head-xxxxx                 2/2     Running   0          xxm
# raycluster-autoscaler-worker-small-group-yyyyy   1/1     Running   0          xxm

# Step 5.3: Create a detached actor which requires 1 CPU.
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/detached_actor.py actor2
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                             READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head-xxxxx                 2/2     Running   0          xxm
# raycluster-autoscaler-worker-small-group-yyyyy   1/1     Running   0          xxm
# raycluster-autoscaler-worker-small-group-zzzzz   1/1     Running   0          xxm

# Step 5.4: List all actors in the Ray cluster.
kubectl exec -it $HEAD_POD -- ray list actors


# ======= List: 2023-09-06 13:26:49.228594 ========
# Stats:
# ------------------------------
# Total: 2

# Table:
# ------------------------------
#     ACTOR_ID  CLASS_NAME    STATE    JOB_ID    NAME    ...
#  0  xxxxxxxx  Actor         ALIVE    02000000  actor1  ...
#  1  xxxxxxxx  Actor         ALIVE    03000000  actor2  ...
```

The Ray Autoscaler generates a new worker Pod for each new detached actor.
This is because the `rayStartParams` field in the Ray head specifies `num-cpus: "0"`, preventing the Ray scheduler from scheduling any Ray actors or tasks on the Ray head Pod.
In addition, each Ray worker Pod has a capacity of 1 CPU, so the Autoscaler creates a new worker Pod to satisfy the resource requirement of the detached actor which requires 1 CPU.

* Using detached actors isn't necessary to trigger cluster scale-up.
Normal actors and tasks can also initiate it.
[Detached actors](actor-lifetimes) remain persistent even after the job's driver process exits, which is why the Autoscaler doesn't scale down the cluster automatically when the `detached_actor.py` process exits, making it more convenient for this tutorial.

* In this RayCluster custom resource, each Ray worker Pod possesses only 1 logical CPU from the perspective of the Ray Autoscaler.
Therefore, if you create a detached actor with `@ray.remote(num_cpus=2)`, the Autoscaler doesn't initiate the creation of a new worker Pod because the capacity of the existing Pod is limited to 1 CPU.

* (Advanced) The Ray Autoscaler also offers a [Python SDK](ref-autoscaler-sdk), enabling advanced users, like Ray maintainers, to request resources directly from the Autoscaler. Generally, most users don't need to use the SDK.

### Step 6: Trigger RayCluster scale-down by terminating detached actors

```bash
# Step 6.1: Terminate the detached actor "actor1".
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/terminate_detached_actor.py actor1

# Step 6.2: A worker Pod will be deleted after `idleTimeoutSeconds` (default 60s) seconds.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                             READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head-xxxxx                 2/2     Running   0          xxm
# raycluster-autoscaler-worker-small-group-zzzzz   1/1     Running   0          xxm

# Step 6.3: Terminate the detached actor "actor1".
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/terminate_detached_actor.py actor2

# Step 6.4: A worker Pod will be deleted after `idleTimeoutSeconds` (default 60s) seconds.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                             READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head-xxxxx                 2/2     Running   0          xxm
```

### Step 7: Ray Autoscaler observability

```bash
# Method 1: "ray status"
kubectl exec $HEAD_POD -it -c ray-head -- ray status

# [Example output]:
# ======== Autoscaler status: 2023-09-06 13:42:46.372683 ========
# Node status
# ---------------------------------------------------------------
# Healthy:
#  1 head-group
# Pending:
#  (no pending nodes)
# Recent failures:
#  (no failures)

# Resources
# ---------------------------------------------------------------
# Usage:
#  0B/1.86GiB memory
#  0B/514.69MiB object_store_memory

# Demands:
#  (no resource demands)

# Method 2: "kubectl logs"
kubectl logs $HEAD_POD -c autoscaler | tail -n 20

# [Example output]:
# 2023-09-06 13:43:22,029 INFO autoscaler.py:421 --
# ======== Autoscaler status: 2023-09-06 13:43:22.028870 ========
# Node status
# ---------------------------------------------------------------
# Healthy:
#  1 head-group
# Pending:
#  (no pending nodes)
# Recent failures:
#  (no failures)

# Resources
# ---------------------------------------------------------------
# Usage:
#  0B/1.86GiB memory
#  0B/514.69MiB object_store_memory

# Demands:
#  (no resource demands)
# 2023-09-06 13:43:22,029 INFO autoscaler.py:464 -- The autoscaler took 0.036 seconds to complete the update iteration.
```

### Step 8: Clean up the Kubernetes cluster

```bash
# Delete RayCluster and ConfigMap
kubectl delete -f ray-cluster.autoscaler.yaml

# Uninstall the KubeRay operator
helm uninstall kuberay-operator
```

(kuberay-autoscaling-config)=
## KubeRay Autoscaling Configurations

The [ray-cluster.autoscaler.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.autoscaler.yaml) used in the quickstart example contains detailed comments about the configuration options.
***It's recommended to read this section in conjunction with the YAML file.***

### 1. Enabling autoscaling

* **`enableInTreeAutoscaling`**: By setting `enableInTreeAutoscaling: true`, the KubeRay operator automatically configures an autoscaling sidecar container for the Ray head Pod.
* **`minReplicas` / `maxReplicas` / `replicas`**: 
Set the `minReplicas` and `maxReplicas` fields to define the range for `replicas` in an autoscaling `workerGroup`.
Typically, you would initialize both `replicas` and `minReplicas` with the same value during the deployment of an autoscaling cluster.
Subsequently, the Ray Autoscaler adjusts the `replicas` field as it adds or removes Pods from the cluster.

### 2. Scale-up and scale-down speed

If necessary, you can regulate the pace of adding or removing nodes from the cluster.
For applications with numerous short-lived tasks, considering a more conservative approach to adjusting the upscaling and downscaling speeds might be beneficial.

Utilize the `RayCluster` CR's `autoscalerOptions` field to accomplish this. This field encompasses the following sub-fields:

* **`upscalingMode`**: This controls the rate of scale-up process. The valid values are:
  - `Conservative`: Upscaling is rate-limited; the number of pending worker Pods is at most the number of worker pods connected to the Ray cluster.
  - `Default`: Upscaling isn't rate-limited.
  - `Aggressive`: An alias for Default; upscaling isn't rate-limited.

* **`idleTimeoutSeconds`** (default 60s):
This denotes the waiting time in seconds before scaling down an idle worker pod.
A worker node is idle when it has no active tasks, actors, or referenced objects, either stored in-memory or spilled to disk.

### 3. Autoscaler sidecar container

The `autoscalerOptions` field also provides options for configuring the Autoscaler container. Usually, it's not necessary to specify these options.

* **`resources`**:
The `resources` sub-field of `autoscalerOptions` sets optional resource overrides for the Autoscaler sidecar container.
These overrides should be specified in the standard [container resource
spec format](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/Pod-v1/#resources).
The default values are indicated below:
  ```yaml
  resources:
    limits:
      cpu: "500m"
      memory: "512Mi"
    requests:
      cpu: "500m"
      memory: "512Mi"
  ```

* **`image`**:
This field overrides the Autoscaler container image.
The container uses the same **image** as the Ray container by default. 

* **`imagePullPolicy`**:
This field overrides the Autoscaler container's image pull policy.
The default is `IfNotPresent`.

* **`env`** and **`envFrom`**:
These fields specify Autoscaler container environment variables.
These fields should be formatted following the [Kubernetes API](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/Pod-v1/#environment-variables)
for container environment variables.

## Next steps

See [(Advanced) Understanding the Ray Autoscaler in the Context of Kubernetes](ray-k8s-autoscaler-comparison) for more details about the relationship between the Ray Autoscaler and Kubernetes autoscalers.
