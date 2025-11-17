(kuberay-autoscaling)=

# KubeRay Autoscaling

This guide explains how to configure the Ray Autoscaler on Kubernetes.
The Ray Autoscaler is a Ray cluster process that automatically scales a cluster up and down based on resource demand.
The Autoscaler does this by adjusting the number of nodes (Ray Pods) in the cluster based on the resources required by tasks, actors, or placement groups.

The Autoscaler utilizes logical resource requests, indicated in `@ray.remote` and shown in `ray status`, not the physical machine utilization, to scale.
If you launch an actor, task, or placement group and resources are insufficient, the Autoscaler queues the request.
It adjusts the number of nodes to meet queue demands and removes idle nodes that have no tasks, actors, or objects over time.

```{admonition} When to use Autoscaling?
Autoscaling can reduce workload costs, but adds node launch overheads and can be tricky to configure.
We recommend starting with non-autoscaling clusters if you're new to Ray.
```

```{admonition} Ray Autoscaling V2 alpha with KubeRay (@ray 2.10.0)
With Ray 2.10, Ray Autoscaler V2 alpha is available with KubeRay. It has improvements on observability and stability. Please see the [section](kuberay-autoscaler-v2) for more details.
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
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.

### Step 3: Create a RayCluster custom resource with autoscaling enabled

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.autoscaler.yaml
```

### Step 4: Verify the Kubernetes cluster status

```bash
# Step 4.1: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                         READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head   2/2     Running   0          107s

# Step 4.2: Check the ConfigMap in the `default` namespace.
kubectl get configmaps

# [Example output]
# NAME                  DATA   AGE
# ray-example           2      21s
# ...
```

The RayCluster has one head Pod and zero worker Pods. The head Pod has two containers: a Ray head container and a Ray Autoscaler sidecar container.
Additionally, the [ray-cluster.autoscaler.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-cluster.autoscaler.yaml) includes a ConfigMap named `ray-example` that contains two Python scripts: `detached_actor.py` and `terminate_detached_actor.py`.

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
# raycluster-autoscaler-head                       2/2     Running   0          xxm
# raycluster-autoscaler-small-group-worker-yyyyy   1/1     Running   0          xxm

# Step 5.3: Create a detached actor which requires 1 CPU.
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/detached_actor.py actor2
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                             READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head                       2/2     Running   0          xxm
# raycluster-autoscaler-small-group-worker-yyyyy   1/1     Running   0          xxm
# raycluster-autoscaler-small-group-worker-zzzzz   1/1     Running   0          xxm

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
# raycluster-autoscaler-head                       2/2     Running   0          xxm
# raycluster-autoscaler-small-group-worker-zzzzz   1/1     Running   0          xxm

# Step 6.3: Terminate the detached actor "actor2".
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/terminate_detached_actor.py actor2

# Step 6.4: A worker Pod will be deleted after `idleTimeoutSeconds` (default 60s) seconds.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                         READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head   2/2     Running   0          xxm
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
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.autoscaler.yaml

# Uninstall the KubeRay operator
helm uninstall kuberay-operator

# Delete the kind cluster
kind delete cluster
```

(kuberay-autoscaling-config)=
## KubeRay Autoscaling Configurations

The [ray-cluster.autoscaler.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-cluster.autoscaler.yaml) used in the quickstart example contains detailed comments about the configuration options.
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

### 4. Set the `rayStartParams` and the resource limits for the Ray container

```{admonition} Resource limits are optional starting from Ray 2.41.0
Starting from Ray 2.41.0, the Ray Autoscaler can read resource specifications from `rayStartParams`, resource limits, or resource requests of the Ray container. You must specify at least one of these fields.
Earlier versions only support `rayStartParams` or resource limits, and don't recognize resource requests.
```

```{admonition} rayStartParams is optional if you're using an autoscaler image with Ray 2.45.0 or later.
`rayStartParams` is optional with RayCluster CRD from KubeRay 1.4.0 or later but required in earlier versions.
If you omit `rayStartParams` and want to use autoscaling, the autoscaling image must have Ray 2.45.0 or later.
```

The Ray Autoscaler reads the `rayStartParams` field or the Ray container's resource limits in the RayCluster custom resource specification to determine the Ray Pod's resource requirements.
The information regarding the number of CPUs is essential for the Ray Autoscaler to scale the cluster.
Therefore, without this information, the Ray Autoscaler reports an error and fails to start.
Take [ray-cluster.autoscaler.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-cluster.autoscaler.yaml) as an example below:

* If users set `num-cpus` in `rayStartParams`, Ray Autoscaler would work regardless of the resource limits on the container.
* If users don't set `rayStartParams`, the Ray container must have a specified CPU resource limit.

```yaml
headGroupSpec:
  rayStartParams:
    num-cpus: "0"
  template:
    spec:
      containers:
      - name: ray-head
        resources:
          # The Ray Autoscaler still functions if you comment out the `limits` field for the
          # head container, as users have already specified `num-cpus` in `rayStartParams`.
          limits:
            cpu: "1"
            memory: "2G"
          requests:
            cpu: "1"
            memory: "2G"
...
workerGroupSpecs:
- groupName: small-group
  template:
    spec:
      containers:
      - name: ray-worker
        resources:
          limits:
            # The Ray Autoscaler versions older than 2.41.0 will fail to start if the CPU resource limit for the worker
            # container is commented out because `rayStartParams` is empty.
            # The Ray Autoscaler starting from 2.41.0 will not fail but use the resource requests if the resource
            # limits are commented out and `rayStartParams` is empty.
            cpu: "1"
            memory: "1G"
          requests:
            cpu: "1"
            memory: "1G"
```

### 5. Autoscaler environment configuration

You can configure the Ray autoscaler using environment variables specified in the `env` or `envFrom` fields under the `autoscalerOptions` section of your RayCluster custom resource. These variables provide fine-grained control over how the autoscaler behaves internally.

For example, `AUTOSCALER_UPDATE_INTERVAL_S` determines how frequently the autoscaler checks the cluster status and decides whether to scale up or down.

For complete examples, see [ray-cluster.autoscaler.yaml](https://github.com/ray-project/kuberay/blob/099bf616c012975031ea9e5bbf7843af03e5f05b/ray-operator/config/samples/ray-cluster.autoscaler.yaml#L28-L33) and [ray-cluster.autoscaler-v2.yaml](https://github.com/ray-project/kuberay/blob/099bf616c012975031ea9e5bbf7843af03e5f05b/ray-operator/config/samples/ray-cluster.autoscaler-v2.yaml#L16_L21).

```yaml
autoscalerOptions:
  env:
    - name: AUTOSCALER_UPDATE_INTERVAL_S
      value: "5"
```

## Next steps

See [(Advanced) Understanding the Ray Autoscaler in the Context of Kubernetes](ray-k8s-autoscaler-comparison) for more details about the relationship between the Ray Autoscaler and Kubernetes autoscalers.

(kuberay-autoscaler-v2)=
### Autoscaler V2 with KubeRay

#### Prerequisites

* KubeRay v1.4.0 and the latest Ray version are the preferred setup for Autoscaler V2.

The release of Ray 2.10.0 introduces the alpha version of Ray Autoscaler V2 integrated with KubeRay, bringing enhancements in terms of observability and stability:


1. **Observability**: The Autoscaler V2 provides instance level tracing for each Ray worker's lifecycle, making it easier to debug and understand the Autoscaler behavior. It also reports the idle information about each node, including details on why nodes are idle or active:

```bash

> ray status -v

======== Autoscaler status: 2024-03-08 21:06:21.023751 ========
GCS request time: 0.003238s

Node status
---------------------------------------------------------------
Active:
 1 node_40f427230584b2d9c9f113d8db51d10eaf914aa9bf61f81dc7fabc64
Idle:
 1 node_2d5fd3d4337ba5b5a8c3106c572492abb9a8de2dee9da7f6c24c1346
Pending:
 (no pending nodes)
Recent failures:
 (no failures)

Resources
---------------------------------------------------------------
Total Usage:
 1.0/64.0 CPU
 0B/72.63GiB memory
 0B/33.53GiB object_store_memory

Pending Demands:
 (no resource demands)

Node: 40f427230584b2d9c9f113d8db51d10eaf914aa9bf61f81dc7fabc64
 Usage:
  1.0/32.0 CPU
  0B/33.58GiB memory
  0B/16.79GiB object_store_memory
 # New in autoscaler V2: activity information
 Activity:
  Busy workers on node.
  Resource: CPU currently in use.

Node: 2d5fd3d4337ba5b5a8c3106c572492abb9a8de2dee9da7f6c24c1346
 # New in autoscaler V2: idle information
 Idle: 107356 ms
 Usage:
  0.0/32.0 CPU
  0B/39.05GiB memory
  0B/16.74GiB object_store_memory
 Activity:
  (no activity)
```

2. **Stability**
Autoscaler V2 makes significant improvements to idle node handling. The V1 autoscaler could stop nodes that became active during termination processing, potentially failing tasks or actors. V2 uses Ray's graceful draining mechanism, which safely stops idle nodes without disrupting ongoing work.

[ray-cluster.autoscaler-v2.yaml](https://github.com/ray-project/kuberay/blob/v1.5.0/ray-operator/config/samples/ray-cluster.autoscaler-v2.yaml) is an example YAML file of a RayCluster with Autoscaler V2 enabled that works with the latest KubeRay version.

If you're using KubeRay >= 1.4.0, enable V2 by setting `RayCluster.spec.autoscalerOptions.version: v2`.

```yaml
spec:
  enableInTreeAutoscaling: true
  # Set .spec.autoscalerOptions.version: v2
  autoscalerOptions:
    version: v2
```

If you're using KubeRay < 1.4.0, enable V2 by setting the `RAY_enable_autoscaler_v2` environment variable
in the head and using `restartPolicy: Never` on head and all worker groups.

```yaml
spec:
  enableInTreeAutoscaling: true
  headGroupSpec:
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.X.Y
          env:
          # Set this environment variable
          - name: RAY_enable_autoscaler_v2
            value: "1"
        restartPolicy: Never # Prevent container restart to maintain Ray health.


  # Prevent Kubernetes from restarting Ray worker pod containers, enabling correct instance management by Ray.
  workerGroupSpecs:
  - replicas: 1
    template:
      spec:
        restartPolicy: Never
        ...
```
