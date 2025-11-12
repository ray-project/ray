(kuberay-gcs-ft)=
# GCS fault tolerance in KubeRay

Global Control Service (GCS) manages cluster-level metadata.
By default, the GCS lacks fault tolerance as it stores all data in-memory, and a failure can cause the entire Ray cluster to fail.
To make the GCS fault tolerant, you must have a high-availability Redis.
This way, in the event of a GCS restart, it retrieves all the data from the Redis instance and resumes its regular functions.

```{admonition} Fate-sharing without GCS fault tolerance
Without GCS fault tolerance, the Ray cluster, the GCS process, and the Ray head Pod are fate-sharing.
If the GCS process dies, the Ray head Pod dies as well after `RAY_gcs_rpc_server_reconnect_timeout_s` seconds.
If the Ray head Pod is restarted according to the Pod's `restartPolicy`, worker Pods attempt to reconnect to the new head Pod.
The worker Pods are terminated by the new head Pod; without GCS fault tolerance enabled, the cluster state is lost, and the worker Pods are perceived as "unknown workers" by the new head Pod.
This is adequate for most Ray applications; however, it is not ideal for Ray Serve, especially if high availability is crucial for your use cases.
Hence, we recommend enabling GCS fault tolerance on the RayService custom resource to ensure high availability.
See {ref}`Ray Serve end-to-end fault tolerance documentation <serve-e2e-ft-guide-gcs>` for more information.
```

```{seealso}
If you need fault tolerance for Redis as well, see {ref}`Tuning Redis for a
Persistent Fault Tolerant GCS <kuberay-gcs-persistent-ft>`.
```

## Use cases

* **Ray Serve**: The recommended configuration is enabling GCS fault tolerance on the RayService custom resource to ensure high availability.
See {ref}`Ray Serve end-to-end fault tolerance documentation <serve-e2e-ft-guide-gcs>` for more information.

* **Other workloads**: GCS fault tolerance isn't recommended and the compatibility isn't guaranteed.

## Prerequisites

* Ray 2.0.0+
* KubeRay 1.3.0+
* Redis: single shard Redis Cluster or Redis Sentinel, one or multiple replicas

## Quickstart

### Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

### Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository.

### Step 3: Install a RayCluster with GCS FT enabled

```sh
curl -LO https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-cluster.external-redis.yaml
kubectl apply -f ray-cluster.external-redis.yaml
```

### Step 4: Verify the Kubernetes cluster status

```sh
# Step 4.1: List all Pods in the `default` namespace.
# The expected output should be 4 Pods: 1 head, 1 worker, 1 KubeRay, and 1 Redis.
kubectl get pods

# [Example output]
# NAME                                                 READY   STATUS    RESTARTS   AGE
# kuberay-operator-6bc45dd644-ktbnh                    1/1     Running   0          3m4s
# raycluster-external-redis-head                       1/1     Running   0          2m41s
# raycluster-external-redis-small-group-worker-dwt98   1/1     Running   0          2m41s
# redis-6cf756c755-qljcv                               1/1     Running   0          2m41s

# Step 4.2: List all ConfigMaps in the `default` namespace.
kubectl get configmaps

# [Example output]
# NAME                  DATA   AGE
# kube-root-ca.crt      1      3m4s
# ray-example           2      5m45s
# redis-config          1      5m45s
```

The [ray-cluster.external-redis.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.external-redis.yaml) file defines Kubernetes resources for RayCluster, Redis, and ConfigMaps.
There are two ConfigMaps in this example: `ray-example` and `redis-config`.
The `ray-example` ConfigMap houses two Python scripts: `detached_actor.py` and `increment_counter.py`.

* `detached_actor.py` is a Python script that creates a detached actor with the name, `counter_actor`.
    ```python
    import ray

    @ray.remote(num_cpus=1)
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    ray.init(namespace="default_namespace")
    Counter.options(name="counter_actor", lifetime="detached").remote()
    ```

* `increment_counter.py` is a Python script that increments the counter.
    ```python
    import ray

    ray.init(namespace="default_namespace")
    counter = ray.get_actor("counter_actor")
    print(ray.get(counter.increment.remote()))
    ```

### Step 5: Create a detached actor

```sh
# Step 5.1: Create a detached actor with the name, `counter_actor`.
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/detached_actor.py

# 2025-04-18 02:51:25,359	INFO worker.py:1514 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
# 2025-04-18 02:51:25,361	INFO worker.py:1654 -- Connecting to existing Ray cluster at address: 10.244.0.8:6379...
# 2025-04-18 02:51:25,557	INFO worker.py:1832 -- Connected to Ray cluster. View the dashboard at 10.244.0.8:8265 

# Step 5.2: Increment the counter.
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/increment_counter.py

# 2025-04-18 02:51:29,069	INFO worker.py:1514 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
# 2025-04-18 02:51:29,072	INFO worker.py:1654 -- Connecting to existing Ray cluster at address: 10.244.0.8:6379...
# 2025-04-18 02:51:29,198	INFO worker.py:1832 -- Connected to Ray cluster. View the dashboard at 10.244.0.8:8265 
# 1
```

(kuberay-external-storage-namespace-example)=
### Step 6: Check the data in Redis

```sh
# Step 6.1: Check the RayCluster's UID.
kubectl get rayclusters.ray.io raycluster-external-redis -o=jsonpath='{.metadata.uid}'
# [Example output]: 864b004c-6305-42e3-ac46-adfa8eb6f752

# Step 6.2: Check the head Pod's environment variable `RAY_external_storage_namespace`.
kubectl get pods $HEAD_POD -o=jsonpath='{.spec.containers[0].env}' | jq
# [Example output]:
# [
#   {
#     "name": "RAY_external_storage_namespace",
#     "value": "864b004c-6305-42e3-ac46-adfa8eb6f752"
#   },
#   ...
# ]

# Step 6.3: Log into the Redis Pod.
# The password `5241590000000000` is defined in the `redis-config` ConfigMap.
# Step 6.4: Check the keys in Redis.
# Note: the schema changed in Ray 2.38.0. Previously we use a single HASH table,
# now we use multiple HASH tables with a common prefix.
export REDIS_POD=$(kubectl get pods --selector=app=redis -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -it $REDIS_POD -- env REDISCLI_AUTH="5241590000000000" redis-cli KEYS "*"

# [Example output]:
# 1) "RAY864b004c-6305-42e3-ac46-adfa8eb6f752@INTERNAL_CONFIG"
# 2) "RAY864b004c-6305-42e3-ac46-adfa8eb6f752@KV"
# 3) "RAY864b004c-6305-42e3-ac46-adfa8eb6f752@NODE"
# [Example output Before Ray 2.38.0]:
# 2) "864b004c-6305-42e3-ac46-adfa8eb6f752"
#

# Step 6.5: Check the value of the key.
kubectl exec -it $REDIS_POD -- env REDISCLI_AUTH="5241590000000000" redis-cli HGETALL RAY864b004c-6305-42e3-ac46-adfa8eb6f752@NODE
# Before Ray 2.38.0:
# HGETALL 864b004c-6305-42e3-ac46-adfa8eb6f752
```

In [ray-cluster.external-redis.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.external-redis.yaml), the `gcsFaultToleranceOptions.externalStorageNamespace` option isn't set for the RayCluster.
Therefore, KubeRay automatically injects the environment variable `RAY_external_storage_namespace` to all Ray Pods managed by the RayCluster with the RayCluster's UID as the external storage namespace by default.
See [this section](kuberay-external-storage-namespace) to learn more about the option.

### Step 7: Kill the GCS process in the head Pod

```sh
# Step 7.1: Check the `RAY_gcs_rpc_server_reconnect_timeout_s` environment variable in both the head Pod and worker Pod.
kubectl get pods $HEAD_POD -o=jsonpath='{.spec.containers[0].env}' | jq
# [Expected result]:
# No `RAY_gcs_rpc_server_reconnect_timeout_s` environment variable is set. Hence, the Ray head uses its default value of `60`.

export YOUR_WORKER_POD=$(kubectl get pods -l ray.io/group=small-group -o jsonpath='{.items[0].metadata.name}')
kubectl get pods $YOUR_WORKER_POD -o=jsonpath='{.spec.containers[0].env}' | jq
# [Expected result]:
# KubeRay injects the `RAY_gcs_rpc_server_reconnect_timeout_s` environment variable with the value `600` to the worker Pod.
# [
#   {
#     "name": "RAY_gcs_rpc_server_reconnect_timeout_s",
#     "value": "600"
#   },
#   ...
# ]

# Step 7.2: Kill the GCS process in the head Pod.
kubectl exec -it $HEAD_POD -- pkill gcs_server

# Step 7.3: The head Pod fails and restarts after `RAY_gcs_rpc_server_reconnect_timeout_s` (60) seconds.
# In addition, the worker Pod isn't terminated by the new head after reconnecting because GCS fault
# tolerance is enabled.
kubectl get pods -l=ray.io/is-ray-node=yes
# [Example output]:
# NAME                                                 READY   STATUS    RESTARTS      AGE
# raycluster-external-redis-head                       1/1     Running   1 (64s ago)   xxm
# raycluster-external-redis-worker-small-group-yyyyy   1/1     Running   0             xxm
```

In [ray-cluster.external-redis.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.external-redis.yaml), the `RAY_gcs_rpc_server_reconnect_timeout_s` environment variable isn't set in the specifications for either the head Pod or the worker Pod within the RayCluster.
Therefore, KubeRay automatically injects the `RAY_gcs_rpc_server_reconnect_timeout_s` environment variable with the value **600** to the worker Pod and uses the default value **60** for the head Pod.
The timeout value for worker Pods must be longer than the timeout value for the head Pod so that the worker Pods don't terminate before the head Pod restarts from a failure.

### Step 8: Access the detached actor again

```sh
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/increment_counter.py
# 2023-09-07 17:31:17,793 INFO worker.py:1313 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
# 2023-09-07 17:31:17,793 INFO worker.py:1431 -- Connecting to existing Ray cluster at address: 10.244.0.21:6379...
# 2023-09-07 17:31:17,875 INFO worker.py:1612 -- Connected to Ray cluster. View the dashboard at http://10.244.0.21:8265
# 2
```

```{admonition} The detached actor is always on the worker Pod in this example.
The head Pod's `rayStartParams` is set to `num-cpus: "0"`.
Hence, no tasks or actors will be scheduled on the head Pod.
```

With GCS fault tolerance enabled, you can still access the detached actor after the GCS process is dead and restarted.
Note that the fault tolerance doesn't persist the actor's state.
The reason why the result is 2 instead of 1 is that the detached actor is on the worker Pod which is always running.
On the other hand, if the head Pod hosts the detached actor, the `increment_counter.py` script yields a result of 1 in this step.

### Step 9: Remove the key stored in Redis when deleting RayCluster

```shell
# Step 9.1: Delete the RayCluster custom resource.
kubectl delete raycluster raycluster-external-redis

# Step 9.2: KubeRay operator deletes all Pods in the RayCluster.
# Step 9.3: KubeRay operator creates a Kubernetes Job to delete the Redis key after the head Pod is terminated.

# Step 9.4: Check whether the RayCluster has been deleted.
kubectl get raycluster
# [Expected output]: No resources found in default namespace.

# Step 9.5: Check Redis keys after the Kubernetes Job finishes.
export REDIS_POD=$(kubectl get pods --selector=app=redis -o custom-columns=POD:metadata.name --no-headers)
kubectl exec -i $REDIS_POD -- env REDISCLI_AUTH="5241590000000000" redis-cli KEYS "*"
# [Expected output]: (empty list or set)
```

In KubeRay v1.0.0, the KubeRay operator adds a Kubernetes finalizer to the RayCluster with GCS fault tolerance enabled to ensure Redis cleanup.
KubeRay only removes this finalizer after the Kubernetes Job successfully cleans up Redis.

* In other words, if the Kubernetes Job fails, the RayCluster won't be deleted. In that case, you should remove the finalizer and cleanup Redis manually.
  ```shell
  kubectl patch rayclusters.ray.io raycluster-external-redis --type json --patch='[ { "op": "remove", "path": "/metadata/finalizers" } ]'
  ```

Starting with KubeRay v1.1.0, KubeRay changes the Redis cleanup behavior from a mandatory to a best-effort basis.
KubeRay still removes the Kubernetes finalizer from the RayCluster if the Kubernetes Job fails, thereby unblocking the deletion of the RayCluster.

Users can turn off this by setting the feature gate value `ENABLE_GCS_FT_REDIS_CLEANUP`.
Refer to the [KubeRay GCS fault tolerance configurations](kuberay-redis-cleanup-gate) section for more details.

### Step 10: Delete the Kubernetes cluster

```sh
kind delete cluster
```

## KubeRay GCS fault tolerance configurations

The [ray-cluster.external-redis.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.external-redis.yaml) used in the quickstart example contains detailed comments about the configuration options.
***Read this section in conjunction with the YAML file.***

```{admonition} These configurations require KubeRay 1.3.0+
The following section uses the new `gcsFaultToleranceOptions` field introduced in KubeRay 1.3.0.
For the old GCS fault tolerance configurations, including the `ray.io/ft-enabled` annotation, please refer to [the old document](https://docs.ray.io/en/releases-2.42.1/cluster/kubernetes/user-guides/kuberay-gcs-ft.html).
```

### 1. Enable GCS fault tolerance

* **`gcsFaultToleranceOptions`**: Add `gcsFaultToleranceOptions` field to the RayCluster custom resource to enable GCS fault tolerance.
    ```yaml
    kind: RayCluster
    metadata:
    spec:
      gcsFaultToleranceOptions: # <- Add this field to enable GCS fault tolerance.
    ```

### 2. Connect to an external Redis

* **`redisAddress`**: Add `redisAddress` to the `gcsFaultToleranceOptions` field.
Use this option to specify the address for the Redis service, thus allowing the Ray head to connect to it.
In the [ray-cluster.external-redis.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.external-redis.yaml), the RayCluster custom resource uses the `redis` Kubernetes ClusterIP service name as the connection point to the Redis server. The ClusterIP service is also created by the YAML file.
    ```yaml
    kind: RayCluster
    metadata:
    spec:
      gcsFaultToleranceOptions:
        redisAddress: "redis:6379" # <- Add redis address here.
    ```

* **`redisPassword`**: Add `redisPassword` to the `gcsFaultToleranceOptions` field.
Use this option to specify the password for the Redis service, thus allowing the Ray head to connect to it.
In the [ray-cluster.external-redis.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.external-redis.yaml), the RayCluster custom resource loads the password from a Kubernetes secret.
    ```yaml
    kind: RayCluster
    metadata:
    spec:
      gcsFaultToleranceOptions:
        redisAddress: "redis:6379"
        redisPassword: # <- Add redis password from a Kubernetes secret.
          valueFrom:
            secretKeyRef:
              name: redis-password-secret
              key: password
    ```


(kuberay-external-storage-namespace)=
### 3. Use an external storage namespace

* **`externalStorageNamespace`** (**optional**): Add `externalStorageNamespace` to the `gcsFaultToleranceOptions` field.
KubeRay uses the value of this option to set the environment variable `RAY_external_storage_namespace` to all Ray Pods managed by the RayCluster.
In most cases, ***you don't need to set `externalStorageNamespace`*** because KubeRay automatically sets it to the UID of RayCluster.
Only modify this option if you fully understand the behaviors of the GCS fault tolerance and RayService to avoid [this issue](kuberay-raysvc-issue10).
Refer to [this section](kuberay-external-storage-namespace-example) in the earlier quickstart example for more details.
    ```yaml
    kind: RayCluster
    metadata:
    spec:
      gcsFaultToleranceOptions:
        externalStorageNamespace: "my-raycluster-storage" # <- Add this option to specify a storage namespace
    ```

(kuberay-redis-cleanup-gate)=
### 4. Turn off Redis cleanup

* `ENABLE_GCS_FT_REDIS_CLEANUP`: True by default. You can turn this feature off by setting the environment variable in the [KubeRay operator's Helm chart](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/values.yaml).

```{admonition} Key eviction setup on Redis
If you disable `ENABLE_GCS_FT_REDIS_CLEANUP` but want Redis to remove GCS metadata automatically,
set these two options in your `redis.conf` or in the command line options of your redis-server command [(example)](https://github.com/ray-project/ray/pull/40949#issuecomment-1799057691):

* `maxmemory=<your_memory_limit>`
* `maxmemory-policy=allkeys-lru`

These two options instruct Redis to delete the least recently used keys when it reaches the `maxmemory` limit.
See [Key eviction](https://redis.io/docs/reference/eviction/) from Redis for more information.

Note that Redis does this eviction and it doesn't guarantee that
Ray won't use the deleted keys.
```

## Next steps

* See {ref}`Ray Serve end-to-end fault tolerance documentation <serve-e2e-ft-guide-gcs>` for more information.
* See {ref}`Ray Core GCS fault tolerance documentation <fault-tolerance-gcs>` for more information.