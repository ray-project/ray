(serve-e2e-ft)=
# Adding End-to-End Fault Tolerance

This section helps you:

* provide additional fault tolerance for your Serve application
* understand Serve's recovery procedures
* simulate system errors in your Serve application

:::{admonition} Relevant Guides
:class: seealso
This section discusses concepts from:
* Serve's [architecture guide](serve-architecture)
* Serve's [Kubernetes production guide](serve-in-production-kubernetes)
:::

(serve-e2e-ft-guide)=
## Guide: end-to-end fault tolerance for your Serve app

Serve provides some [fault tolerance](serve-ft-detail) features out of the box. You can provide end-to-end fault tolerance by tuning these features and running Serve on top of [KubeRay].

### Replica health-checking

By default, the Serve controller periodically health-checks each Serve deployment replica and restarts it on failure.

You can define custom application-level health-checks and adjust their frequency and timeout.
To define a custom health-check, add a `check_health` method to your deployment class.
This method should take no arguments and return no result, and it should raise an exception if the replica should be considered unhealthy.
The Serve controller logs the raised exception if the health-check fails.
You can also use the deployment options to customize how frequently the health-check is run and the timeout after which a replica is marked unhealthy.

```{literalinclude} ../doc_code/fault_tolerance/replica_health_check.py
:start-after: __health_check_start__
:end-before: __health_check_end__
:language: python
```

### Worker node recovery

:::{admonition} KubeRay Required
:class: caution, dropdown
You **must** deploy your Serve application with [KubeRay] to use this feature.

See Serve's [Kubernetes production guide](serve-in-production-kubernetes) to learn how you can deploy your app with KubeRay.
:::

By default, Serve can recover from certain failures, such as unhealthy actors. When [Serve runs on Kubernetes](serve-in-production-kubernetes) with [KubeRay], it can also recover from some cluster-level failures, such as dead worker or head nodes.

When a worker node fails, the actors running on it also fail. Serve detects that the actors have failed, and it attempts to respawn the actors on the remaining, healthy nodes. Meanwhile, KubeRay detects that the node itself has failed, so it brings up a new healthy node to replace it. Serve can then respawn any pending actors on that node as well. The deployment replicas running on healthy nodes can continue serving traffic throughout the recovery period.

(serve-e2e-ft-guide-gcs)=
### Head node recovery: Ray GCS fault tolerance

:::{admonition} KubeRay Required
:class: caution, dropdown
You **must** deploy your Serve application with [KubeRay] to use this feature.

See Serve's [Kubernetes production guide](serve-in-production-kubernetes) to learn how you can deploy your app with KubeRay.
:::

In this section, you'll learn how to add fault tolerance to Ray's Global Control Store (GCS), which allows your Serve application to serve traffic even when the head node crashes.

By default the Ray head node is a single point of failure: if it crashes, the entire Ray cluster crashes and must be restarted. When running on Kubernetes, the `RayService` controller health-checks the Ray cluster and restarts it if this occurs, but this introduces some downtime.

In Ray 2.0, KubeRay has added experimental support for [Global Control Store (GCS) fault tolerance](https://ray-project.github.io/kuberay/guidance/gcs-ft/#ray-gcs-fault-tolerancegcs-ft-experimental), preventing the Ray cluster from crashing if the head node goes down.
While the head node is recovering, Serve applications can still handle traffic but cannot be updated or recover from other failures (e.g. actors or worker nodes crashing).
Once the GCS is recovered, the cluster will return to normal behavior.

You can enable GCS fault tolerance on KubeRay by adding an external Redis server and modifying your `RayService` Kubernetes object.

Below, we explain how to do each of these.

#### Step 1: Add external Redis server

GCS fault tolerance requires an external Redis database. You can choose to host your own Redis database, or you can use one through a third-party vendor. We recommend using a highly-available Redis database for resiliency.

**For development puposes**, you can also host a Redis database on the same Kubernetes cluster as your Ray cluster. For example, you can add a 1-node Redis cluster by prepending these three Redis objects to your Kubernetes YAML:

(one-node-redis-example)=
```YAML
kind: ConfigMap
apiVersion: v1
metadata:
  name: redis-config
  labels:
    app: redis
data:
  redis.conf: |-
    dir /data
    port 6379
    bind 0.0.0.0
    appendonly yes
    protected-mode no
    pidfile /data/redis-6379.pid
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  labels:
    app: redis
spec:
  type: ClusterIP
  ports:
    - name: redis
      port: 6379
  selector:
    app: redis
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  labels:
    app: redis
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:5.0.8
          command:
            - "sh"
            - "-c"
            - "redis-server /usr/local/etc/redis/redis.conf"
          ports:
            - containerPort: 6379
          volumeMounts:
            - name: config
              mountPath: /usr/local/etc/redis/redis.conf
              subPath: redis.conf
      volumes:
        - name: config
          configMap:
            name: redis-config
---
```

**This configuration is NOT production-ready**, but it is useful for development and testing. When you move to production, it's highly recommended that you replace this 1-node Redis cluster with a highly-available Redis cluster.

#### Step 2: Add Redis info to RayService

After adding the Redis objects, you also need to modify the `RayService` configuration.

First, you need to update your `RayService` metadata's annotations:

::::{tabbed} Vanilla Config
```yaml
...
apiVersion: ray.io/v1alpha1
kind: RayService
metadata:
  name: rayservice-sample
spec:
...
```
::::

::::{tabbed} Fault Tolerant Config
:selected:
```yaml
...
apiVersion: ray.io/v1alpha1
kind: RayService
metadata:
  name: rayservice-sample
  annotations:
    ray.io/ft-enabled: "true"
    ray.io/external-storage-namespace: "my-raycluster-storage-namespace"
spec:
...
```
::::

The annotations are:
* `ray.io/ft-enabled` (REQUIRED): Enables GCS fault tolerance when true
* `ray.io/external-storage-namespace` (OPTIONAL): Sets the [external storage namespace]

Next, you need to add the `RAY_REDIS_ADDRESS` environment variable to the `headGroupSpec`:

::::{tabbed} Vanilla Config
```yaml
apiVersion: ray.io/v1alpha1
kind: RayService
metadata:
    ...
spec:
    ...
    rayClusterConfig:
        headGroupSpec:
            ...
            template:
                ...
                spec:
                    ...
                    env:
                        ...
```
::::

::::{tabbed} Fault Tolerant Config
:selected:
```yaml
apiVersion: ray.io/v1alpha1
kind: RayService
metadata:
    ...
spec:
    ...
    rayClusterConfig:
        headGroupSpec:
            ...
            template:
                ...
                spec:
                    ...
                    env:
                        ...
                        - name: RAY_REDIS_ADDRESS
                          value: redis:6379
```
::::

`RAY_REDIS_ADDRESS`'s value should be your Redis database's `redis://` address. It should contain your Redis database's host and port. An [example Redis address](redis://user:secret@localhost:6379/0?foo=bar&qux=baz) is `redis://user:secret@localhost:6379/0?foo=bar&qux=baz`.

In the example above, the Redis deployment name (`redis`) is its host within the Kubernetes cluster, and the Redis port is `6379`. The example is compatible with last section's [example config](one-node-redis-example).

After you apply the Redis objects along with your updated `RayService`, your Ray cluster can recover from head node crashes without restarting all the workers!

:::{seealso}
Check out the KubeRay guide on [GCS fault tolerance](https://ray-project.github.io/kuberay/guidance/gcs-ft/#ray-gcs-fault-tolerancegcs-ft-experimental) to learn more about how Serve leverages the external Redis cluster to provide head node fault tolerance.
:::

(serve-e2e-ft-behavior)=
## Serve's recovery procedures

This section explains how Serve recovers from system failures. It uses the following Serve application and config as a working example.

::::{tabbed} Python Code
```{literalinclude} ../doc_code/fault_tolerance/sleepy_pid.py
:start-after: __start__
:end-before: __end__
:language: python
```
::::

::::{tabbed} Kubernetes Config
```{literalinclude} ../doc_code/fault_tolerance/k8s_config.yaml
:start-after: __start__
:end-before: __end__
:language: yaml
```
::::

### Worker node failure

### Head node failure

### Serve controller failure

### Deployment replica failure

### HTTPProxy failure

[KubeRay]: https://ray-project.github.io/kuberay/
[external storage namespace]: https://ray-project.github.io/kuberay/guidance/gcs-ft/#external-storage-namespace