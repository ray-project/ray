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

In Ray 2.0, KubeRay added **experimental support** for [Global Control Store (GCS) fault tolerance](https://ray-project.github.io/kuberay/guidance/gcs-ft/#ray-gcs-fault-tolerancegcs-ft-experimental), preventing the Ray cluster from crashing if the head node goes down.
While the head node is recovering, Serve applications can still handle traffic via worker nodes but cannot be updated or recover from other failures (e.g. actors or worker nodes crashing).
Once the GCS is recovered, the cluster will return to normal behavior.

You can enable GCS fault tolerance on KubeRay by adding an external Redis server and modifying your `RayService` Kubernetes object.

Below, we explain how to do each of these.

#### Step 1: Add external Redis server

GCS fault tolerance requires an external Redis database. You can choose to host your own Redis database, or you can use one through a third-party vendor. We recommend using a highly-available Redis database for resiliency.

**For development puposes**, you can also host a small Redis database on the same Kubernetes cluster as your Ray cluster. For example, you can add a 1-node Redis cluster by prepending these three Redis objects to your Kubernetes YAML:

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
    port 6379
    bind 0.0.0.0
    protected-mode no
    requirepass 5241590000000000
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

In the example above, the Redis deployment name (`redis`) is the host within the Kubernetes cluster, and the Redis port is `6379`. The example is compatible with last section's [example config](one-node-redis-example).

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
:language: yaml
```
::::

You can follow along using your own Kubernetes cluster. Make sure it has at least 4 CPUs and 6 GB of memory to run the working example. First, install the [KubeRay operator](serve-installing-kuberay-operator):

```console
$ kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.3.0&timeout=90s"
$ kubectl get deployments -n ray-system
NAME                READY   UP-TO-DATE   AVAILABLE   AGE
kuberay-operator    1/1     1            1           13s

$ kubectl get pods -n ray-system
NAME                                 READY   STATUS    RESTARTS   AGE
kuberay-operator-68c75b5d5f-m8xd7    1/1     Running   0          42s
```

Then, [deploy the Serve application](serve-deploy-app-on-kuberay) above:

```console
$ kubectl apply -f config.yaml
```

### Worker node failure

When a worker node fails, any actors running on the node also crash. Serve detects that the failed node's actors have crashed, and it attempts to relaunch the actors on the remaining nodes. Meanwhile, KubeRay detects that the node crashed, and it attempts to start a new node to replace it. Once KubeRay starts the new node, Serve can launch any pending actors on the new node as well.

We can simulate a worker node failure in our working example. First, let's take a look at the nodes and pods running in our Kubernetes cluster:

```console
$ kubectl get nodes

NAME                                        STATUS   ROLES    AGE     VERSION
gke-serve-demo-default-pool-ed597cce-nvm2   Ready    <none>   3d22h   v1.22.12-gke.1200
gke-serve-demo-default-pool-ed597cce-m888   Ready    <none>   3d22h   v1.22.12-gke.1200
gke-serve-demo-default-pool-ed597cce-pu2q   Ready    <none>   3d22h   v1.22.12-gke.1200

$ kubectl get pods -o wide

NAME                                                      READY   STATUS    RESTARTS        AGE    IP           NODE                                        NOMINATED NODE   READINESS GATES
ervice-sample-raycluster-thwmr-worker-small-group-bdv6q   1/1     Running   0               3m3s   10.68.2.62   gke-serve-demo-default-pool-ed597cce-nvm2   <none>           <none>
ervice-sample-raycluster-thwmr-worker-small-group-pztzk   1/1     Running   0               3m3s   10.68.2.61   gke-serve-demo-default-pool-ed597cce-m888   <none>           <none>
rayservice-sample-raycluster-thwmr-head-28mdh             1/1     Running   1 (2m55s ago)   3m3s   10.68.0.45   gke-serve-demo-default-pool-ed597cce-pu2q   <none>           <none>
redis-75c8b8b65d-4qgfz                                    1/1     Running   0               3m3s   10.68.2.60   gke-serve-demo-default-pool-ed597cce-nvm2   <none>           <none>
```

Open a separate terminal window and port-forward to one of the worker nodes:

```console
$ kubectl port-forward ervice-sample-raycluster-thwmr-worker-small-group-bdv6q 8000
Forwarding from 127.0.0.1:8000 -> 8000
Forwarding from [::1]:8000 -> 8000
```

While the `port-forward` is running, you can query the application in another terminal window:

```console
$ curl localhost:8000
418
```

The output is the process ID of the deployment replica that handled the request. The application launches 6 deployment replicas, so if you run the query multiple times, you should see different process IDs:

```console
$ curl localhost:8000
418
$ curl localhost:8000
256
$ curl localhost:8000
385
```

Now you can simulate worker failues. You have two options: kill a worker pod or kill a worker node. Let's start with the worker pod. Make sure to kill the pod that you're **not** port-forwarding to, so you can continue querying the living worker while the other one relaunches.

```console
$ kubectl delete pod ervice-sample-raycluster-thwmr-worker-small-group-pztzk
pod "ervice-sample-raycluster-thwmr-worker-small-group-pztzk" deleted

$ curl localhost:8000
6318
```

While the pod crashes and recovers, the live pod can continue serving traffic!

You can similarly kill a worker node and see that the other nodes can continue serving traffic:

```console
$ kubectl get pods -o wide

NAME                                                      READY   STATUS    RESTARTS      AGE     IP           NODE                                        NOMINATED NODE   READINESS GATES
ervice-sample-raycluster-thwmr-worker-small-group-bdv6q   1/1     Running   0             65m     10.68.2.62   gke-serve-demo-default-pool-ed597cce-nvm2   <none>           <none>
ervice-sample-raycluster-thwmr-worker-small-group-mznwq   1/1     Running   0             5m46s   10.68.1.3    gke-serve-demo-default-pool-ed597cce-m888   <none>           <none>
rayservice-sample-raycluster-thwmr-head-28mdh             1/1     Running   1 (65m ago)   65m     10.68.0.45   gke-serve-demo-default-pool-ed597cce-pu2q   <none>           <none>
redis-75c8b8b65d-4qgfz                                    1/1     Running   0             65m     10.68.2.60   gke-serve-demo-default-pool-ed597cce-nvm2   <none>           <none>

$ kubectl delete node gke-serve-demo-default-pool-ed597cce-m888
node "gke-serve-demo-default-pool-ed597cce-m888" deleted

$ curl localhost:8000
6318
```

### Head node failure

### Serve controller failure

### Deployment replica failure

### HTTPProxy failure

[KubeRay]: https://ray-project.github.io/kuberay/
[external storage namespace]: https://ray-project.github.io/kuberay/guidance/gcs-ft/#external-storage-namespace