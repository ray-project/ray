(serve-deploy-tutorial)=

# Deploying Ray Serve

This section should help you:

- understand how Ray Serve runs on a Ray cluster beyond the basics
- deploy and update your Serve application over time
- monitor your Serve application using the Ray Dashboard and logging

```{contents} Deploying Ray Serve
```

(ray-serve-instance-lifetime)=

## Lifetime of a Ray Serve Instance

Ray Serve instances run on top of Ray clusters and are started using {mod}`serve.start <ray.serve.start>`.
Once {mod}`serve.start <ray.serve.start>` has been called, further API calls can be used to create and update the deployments that will be used to serve your Python code (including ML models).
The Serve instance will be torn down when the script exits.

When running on a long-lived Ray cluster (e.g., one started using `ray start` and connected
to using `ray.init(address="auto")`, you can also deploy a Ray Serve instance as a long-running
service using `serve.start(detached=True)`. In this case, the Serve instance will continue to
run on the Ray cluster even after the script that calls it exits. If you want to run another script
to update the Serve instance, you can run another script that connects to the same Ray cluster and makes further API calls (e.g., to create, update, or delete a deployment). Note that there can only be one detached Serve instance on each Ray cluster.

:::{note}
All Serve actors– including the Serve controller, the HTTP proxies, and the deployment replicas– run in the `"serve"` namespace, even if the Ray driver namespace is different.
:::

If `serve.start()` is called again in a process in which there is already a running Serve instance, Serve will re-connect to the existing instance (regardless of whether the original instance was detached or not). To reconnect to a Serve instance that exists in the Ray cluster but not in the current process, connect to the cluster and run `serve.start()`.

## Deploying on a Single Node

While Ray Serve makes it easy to scale out on a multi-node Ray cluster, in some scenarios a single node may suit your needs.
There are two ways you can run Ray Serve on a single node, shown below.
In general, **Option 2 is recommended for most users** because it allows you to fully make use of Serve's ability to dynamically update running deployments.

1. Start Ray and deploy with Ray Serve all in a single Python file.

```python
import ray
from ray import serve
import time

# This will start Ray locally and start Serve on top of it.
serve.start()

@serve.deployment
def my_func(request):
  return "hello"

my_func.deploy()

# Serve will be shut down once the script exits, so keep it alive manually.
while True:
    time.sleep(5)
    print(serve.list_deployments())
```

2. First running `ray start --head` on the machine, then connecting to the running local Ray cluster using `ray.init(address="auto")` in your Serve script(s). You can run multiple scripts to update your deployments over time.

```bash
ray start --head # Start local Ray cluster.
serve start # Start Serve on the local Ray cluster.
```

```python
import ray
from ray import serve

# This will connect to the running Ray cluster.
ray.init(address="auto", namespace="serve")

@serve.deployment
def my_func(request):
  return "hello"

my_func.deploy()
```

## Deploying on Kubernetes

In order to deploy Ray Serve on Kubernetes, we need to do the following:

1. Start a Ray cluster on Kubernetes.
2. Expose the head node of the cluster as a [Service].
3. Start Ray Serve on the cluster.

There are multiple ways to start a Ray cluster on Kubernetes, see {ref}`ray-k8s-deploy` for more information.
Here, we will be using the [Ray Cluster Launcher](cluster-cloud) tool, which has support for Kubernetes as a backend.

The cluster launcher takes in a yaml config file that describes the cluster.
Here, we'll be using the [Kubernetes default config] with a few small modifications.
First, we need to make sure that the head node of the cluster, where Ray Serve will run its HTTP server, is exposed as a Kubernetes [Service].
There is already a default head node service defined in the `services` field of the config, so we just need to make sure that it's exposing the right port: 8000, which Ray Serve binds on by default.

```yaml
# Service that maps to the head node of the Ray cluster.
- apiVersion: v1
  kind: Service
  metadata:
      name: ray-head
  spec:
      # Must match the label in the head pod spec below.
      selector:
          component: ray-head
      ports:
          - protocol: TCP
            # Port that this service will listen on.
            port: 8000
            # Port that requests will be sent to in pods backing the service.
            targetPort: 8000
```

Then, we also need to make sure that the head node pod spec matches the selector defined here and exposes the same port:

```yaml
head_node:
  apiVersion: v1
  kind: Pod
  metadata:
    # Automatically generates a name for the pod with this prefix.
    generateName: ray-head-

    # Matches the selector in the service definition above.
    labels:
        component: ray-head

  spec:
    # ...
    containers:
    - name: ray-node
      # ...
      ports:
          - containerPort: 8000 # Ray Serve default port.
    # ...
```

The rest of the config remains unchanged for this example, though you may want to change the container image or the number of worker pods started by default when running your own deployment.
Now, we just need to start the cluster:

```shell
# Start the cluster.
$ ray up ray/python/ray/autoscaler/kubernetes/example-full.yaml

# Check the status of the service pointing to the head node. If configured
# properly, you should see the 'Endpoints' field populated with an IP
# address like below. If not, make sure the head node pod started
# successfully and the selector/labels match.
$ kubectl -n ray describe service ray-head
  Name:              ray-head
  Namespace:         ray
  Labels:            <none>
  Annotations:       <none>
  Selector:          component=ray-head
  Type:              ClusterIP
  IP:                10.100.188.203
  Port:              <unset>  8000/TCP
  TargetPort:        8000/TCP
  Endpoints:         192.168.73.98:8000
  Session Affinity:  None
  Events:            <none>
```

With the cluster now running, we can run a simple script to start Ray Serve and deploy a "hello world" deployment:

> ```python
> import ray
> from ray import serve
>
> # Connect to the running Ray cluster.
> ray.init(address="auto")
> # Bind on 0.0.0.0 to expose the HTTP server on external IPs.
> serve.start(detached=True, http_options={"host": "0.0.0.0"})
>
>
> @serve.deployment(route_prefix="/hello")
> def hello(request):
>     return "hello world"
>
> hello.deploy()
> ```

Save this script locally as `deploy.py` and run it on the head node using `ray submit`:

> ```shell
> $ ray submit ray/python/ray/autoscaler/kubernetes/example-full.yaml deploy.py
> ```

Now we can try querying the service by sending an HTTP request to the service from within the Kubernetes cluster.

> ```shell
> # Get a shell inside of the head node.
> $ ray attach ray/python/ray/autoscaler/kubernetes/example-full.yaml
>
> # Query the Ray Serve deployment. This can be run from anywhere in the
> # Kubernetes cluster.
> $ curl -X GET http://$RAY_HEAD_SERVICE_HOST:8000/hello
> hello world
> ```

In order to expose the Ray Serve deployment externally, we would need to deploy the Service we created here behind an [Ingress] or a [NodePort].
Please refer to the Kubernetes documentation for more information.

## Health Checking

By default, each actor making up a Serve deployment is health checked and restarted on failure.

:::{note}
User-defined health checks are experimental and may be subject to change before the interface is stabilized. If you have any feedback or run into any issues or unexpected behaviors, please file an issue on GitHub.
:::

You can customize this behavior to perform an application-level health check or to adjust the frequency/timeout.
To define a custom healthcheck, define a `check_health` method on your deployment class.
This method should take no arguments and return no result, raising an exception if the replica should be considered unhealthy.
You can also customize how frequently the health check is run and the timeout when a replica will be deemed unhealthy if it hasn't responded in the deployment options.

> ```python
> @serve.deployment(_health_check_period_s=10, _health_check_timeout_s=30)
> class MyDeployment:
>     def __init__(self, db_addr: str):
>         self._my_db_connection = connect_to_db(db_addr)
>
>     def __call__(self, request):
>         return self._do_something_cool()
>
>     # Will be called by Serve to check the health of the replica.
>     def check_health(self):
>         if not self._my_db_connection.is_connected():
>             # The specific type of exception is not important.
>             raise RuntimeError("uh-oh, DB connection is broken.")
> ```

:::{tip}
You can use the Serve CLI command `serve status` to get status info
about your live deployments. The CLI was included with Serve when you did
`pip install "ray[serve]"`. If you're checking your deployments on a
remote Ray cluster, make sure to include the Ray cluster's dashboard address
in the command: `serve status --address [dashboard_address]`.
:::

## Failure Recovery

Ray Serve is resilient to any component failures within the Ray cluster out of the box.
You can checkout the detail of how process and worker node failure handled at {ref}`serve-ft-detail`.
However, when the Ray head node goes down, you would need to recover the state by creating a new
Ray cluster and re-deploys all Serve deployments into that cluster.

:::{note}
Ray currently cannot survive head node failure and we recommend using application specific
failure recovery solutions. Although Ray is not currently highly available (HA), it is on
the long term roadmap and being actively worked on.
:::

Ray Serve added an experimental feature to help recovering the state.
This features enables Serve to write all your deployment configuration and code into a storage location.
Upon Ray cluster failure and restarts, you can simply call Serve to reconstruct the state.

Here is how to use it:

:::{warning}
The API is experimental and subject to change. We welcome you to test it out
and leave us feedback through github issues or discussion forum!
:::

You can use both the start argument and the CLI to specify it:

```python
serve.start(_checkpoint_path=...)
```

or

```shell
serve start --checkpoint-path ...
```

The checkpoint path argument accepts the following format:

- `file://local_file_path`
- `s3://bucket/path`
- `gs://bucket/path`
- `custom://importable.custom_python.Class/path`

While we have native support for on disk, AWS S3, and Google Cloud Storage (GCS), there is no reason we cannot support more.

In Kubernetes environment, we recommend using [Persistent Volumes] to create a disk and mount it into the Ray head node.
For example, you can provision Azure Disk, AWS Elastic Block Store, or GCP Persistent Disk using the K8s [Persistent Volumes] API.
Alternatively, you can also directly write to object store like S3.

You can easily try to plug into your own implementation using the `custom://` path and inherit the [KVStoreBase] class.
Feel free to open new github issues and contribute more storage backends!

[ingress]: https://kubernetes.io/docs/concepts/services-networking/ingress/
[kubernetes default config]: https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/kubernetes/example-full.yaml
[kvstorebase]: https://github.com/ray-project/ray/blob/master/python/ray/serve/storage/kv_store_base.py
[nodeport]: https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types
[persistent volumes]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/
[service]: https://kubernetes.io/docs/concepts/services-networking/service/
