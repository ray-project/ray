(serve-architecture)=

# Architecture

In this section, we explore Serve's key architectural concepts and components. It will offer insight and overview into:
- the role of each component in Serve and how they work
- the different types of actors that make up a Serve application

% Figure source: https://docs.google.com/drawings/d/1jSuBN5dkSj2s9-0eGzlU_ldsRa3TsswQUZM-cMQ29a0/edit?usp=sharing

```{image} architecture-2.0.svg
:align: center
:width: 600px
```

(serve-architecture-high-level-view)=
## High-Level View

Serve runs on Ray and utilizes [Ray actors](actor-guide).

There are three kinds of actors that are created to make up a Serve instance:

- **Controller**: A global actor unique to each Serve instance that manages
  the control plane. The Controller is responsible for creating, updating, and
  destroying other actors. Serve API calls like creating or getting a deployment
  make remote calls to the Controller.
- **HTTP Proxy**: By default there is one HTTP proxy actor on the head node. This actor runs a [Uvicorn](https://www.uvicorn.org/) HTTP
  server that accepts incoming requests, forwards them to replicas, and
  responds once they are completed.  For scalability and high availability,
  you can also run a proxy on each node in the cluster via the `proxy_location` field inside [`serve.start()`](core-apis) or [the config file](serve-in-production-config-file).
- **gRPC Proxy**: If Serve is started with valid `port` and `grpc_servicer_functions`,
  then the gRPC proxy is started alongside with the HTTP proxy. This Actor runs a
  [grpcio](https://grpc.github.io/grpc/python/) server. The gRPC server that accepts
  incoming requests, forwards them to replicas, and responds once they are completed.
- **Replicas**: Actors that actually execute the code in response to a
  request. For example, they may contain an instantiation of an ML model. Each
  replica processes individual requests from the proxy. The replica may batch the requests
  using `@serve.batch`. See the [batching](serve-performance-batching-requests) docs.

## Lifetime of a request

When an HTTP or gRPC request is sent to the corresponding HTTP or gRPC proxy, the following happens:

1. The request is received and parsed.
2. Ray Serve looks up the correct deployment associated with the HTTP URL path or
  application name metadata. Serve places the request in a queue.
3. For each request in a deployment's queue, an available replica is looked up
  and the request is sent to it. If no replicas are available (that is, more
  than `max_ongoing_requests` requests are outstanding at each replica), the request
  is left in the queue until a replica becomes available.

Each replica maintains a queue of requests and executes requests one at a time, possibly
using `asyncio` to process them concurrently. If the handler (the deployment function or the `__call__` method of the deployment class) is declared with `async def`, the replica will not wait for the
handler to run.  Otherwise, the replica blocks until the handler returns.

When making a request via a [DeploymentHandle](serve-key-concepts-deployment-handle) instead of HTTP or gRPC for [model composition](serve-model-composition), the request is placed on a queue in the `DeploymentHandle`, and we skip to step 3 above.

(serve-ft-detail)=

## Fault tolerance

Application errors like exceptions in your model evaluation code are caught and
wrapped. A 500 status code will be returned with the traceback information. The
replica will be able to continue to handle requests.

Machine errors and faults are handled by Ray Serve as follows:

- When replica Actors fail, the Controller Actor replaces them with new ones.
- When the proxy Actor fails, the Controller Actor restarts it.
- When the Controller Actor fails, Ray restarts it.
- When using the [KubeRay RayService](kuberay-rayservice-quickstart), KubeRay recovers crashed nodes or a crashed cluster. You can avoid cluster crashes by using the [GCS FT feature](kuberay-gcs-ft).
- If you aren't using KubeRay, when the Ray cluster fails, Ray Serve cannot recover.

When a machine hosting any of the actors crashes, those actors are automatically restarted on another
available machine. All data in the Controller (routing policies, deployment
configurations, etc) is checkpointed to the Ray Global Control Store (GCS) on the head node. Transient data in the
router and the replica (like network connections and internal request queues) will be lost for this kind of failure.
See [the end-to-end fault tolerance guide](serve-e2e-ft) for more details on how actor crashes are detected.

(serve-autoscaling-architecture)=

## Ray Serve Autoscaling

Ray Serve's autoscaling feature automatically increases or decreases a deployment's number of replicas based on its load.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling.svg)

- The Serve Autoscaler runs in the Serve Controller actor.
- Each `DeploymentHandle` and each replica periodically pushes its metrics to the autoscaler.
- For each deployment, the autoscaler periodically checks `DeploymentHandle` queues and in-flight queries on replicas to decide whether or not to scale the number of replicas.
- Each `DeploymentHandle` continuously polls the controller to check for new deployment replicas. Whenever new replicas are discovered, it sends any buffered or new queries to the replica until `max_ongoing_requests` is reached.  Queries are sent to replicas in round-robin fashion, subject to the constraint that no replica is handling more than `max_ongoing_requests` requests at a time.

:::{note}
When the controller dies, requests can still be sent via HTTP, gRPC and `DeploymentHandle`, but autoscaling is paused. When the controller recovers, the autoscaling resumes, but all previous metrics collected are lost.
:::

## Ray Serve API Server

Ray Serve provides a [CLI](serve-cli) for managing your Ray Serve instance, as well as a [REST API](serve-rest-api).
Each node in your Ray cluster provides a Serve REST API server that can connect to Serve and respond to Serve REST requests.

## FAQ

### How does Serve ensure horizontal scalability and availability?

You can configure Serve to start one proxy Actor per node with the `proxy_location` field inside [`serve.start()`](core-apis) or [the config file](serve-in-production-config-file). Each proxy binds to the same port. You
should be able to reach Serve and send requests to any models with any of the
servers.  You can use your own load balancer on top of Ray Serve.

This architecture ensures horizontal scalability for Serve. You can scale your HTTP and gRPC ingress by adding more nodes. You can also scale your model inference by increasing the number
of replicas via the `num_replicas` option of your deployment.

### How do DeploymentHandles work?

{mod}`DeploymentHandles <ray.serve.handle.DeploymentHandle>` wrap a handle to a "router" on the
same node which routes requests to replicas for a deployment. When a
request is sent from one replica to another via the handle, the
requests go through the same data path as incoming HTTP or gRPC requests. This enables
the same deployment selection and batching procedures to happen. DeploymentHandles are
often used to implement [model composition](serve-model-composition).

### What happens to large requests?

Serve utilizes Ray’s [shared memory object store](plasma-store) and in process memory
store. Small request objects are directly sent between actors via network
call. Larger request objects (100KiB+) are written to the object store and the replica can read them via zero-copy read.
