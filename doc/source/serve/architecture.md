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
  you can also run a proxy on each node in the cluster via the `location` field of [`http_options`](core-apis).
- **Replicas**: Actors that actually execute the code in response to a
  request. For example, they may contain an instantiation of an ML model. Each
  replica processes individual requests from the HTTP proxy (these may be batched
  by the replica using `@serve.batch`, see the [batching](serve-performance-batching-requests) docs).

## Lifetime of a Request

When an HTTP request is sent to the HTTP proxy, the following happens:

1. The HTTP request is received and parsed.
2. The correct deployment associated with the HTTP URL path is looked up. The
  request is placed on a queue.
3. For each request in a deployment's queue, an available replica is looked up in round-robin fashion
  and the request is sent to it. If there are no available replicas (i.e. there
  are more than `max_concurrent_queries` requests outstanding at each replica), the request
  is left in the queue until a replica becomes available.

Each replica maintains a queue of requests and executes requests one at a time, possibly
using `asyncio` to process them concurrently. If the handler (the deployment function or the `__call__` method of the deployment class) is declared with `async def`, the replica will not wait for the
handler to run.  Otherwise, the replica will block until the handler returns.

When making a request via [ServeHandle](serve-handle-explainer) instead of HTTP, the request is placed on a queue in the ServeHandle, and we skip to step 3 above.

(serve-ft-detail)=

## Fault tolerance

Application errors like exceptions in your model evaluation code are caught and
wrapped. A 500 status code will be returned with the traceback information. The
replica will be able to continue to handle requests.

Machine errors and faults will be handled by Ray Serve as follows:

- When replica actors fail, the Controller actor will replace them with new ones.
- When the HTTP proxy actor fails, the Controller actor will restart it.
- When the Controller actor fails, Ray will restart it.
- When using the [KubeRay RayService](https://ray-project.github.io/kuberay/guidance/rayservice/), KubeRay will recover crashed nodes or a crashed cluster.  Cluster crashes can be avoided using the [GCS FT feature](https://ray-project.github.io/kuberay/guidance/gcs-ft/).
- If not using KubeRay, when the Ray cluster fails, Ray Serve cannot recover.

When a machine hosting any of the actors crashes, those actors will be automatically restarted on another
available machine. All data in the Controller (routing policies, deployment
configurations, etc) is checkpointed to the Ray Global Control Store (GCS) on the head node. Transient data in the
router and the replica (like network connections and internal request queues) will be lost for this kind of failure.
See [the end-to-end fault tolerance guide](serve-e2e-ft) for more details on how actor crashes are detected.

(serve-autoscaling-architecture)=

## Ray Serve Autoscaling

Ray Serve's autoscaling feature automatically increases or decreases a deployment's number of replicas based on its load.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling.svg)

- The Serve Autoscaler runs in the Serve Controller actor.
- Each ServeHandle and each replica periodically pushes its metrics to the autoscaler.
- For each deployment, the autoscaler periodically checks ServeHandle queues and in-flight queries on replicas to decide whether or not to scale the number of replicas.
- Each ServeHandle continuously polls the controller to check for new deployment replicas. Whenever new replicas are discovered, it will send any buffered or new queries to the replica until `max_concurrent_queries` is reached.  Queries are sent to replicas in round-robin fashion, subject to the constraint that no replica is handling more than `max_concurrent_queries` requests at a time.

:::{note}
When the controller dies, requests can still be sent via HTTP and ServeHandles, but autoscaling will be paused. When the controller recovers, the autoscaling will resume, but all previous metrics collected will be lost.
:::

## Ray Serve API Server

Ray Serve provides a [CLI](serve-cli) for managing your Ray Serve instance, as well as a [REST API](serve-rest-api).
Each node in your Ray cluster provides a Serve REST API server that can connect to Serve and respond to Serve REST requests.

## FAQ

### How does Serve ensure horizontal scalability and availability?

Serve can be configured to start one HTTP proxy actor per node via the `location` field of [`http_options`](core-apis). Each one will bind the same port. You
should be able to reach Serve and send requests to any models via any of the
servers.  You can use your own load balancer on top of Ray Serve.

This architecture ensures horizontal scalability for Serve. You can scale your HTTP ingress by adding more nodes and scale your model inference by increasing the number
of replicas via the `num_replicas` option of your deployment.

### How do ServeHandles work?

{mod}`ServeHandles <ray.serve.handle.RayServeHandle>` wrap a handle to a "router" on the
same node which routes requests to replicas for a deployment. When a
request is sent from one replica to another via the handle, the
requests go through the same data path as incoming HTTP requests. This enables
the same deployment selection and batching procedures to happen. ServeHandles are
often used to implement [model composition](serve-model-composition).

### What happens to large requests?

Serve utilizes Ray’s [shared memory object store](plasma-store) and in process memory
store. Small request objects are directly sent between actors via network
call. Larger request objects (100KiB+) are written to the object store and the replica can read them via zero-copy read.
