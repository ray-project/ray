---
myst:
  html_meta:
    description: "Write a custom Ray Serve request router, from a uniform router to a throughput-aware one, using the request routing utility mixins."
---

(custom-request-router-guide)=
# Use a custom algorithm for request routing

:::{warning}
This API is alpha and might change before it becomes stable.
:::

Different Ray Serve applications need different load-balancing logic. For example, when you serve LLMs, you might want a policy other than balancing the number of requests across replicas, such as balancing ongoing input tokens or KV cache utilization. [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst) is a Ray Serve abstraction for extending and customizing the load-balancing logic of each deployment.

This guide shows how to use the [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst) API to customize load balancing across the replicas of a deployment.


(simple-uniform-request-router)=
(define-simple-uniform-request-router)=
## Define a simple uniform request router
Create a file `custom_request_router.py` with the following code:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_uniform_request_router__
:end-before: __end_define_uniform_request_router__
:language: python
```
This code defines a simple uniform request router that routes each request to a random replica, which distributes the load evenly regardless of each replica's queue length or the request body. The router is a class that inherits from [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst). It implements the [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst) method, which returns a random replica for every incoming request. The return type is a list of lists of replicas, where each inner list represents a rank of replicas. The first rank is the most preferred, and the last rank is the least preferred. Ray Serve goes through the ranks in order and, within each rank, attempts to route the request to the replica with the shortest request queue, until a replica can process the request. If no replica can process the request, Ray Serve calls [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst) again after a backoff delay, and repeats until a replica can process the request.


:::{note}
This request router also implements [`on_request_routed`](../api/doc/ray.serve.request_router.RequestRouter.on_request_routed.rst), which you can use to update the state of the request router after a request is routed.
:::

(deploy-app-with-uniform-request-router)=
## Deploy an app with the uniform request router
To use a custom request router, pass the `request_router_class` argument to the [`deployment`](../api/doc/ray.serve.deployment_decorator.rst) decorator. You can pass `request_router_class` as the imported class or as a string with the import path of the class. The following example deploys a simple app that uses the uniform request router:

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_uniform_request_router__
:end-before: __end_deploy_app_with_uniform_request_router__
:language: python
```

As Ray Serve routes the request, the console prints both the "UniformRequestRouter routing request" and "on_request_routed callback is called!!" messages. The router sends each request to a random replica. To test this, send more requests and check that they're distributed roughly equally across the replicas.

:::{note}
Currently, the only way to configure the request router is to pass it as an argument to the deployment decorator. You can't change the request router for an existing deployment handle with a running router. If your use case requires reconfiguring the request router on a deployment handle, open a feature request on the [Ray GitHub repository](https://github.com/ray-project/ray/issues).
:::

(utility-mixin)=
## Utility mixins for request routing
Ray Serve provides utility mixins that extend the request router. Use them to implement common routing policies such as locality-aware routing, multiplexed model support, and first-in, first-out (FIFO) request routing.

- [`FIFOMixin`](../api/doc/ray.serve.request_router.FIFOMixin.rst): This mixin implements FIFO request routing. By default, the request router routes requests out of order, sending each request to the exact replica that [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst) assigned to it. This mixin is useful for routing algorithms that work independently of the request content, so requests can be routed as soon as possible in the order they arrive. When you include this mixin in your custom request router, the request matching algorithm routes requests in FIFO order. The mixin needs no additional flags and provides no additional helper methods.
- [`LocalityMixin`](../api/doc/ray.serve.request_router.LocalityMixin.rst): This mixin implements locality-aware request routing. It updates its internal state on replica updates to track which replicas are on the same node, in the same zone, or elsewhere. It offers the helpers [`apply_locality_routing`](../api/doc/ray.serve.request_router.LocalityMixin.apply_locality_routing.rst) and [`rank_replicas_via_locality`](../api/doc/ray.serve.request_router.LocalityMixin.rank_replicas_via_locality.rst) to route and rank replicas by their locality to the request, which can help reduce latency and improve performance.
- [`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst): When you use model multiplexing, you need to route requests based on which replicas already have a hot version of the model. This mixin updates its internal state on replica updates to track the models loaded on each replica and the size of each replica's model cache. It offers the helpers [`apply_multiplex_routing`](../api/doc/ray.serve.request_router.MultiplexMixin.apply_multiplex_routing.rst) and [`rank_replicas_via_multiplex`](../api/doc/ray.serve.request_router.MultiplexMixin.rank_replicas_via_multiplex.rst) to route and rank replicas by the multiplexed model ID of the request.


(throughput-aware-request-router)=
## Define a throughput-aware request router
The following router first prefers replicas that have the requested model loaded, then those with the fewest loaded models. Within each model rank, it prefers replicas on the same node, in the same availability zone, and elsewhere, in that order. It breaks ties using increasing reported throughput. Add this class to `custom_request_router.py`:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_throughput_aware_request_router__
:end-before: __end_define_throughput_aware_request_router__
:language: python
```

The router keeps every rank returned by the multiplexing and locality helpers. The first rank can be empty when no replica has loaded a model or no replica shares the router's node. Keeping the remaining ranks provides a fallback to other replicas.

The router returns each replica in its own rank so the base router tries them in throughput order. It doesn't call `select_available_replicas`, which skips replicas whose cached queue length has reached `max_ongoing_requests`. The cached value can be stale, so the base router probes those replicas before it moves to the next rank. If a preferred replica rejects the request, lower-ranked replicas remain available as fallbacks. This policy returns all fallback ranks in one call and sets `routing_context.should_backoff` to `True` for the next retry. If every replica is at capacity, the base router retries with backoff.

(deploy-app-with-throughput-aware-request-router)=
## Deploy an app with the throughput-aware request router
The following example deploys an app that uses the throughput-aware request router:

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_throughput_aware_request_router__
:end-before: __end_deploy_app_with_throughput_aware_request_router__
:language: python
```

As with the uniform request router, you set the custom request router in the `request_router_class` argument of the [`deployment`](../api/doc/ray.serve.deployment_decorator.rst) decorator. The Serve controller pulls statistics from the replicas of each deployment by calling `record_routing_stats`. The `request_routing_stats_period_s` and `request_routing_stats_timeout_s` arguments control how often the Serve controller pulls this information from each replica in its background thread, and the timeout for each pull. To customize these statistics, override `record_routing_stats` in the deployment class. The custom request router can then read the updated statistics from the `routing_stats` attribute of the running replicas and use them in its routing policy.


(round-robin-request-router)=
## Experimental: Use the round-robin request router

`RoundRobinRouter` cycles through replicas in round-robin fashion. If the chosen replica is at capacity, the router falls back to the next replica in order and wraps around the candidate list. Each router instance keeps its own cursor. With multiple routers, such as one per Ray Serve proxy, the fan-out is round-robin per router and approximately uniform across replicas in aggregate.

The router mixes in the following two classes:

- [`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst) to support model multiplexing.
- [`FIFOMixin`](../api/doc/ray.serve.request_router.FIFOMixin.rst) so queued requests are routed in arrival order.

For a multiplexed request, the router first round-robins among replicas that already host the requested model, using a cursor scoped to that model. If the router knows no host for the model, or has already tried those replicas, it falls back to the replicas with the fewest loaded models. After the matching timeout, it tries those replicas once more and then all replicas.

### When to use
Use the round-robin router when you want a predictable, even distribution across replicas and don't need load-aware or locality-aware decisions. It fits stateless workloads with roughly uniform per-request latency. Unlike the default power-of-two-choices router, the round-robin router doesn't compare replicas by their number of ongoing requests. It only skips a replica once that replica reaches `max_ongoing_requests`. As a result, requests can pile up behind a slow replica before the router falls back to the next one.

### Example
Reference the router by import path through [`RequestRouterConfig`](../api/doc/ray.serve.config.RequestRouterConfig.rst):

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_round_robin_router__
:end-before: __end_deploy_app_with_round_robin_router__
:language: python
```


(consistent-hash-request-router)=
## Experimental: Use the consistent-hash request router for session stickiness

`ConsistentHashRouter` pins each session to a specific replica using a consistent-hash ring with virtual nodes. The routing key is the session ID from the HTTP header that `RAY_SERVE_SESSION_ID_HEADER_KEY` names, which defaults to `x-session-id`, so the same session ID consistently maps to the same replica. Requests without that header fall back to a per-request internal ID and spread uniformly across replicas. If the chosen replica rejects the request, for example because of backpressure from its number of ongoing requests, the router walks up to `num_fallback_replicas` clockwise successors on the ring. If those are also at capacity, the router sleeps with exponential backoff and retries the same primary and fallbacks until one accepts. When the replica set changes, the ring is rebuilt, and only keys owned by added or removed replicas are reshuffled.

Two parameters are configurable through [`request_router_kwargs`](../api/doc/ray.serve.config.RequestRouterConfig.rst):

* `num_virtual_nodes`: The number of virtual nodes per replica on the ring. Defaults to `100`. Higher values spread sessions more evenly across replicas.
* `num_fallback_replicas`: The number of clockwise successors the router tries after the primary rejects. Defaults to `2`. Set it to `0` for strict affinity with no fallback.

### When to use
Use the consistent-hash router when requests carry session-scoped state worth keeping warm on a single replica, such as in-memory caches keyed by user or KV-cache reuse for LLM chat sessions. Skip it for stateless workloads. Affinity sacrifices queue-aware balancing, and a hot session can saturate its assigned replica while other replicas are idle. The router doesn't support model multiplexing, queue-depth signals, or locality signals, because mixing them in would break the determinism of consistent hashing and, with it, affinity.

### Example
Configure the router through [`RequestRouterConfig`](../api/doc/ray.serve.config.RequestRouterConfig.rst) and pass tuning parameters through `request_router_kwargs`:

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_consistent_hash_router__
:end-before: __end_deploy_app_with_consistent_hash_router__
:language: python
```

If your clients send the session identifier under a different header, such as `x-correlation-id`, point Ray Serve at it before the cluster starts:

```bash
export RAY_SERVE_SESSION_ID_HEADER_KEY=x-correlation-id
```


(capacity-queue-request-router)=
## Experimental: Define a centralized capacity queue request router

In the previous examples, each router bases its routing decisions on the state of the target replicas as the router sees it locally. This view is eventually consistent, not strongly consistent, because the Serve controller frequently broadcasts replica information to the router. Under high concurrency with multiple routers, this information can drift from reality and cause several routers to pick the same replica at the same time. The result is transient load imbalance, or rejections and retries, which can lower throughput for some applications. A centralized approach avoids this problem. A single actor tracks per-replica in-flight counts, and every router acquires a *capacity token* before it forwards a request. Each token guarantees that the target replica has room, which eliminates the rejection protocol entirely.

The following example implements such a routing policy. It has three pieces:

1. An importable `CapacityQueue` actor that tracks per-replica capacity and hands out tokens using a least-loaded selection strategy.
1. An importable `CapacityQueueRouter` custom request router that acquires a token before routing and releases it when the request completes. In a real application, you can have multiple instances of `CapacityQueueRouter`, each keeping its own view of the replicas' state. The centralized `CapacityQueue` actor exists to keep that local information synchronized with reality.
1. A deployment that ties them together using a [deployment actor](../api/doc/ray.serve.config.DeploymentActorConfig.rst) for the queue and a [`RequestRouterConfig`](../api/doc/ray.serve.config.RequestRouterConfig.rst) for the router.

(deploy-app-with-capacity-queue-router)=
### Deploy an app with the capacity queue router

The deployment wires the pieces together with a `DeploymentActorConfig` for the capacity queue and a `RequestRouterConfig` that points at the custom router:

```{literalinclude} ../doc_code/capacity_queue_request_router_app.py
:start-after: __begin_deploy_app_with_capacity_queue_router__
:end-before: __end_deploy_app_with_capacity_queue_router__
:language: python
```

When the app starts, the following happens:

1. The Serve controller creates the `CapacityQueue` deployment actor before any replicas start. `CapacityQueue` subscribes to replica updates through long poll.
1. As the controller starts replicas, it sends deployment-target updates. The queue's long-poll callback automatically registers each replica with its `max_ongoing_requests` capacity, and unregisters replicas that are removed during scale-down or crash recovery.
1. The `CapacityQueueRouter` running in each proxy discovers the singleton `CapacityQueue` deployment actor, acquires a token for every incoming request, and routes to the replica that the token identifies.
1. When the request completes, `CapacityQueueRouter.on_request_completed` fires and releases the token back to the queue.

Because the queue is a deployment actor, the controller handles its lifecycle automatically, including health checks, cleanup on app deletion, and versioning during rolling updates.

### Fault tolerance

The `CapacityQueueRouter` handles four kinds of failure:

- **Queue unavailable**: If the queue actor is dead, not yet discovered, or raises errors, the router retries with exponential backoff and falls back to power-of-two-choices after `MAX_FAULT_RETRIES` consecutive failures. Requests never raise exceptions because of queue issues.
- **Capacity exhausted**: When all replicas are at capacity, the router backs off and retries until capacity frees up.
- **Queue restart**: A restarted queue has no knowledge of pre-crash in-flight counts and might temporarily over-provision. This condition corrects itself. Replicas reject excess requests, and the router intentionally doesn't release rejected tokens, which ratchets up `in_flight` on the queue until it matches reality. If you configure `token_ttl_s`, it automatically reclaims any remaining leaked tokens.
- **Replica death**: The controller sends a long-poll update, the queue unregisters the dead replica, and the queue issues tokens only for live replicas.

### When to use
The centralized capacity queue request router might improve performance, particularly in a deployment with constrained supply, where `max_ongoing_requests` is `1` or `2`.

### Benchmark

The following benchmark measures `CapacityQueueRouter` under normal conditions and under a simulated fault.

#### Benchmark setup
- Deployment topology: Client -> `ParentDeployment` -> `ChildDeployment`. The benchmark applies the request router selection to both deployments, so it controls how the HTTP proxy selects parent replicas and how the parent's `DeploymentHandle` selects child replicas.
- Scale: small with 8 replicas, medium with 32, large with 128, and xlarge with 512.
- Workload: Replica processing latency follows an exponential distribution with a mean of 1 second, capped at 10 seconds.
- `max_ongoing_requests` is `2`.
- Load generation: Closed-loop load generation keeps replicas saturated at `max_ongoing_requests` concurrency.
- Warmup: 10 seconds. The benchmark discards all metrics from the warmup window.

#### Benchmark metrics
- Throughput: Requests per second, calculated as `num_requests / duration`.
- Utilization: The fraction of a replica's total processing capacity that actual work consumes during the experiment, calculated as `sum(replica_processing_latency_s) / (duration_s * max_ongoing_requests)`. For GPU deployments, utilization serves as a proxy for GPU utilization.
- Latency: The client-side end-to-end latency over the full round trip, from the client to `ParentDeployment`, then `ChildDeployment`, then back through `ParentDeployment` to the client.

#### Normal situation
Under normal conditions without faults, `CapacityQueueRouter` yields higher throughput, higher utilization, and lower latency.

```{image} ../images/capacity-queue-router-normal.png
:align: center
:width: 800px
```

#### Fault situation
The benchmark simulates a fault by killing the `CapacityQueue` router. After recovery, `CapacityQueue` converges toward its pre-fault performance.

```{image} ../images/capacity-queue-router-fault.png
:align: center
:width: 800px
```

:::{note}
If you see the following error when the `CapacityQueue` actor fails and routing decisions fall back to the power-of-two-choices router, set `RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S` to a higher value.

> Failed to get queue length from Replica(id='...', deployment='ParentDeployment', app='...') within 0.1s.

:::

## Gotchas and limitations

:::{warning}
When you provide a custom router, Ray Serve can fully support it as long as it's simple, self-contained Python code that relies only on the standard library. If the router is more complex, such as one that depends on other custom modules or packages, make sure you bundle those modules into the Docker image or environment. Ray Serve uses `cloudpickle` to serialize custom routers, and it doesn't vendor transitive dependencies. If your router inherits from a superclass in another module or imports custom packages, those modules and packages must exist in the target environment. Environment parity also matters. Differences in Python version, `cloudpickle` version, or library versions can affect deserialization.
:::

### Alternatives for complex routers

When your custom request router has complex dependencies, or you want better control over versioning and deployment, you have three alternatives:

- **Use built-in routers**: Consider using the routers that ship with Ray Serve. They're well tested, production ready, and guaranteed to work across different environments.
- **Contribute to Ray Serve**: If your router is general-purpose and might benefit others, consider contributing it to Ray Serve as a built-in router by opening a feature request or pull request on the [Ray GitHub repository](https://github.com/ray-project/ray/issues). The recommended location for the implementation is `python/ray/serve/_private/request_router/`.
- **Install dependencies in your environment**: Install the external dependencies in your Docker image or environment.
