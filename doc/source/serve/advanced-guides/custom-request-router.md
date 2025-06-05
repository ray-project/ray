(custom-request-router-guide)=
# Use custom algorithm for request routing

This page provides an overview of using the [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst)
API to achieve custom load balancing across replicas of a Ray Serve deployment. It
covers the following:
- Define a simple uniform request router for load balancing.
- Deploy an app with the uniform request router.
- Use utility mixins for request routing.
- Define a complex throughput-aware request router.
- Deploy an app with the throughput-aware request router.

## `RequestRouter` overview

The [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst) API
is an abstraction in Ray Serve that allow you to extend and customize
load-balancing logic for each deployment. For
example, in serving LLMs you might want to have a different policy than balancing the
number of requests across replicas such as balancing ongoing input tokens or balancing
kv-cache utilization.



(utility-mixin)=
## Utility mixins for request routing
You can use utility mixins to extend the
request router to implement common routing policies for
locality-aware routing, multiplexed model support, and first in, first out (FIFO) request routing.

- [`FIFOMixin`](../api/doc/ray.serve.request_router.FIFOMixin.rst): This mixin implements FIFO
  request routing. The default behavior for the request router is to route requests to
  the exact replica assigned by the request passed to the [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst).
  Use this mixin when you need to route requests as soon as possible in the order
  received.
- [`LocalityMixin`](../api/doc/ray.serve.request_router.LocalityMixin.rst): This mixin implements locality-aware
  request routing. It updates the internal state between replica updates to track
  the location of replicas across nodes and zones. Use the
  helpers [`apply_locality_routing`](../api/doc/ray.serve.request_router.LocalityMixin.apply_locality_routing.rst)
  and [`rank_replicas_via_locality`](../api/doc/ray.serve.request_router.LocalityMixin.rank_replicas_via_locality.rst) to route and
  ranks replicas based on their locality to the request for
  reduced latency and improved performance.
- [`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst): This mixin
  routes requests using awareness of hot versions of
  the model on replicas. The internal state tracks the model loaded on each replica and
  the size of the model cache on each replica. Use the
  helpers [`apply_multiplex_routing`](../api/doc/ray.serve.request_router.MultiplexMixin.apply_multiplex_routing.rst)
  and [`rank_replicas_via_multiplex`](../api/doc/ray.serve.request_router.MultiplexMixin.rank_replicas_via_multiplex.rst) to route
  and ranks replicas based on the multiplexed model ID of the request.

(simple-uniform-request-router)=
## Example: Uniform request router



### Step 1: Define the request router

Create a file `custom_request_router.py` with the following code:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_uniform_request_router__
:end-before: __end_define_uniform_request_router__
:language: python
```
This code defines a simple uniform request router that routes requests through a random replica
to distribute the load evenly regardless of the queue length of each replica or the body
of the request. The code defined the router as a class that inherits from
[`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst). It
implements the [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst)
method, which returns the random replica for all incoming requests. The returned type
is a list of lists of replicas, where each inner list represents a rank of replicas.
The first rank is the most preferred and the last rank is the least preferred.


:::{note}
This request router also implements [`on_request_routed`](../api/doc/ray.serve.request_router.RequestRouter.on_request_routed.rst)
which can help you update the state of the request router after a request is routed.
:::

### Step 2: Deploy an app with the router

(deploy-app-with-uniform-request-router)=
## Deploy an app with the uniform request router
To use a custom request router, you need to pass the `request_router_class` argument to
the [`deployment`](../api/doc/ray.serve.deployment_decorator.rst)
decorator. You can either pass the `request_router_class` argument using the
imported class or as the string of import path to the class. The following code deploys a simple app
that uses the uniform request router defined in the `custom_request_router.py` file:

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_uniform_request_router__
:end-before: __end_deploy_app_with_uniform_request_router__
:language: python
```

### Step 3: Send a request to the app

Send a request to the application to observe the behavior of the request router. The following is an example
request for our application:

```bash
# Please write an example request here
```

The router prints the message "UniformRequestRouter routing request" as it receives the request.
The "Called on_request_routed callback" message prints when the requesting routing completes. This router
randomly routes requests to one of the replicas, which you can confirm by
sending many requests and reviewing the distribution of replica assignment.

:::{note}
You must configure the request router by passing it as an argument to
the deployment decorator. This means that you can't change the request router for an
existing deployment handle with a running router. 

If you have a usecase where
you need to reconfigure a request router on the deployment handle, open a feature
request on the [Ray GitHub repository](https://github.com/ray-project/ray/issues)
:::



(throughput-aware-request-router)=
## Example: Throughput-aware request router

A fully featured request router can be more complex and should take into account the
multiplexed model, locality, the request queue length on each replica, and using custom
statistics like throughput to decide which replica to route the request to. 

### Step 1: Define the throuphput-aware request router

The
following class defines a throughput-aware request router that routes requests to the
replica with these factors in mind. Add the following code into the
`custom_request_router.py` file:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_throughput_aware_request_router__
:end-before: __end_define_throughput_aware_request_router__
:language: python
```

This request router inherits from [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst),
as well as [`FIFOMixin`](../api/doc/ray.serve.request_router.FIFOMixin.rst) for FIFO
request routing, [`LocalityMixin`](../api/doc/ray.serve.request_router.LocalityMixin.rst)
for locality-aware request routing, and
[`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst)
for multiplexed model support. It implements
[`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst)
to take the highest ranked replicas from [`rank_replicas_via_multiplex`](../api/doc/ray.serve.request_router.MultiplexMixin.rank_replicas_via_multiplex.rst)
and [`rank_replicas_via_locality`](../api/doc/ray.serve.request_router.LocalityMixin.rank_replicas_via_locality.rst)
and uses the [`select_available_replicas`](../api/doc/ray.serve.request_router.RequestRouter.select_available_replicas.rst)
helper to filter out replicas that have reached their maximum request queue length.
Finally, it takes the replicas with the minimum throughput and returns the top one.

(deploy-app-with-throughput-aware-request-router)=
## Step 2: Deploy an app with the throughput-aware request router
To use the throughput-aware request router, you can deploy an app like this:

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_throughput_aware_request_router__
:end-before: __end_deploy_app_with_throughput_aware_request_router__
:language: python
```

Similar to the uniform request router, the custom request router can be defined in the
`request_router_class` argument of the [`deployment`](../api/doc/ray.serve.deployment_decorator.rst)
decorator. The Serve controller pulls statistics from the replica of each deployment by
calling record_routing_stats. The `request_routing_stats_period_s` and
`request_routing_stats_timeout_s` arguments control the frequency and timeout time of
the serve controller pulling information from each replica in its background thread.
You can customize the emission of these statistics by overriding `record_routing_stats`
in the definition of the deployment class. The custom request router can then get the
updated routing stats by looking up the `routing_stats` attribute of the running
replicas and use it in the routing policy.
