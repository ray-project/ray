(custom-request-router-guide)=
# Use Custom Algorithm for Request Routing

:::{warning}
This API is in alpha and may change before becoming stable.
:::

Different Ray serve applications demand different logics for load balancing. For
example, in serving LLMs you might want to have a different policy than balancing
number of requests across replicas: e.g. balancing ongoing input tokens, balancing
kv-cache utilization, etc. [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst)
is an abstraction in Ray Serve that allows extension and customization of
load-balancing logic for each deployment.

This guide shows how to use [`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst)
API to achieve custom load balancing across replicas of a given deployment. It will
cover the following:
- Define a simple uniform request router for load balancing
- Deploy an app with the uniform request router
- Utility mixins for request routing
- Define a complex throughput-aware request router
- Deploy an app with the throughput-aware request router


(simple-uniform-request-router)=
## Define simple uniform request router
Create a file `custom_request_router.py` with the following code:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_uniform_request_router__
:end-before: __end_define_uniform_request_router__
:language: python
```
This code defines a simple uniform request router that routes requests a random replica
to distribute the load evenly regardless of the queue length of each replica or the body
of the request. The router is defined as a class that inherits from
[`RequestRouter`](../api/doc/ray.serve.request_router.RequestRouter.rst). It implements the [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst)
method, which returns the random replica for all incoming requests. The returned type
is a list of lists of replicas, where each inner list represents a rank of replicas.
The first rank is the most preferred and the last rank is the least preferred. The
request will be attempted to be routed to the replica with the shortest request queue in
each set of the rank in order until a replica is able to process the request. If none of
the replicas are able to process the request, [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst)
will be called again with a backoff delay until a replica is able to process the
request.


:::{note}
This request router also implements [`on_request_routed`](../api/doc/ray.serve.request_router.RequestRouter.on_request_routed.rst)
which can help you update the state of the request router after a request is routed.
:::

(deploy-app-with-uniform-request-router)=
## Deploy an app with the uniform request router
To use a custom request router, you need to pass the `request_router_class` argument to
the [`deployment`](../api/doc/ray.serve.deployment_decorator.rst)
decorator. Also note that the `request_router_class` can be passed as the already
imported class or as the string of import path to the class. Let's deploy a simple app
that uses the uniform request router like this:

```{literalinclude} ../doc_code/custom_request_router_app.py
:start-after: __begin_deploy_app_with_uniform_request_router__
:end-before: __end_deploy_app_with_uniform_request_router__
:language: python
```

As the request is routed, both "UniformRequestRouter routing request" and
"on_request_routed callback is called!!" messages will be printed to the console. The
response will also be randomly routed to one of the replicas. You can test this by
sending more requests and seeing the distribution of the replicas are roughly equal.

:::{note}
Currently, the only way to configure the request router is to pass it as an argument to
the deployment decorator. This means that you cannot change the request router for an
existing deployment handle with running router. If you have a particular usecase where
you need to reconfigure a request router on the deployment handle, please open a feature
request on the [Ray GitHub repository](https://github.com/ray-project/ray/issues)
:::

(utility-mixin)=
## Utility mixins for request routing
Ray Serve provides utility mixins that can be used to extend the functionality of the
request router. These mixins can be used to implement common routing policies such as
locality-aware routing, multiplexed model support, and FIFO request routing.

- [`FIFOMixin`](../api/doc/ray.serve.request_router.FIFOMixin.rst): This mixin implements first in first out (FIFO)
  request routing. The default behavior for the request router is OOO (out of order)
  which routes requests to the exact replica which got assigned by the request passed to
  [`choose_replicas`](../api/doc/ray.serve.request_router.RequestRouter.choose_replicas.rst).
  This mixin is useful for the routing algorithm that can work independently of the
  request content, so the requests can be routed as soon as possible in the order they
  were received. By including this mixin, in your custom request router, the request
  matching algorithm will be updated to route requests FIFO. There are no additional
  flags needs to be configured and no additional helper methods provided by this mixin.
- [`LocalityMixin`](../api/doc/ray.serve.request_router.LocalityMixin.rst): This mixin implements locality-aware
  request routing. It updates the internal states when between replica updates to track
  the location between replicas in the same node, same zone, and everything else. It
  offers helpers [`apply_locality_routing`](../api/doc/ray.serve.request_router.LocalityMixin.apply_locality_routing.rst)
  and [`rank_replicas_via_locality`](../api/doc/ray.serve.request_router.LocalityMixin.rank_replicas_via_locality.rst) to route and
  ranks replicas based on their locality to the request, which can be useful for
  reducing latency and improving performance.
- [`MultiplexMixin`](../api/doc/ray.serve.request_router.MultiplexMixin.rst): When you use model-multiplexing
  you need to route requests based on knowing which replica has already a hot version of
  the model. It updates the internal states when between replica updates to track the
  model loaded on each replica, and size of the model cache on each replica. It offers
  helpers [`apply_multiplex_routing`](../api/doc/ray.serve.request_router.MultiplexMixin.apply_multiplex_routing.rst)
  and [`rank_replicas_via_multiplex`](../api/doc/ray.serve.request_router.MultiplexMixin.rank_replicas_via_multiplex.rst) to route
  and ranks replicas based on their multiplexed model id of the request.


(throughput-aware-request-router)=
## Define a complex throughput-aware request router
A fully featured request router can be more complex and should take into account the
multiplexed model, locality, the request queue length on each replica, and using custom
statistics like throughput  to decide which replica to route the request to. The
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
## Deploy an app with the throughput-aware request router
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
