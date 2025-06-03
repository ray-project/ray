(custom-request-router-guide)=
# Use Custom Algorithm for Request Routing

This section helps you understand how to:
- Define a simple uniform request router
- Deploy an app with the uniform request router
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
for distribute the load evenly regardless of the request queue length. The router is
defined as a class that inherits from `ray.serve.request_router.RequestRouter`. It
implements the `choose_replicas` method, which returns the random replica for all
incoming requests.

:::{note}
This request router also implements `on_request_routed` which can help you update the
state of the request router after a request is routed.
:::

(deploy-app-with-uniform-request-router)=
## Deploy an app with the uniform request router
To use a custom request router, you need to pass the `request_router_class` argument to
the `deplpyment` decorator. Let's deploy a simple app that uses the uniform request
router like this:

```{literalinclude} ../doc_code/custom_request_router.py
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
existing deployment handle with running router. If you have a particular use case where
you need to reconfigure a request router, please open an issue on the
[Ray GitHub repository](https://github.com/ray-project/ray/issues)
:::

(throughput-aware-request-router)=
## Define a complex throughput-aware request router
A fully featured request router can be more complex and take into account the
multiplexed model, locality, the request queue length, and using custom stats to decide
which replica to route the request to. The following code defines a throughput-aware
request router that routes requests to the replica with the factors. Add the following
code into the `custom_request_router.py` file:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_throughput_aware_request_router__
:end-before: __end_define_throughput_aware_request_router__
:language: python
```

This request router inherits from `ray.serve.request_router.RequestRouter`, as well as
`ray.serve.request_router.FIFOMixin` for FIFO request routing,
`request_router.LocalityMixin` for locality-aware request routing, and
`request_router.MultiplexedModelMixin` for multiplexed model support. It implements
`choose_replicas` to take the highest ranked replicas from `rank_replicas_via_multiplex`
and `rank_replicas_via_locality` and uses the `select_available_replicas` helper to
filter out replicas that have reached their maximum request queue length. Finally, it
sorts the replicas by their throughput and returns the top one.

(deploy-app-with-throughput-aware-request-router)=
## Deploy an app with the throughput-aware request router
To use the throughput-aware request router, you can deploy an app like this:

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_deploy_app_with_throughput_aware_request_router__
:end-before: __end_deploy_app_with_throughput_aware_request_router__
:language: python
```

Similar to the uniform request router, the custom request router can be defined in the
`request_router_class` argument of the `deployment` decorator. The
`request_routing_stats_period_s` and `request_routing_stats_timeout_s` arguments are
also configured to enable the request routing stats collection at the specified period.
During runtime, `record_routing_stats` method will be called by Serve controller to
collect the routing stats from each replica. The custom request router can then get the
updated routing stats by looking up the `routing_stats` attribute of the running
replicas and use it in the routing policy.
