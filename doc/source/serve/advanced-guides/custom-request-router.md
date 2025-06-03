(custom-request-router-guide)=
# Use Custom Algorithm for Request Routing

This section helps you understand how to: (TODO: Update this list)
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
the `deplpyment` decorator.

```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_deploy_app_with_uniform_request_router__
:end-before: __end_deploy_app_with_uniform_request_router__
:language: python
```

:::{note}
Currently, the only way to configure the request router is to pass it as an argument to
the deployment decorator. This means that you cannot change the request router for an
existing deployment handle with running router. If you have a particular use case where
you need to reconfigure a request router, please open an issue on the 
[Ray GitHub repository](https://github.com/ray-project/ray/issues)
:::

(throughput-aware-request-router)=
## Define a complex throughput-aware request router
```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_define_throughput_aware_request_router__
:end-before: __end_define_throughput_aware_request_router__
:language: python
```

(deploy-app-with-throughput-aware-request-router)=
## Deploy an app with the throughput-aware request router
```{literalinclude} ../doc_code/custom_request_router.py
:start-after: __begin_deploy_app_with_throughput_aware_request_router__
:end-before: __end_deploy_app_with_throughput_aware_request_router__
:language: python
```
