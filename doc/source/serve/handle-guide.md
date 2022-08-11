(serve-handle-explainer)=

# ServeHandle: Calling Deployments from Python

ServeHandle can be used to invoke your Serve deployment and deployment graphs using Python API.

This is particularly useful for two use cases:
Calling deployments dynamically within the deployment graph.
Iterating and testing your application in Python.

To use the ServeHandle, use {mod}`handle.remote <ray.serve.handle.RayServeHandle.remote>` to send requests to that deployment.
These requests can pass ordinary args and kwargs that are passed directly to the method. This returns a Ray `ObjectRef` whose result can be waited for or retrieved using `await` or `ray.get`.

Conceptually, ServeHandle is a client side load balancer. It has the ability to route requests to any replicas of a given deployment. It also performs buffering internally so it won’t overwhelm the replicas. The number of requests buffered is used to inform autoscaler to scale up the number of requests.

![architecture-diagram-of-serve-handle](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/serve-handle-explainer.png)

ServeHandle takes request parameters and returns a `ray.ObjectRef`. The `ray.ObjectRef` corresponds to a future object that will be fulfilled with the result object. Because of the internal buffering, the time from submitting a request to getting a `ray.ObjectRef` varies from instantaneous to indefinitely long.

Because of this, we have two types of handle to make sure the buffering period is handled efficiently:
- `RayServeDeploymentHandle` returns an `asyncio.Task` upon submission. The `asyncio.Task` can be awaited to resolve to a ray.ObjectRef. When a single request is being buffered, other requests can be processed concurrently.
- `RayServeSyncHandle` directly returns a `ray.ObjectRef`. It blocks the current thread until the request is matched to a replica. This also matches the Ray Core Actor API.

When you call `serve.run` to deploy a deployment graph (including single node graph), the driver node’s handle is returned. The return type is a ServeSyncHandle. This is useful for interacting with the deployment graph you just created and test against it.

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_sync_handle__
:end-before: __end_sync_handle__
:language: python
```

Other than `serve.run`, RayServeDeploymentHandle is the default because the API is more performant than its blocking counterpart. For example, when implementing a dynamic dispatch node in deployment graph, the handle is asynchronous.

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_async_handle__
:end-before: __end_async_handle__
:language: python
```

You don't have to use multiple await in common case because you can directly put the result of `deployment_handle.remote()` as argument for downstream handles.

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_async_handle_chain__
:end-before: __end_async_handle_chain__
:language: python
```

## Note about ray.ObjectRef

`ray.ObjectRef` corresponds to a result of a request submission. To retrieve the result, you can use its synchronous API `ray.get(ref)` or async API `await ref`. To wait for the result to be available, you can use its synchronous API `ray.wait([ref])` or async API `await asyncio.wait([ref])`. You can mix and match them; but we recommend using the async API because it helps support high concurrency.

## Calling a specific method

In both types of ServeHandle, you can call a specific method by using the `.method_name` accessor. For example:

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_handle_method__
:end-before: __end_handle_method__
:language: python
```