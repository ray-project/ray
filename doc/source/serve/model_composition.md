(serve-model-composition)=

# Deploy Compositions of Models

This section helps you:

* compose multiple deployments containing ML logic or business logic into a single application
* independently scale and configure each of your ML models and business logic steps

Check out a [new experimental API](serve-deployment-graphs) under development for connecting Ray Serve deployments together with the **deployment graph** API.

(serve-handle-explainer)=

## Composing Deployments using ServeHandles

You can call deployment methods from within other deployments using the [ServeHandle](serve-key-concepts-query-deployment). This lets you divide your application's steps (such as preprocessing, model inference, and post-processing) into independent deployments that can be independently scaled and configured.

To use the `ServeHandle`, use {mod}`handle.remote <ray.serve.handle.RayServeHandle.remote>` to send requests to a deployment.
These requests can be ordinary Python args and kwargs that are passed directly to the method. This method call returns a Ray `ObjectRef` whose result can be waited for or retrieved using `await` or `ray.get`.

(serve-model-composition-serve-handles)=
### Model Composition Example

Here's an example:

```{literalinclude} doc_code/model_composition/class_nodes.py
:start-after: __hello_start__
:end-before: __hello_end__
:language: python
:linenos: true
```

In line 40, the `LanguageClassifier` deployment takes in the `spanish_responder` and `french_responder` as constructor arguments. At runtime, these arguments are converted into `ServeHandles`. `LanguageClassifier` can then call the `spanish_responder` and `french_responder`'s deployment methods using this handle.

For example, the `LanguageClassifier`'s `__call__` method uses the HTTP request's values to decide whether to respond in Spanish or French. It then forwards the request's name to the `spanish_responder` or the `french_responder` on lines 17 and 19 using the `ServeHandles`. The calls are formatted as:

```{literalinclude} doc_code/model_composition/class_nodes.py
:lines: 57
:language: python
```

This call has a few parts:
* `await` lets us issue an asynchronous request through the `ServeHandle`.
* `self.spanish_responder` is the `SpanishResponder` handle taken in through the constructor.
* `say_hello` is the `SpanishResponder` method to invoke.
* `remote` indicates that this is a `ServeHandle` call to another deployment. This is required when invoking a deployment's method through another deployment. It needs to be added to the method name.
* `name` is the argument for `say_hello`. You can pass any number of arguments or keyword arguments here.

This call returns a reference to the result– not the result itself. This pattern allows the call to execute asynchronously. To get the actual result, `await` the reference. `await` blocks until the asynchronous call executes, and then it returns the result. In this example, line 23 calls `await ref` and returns the resulting string. **Note that getting the result needs two `await` statements in total**. First, the script must `await` the `ServeHandle` call itself to retrieve a reference. Then it must `await` the reference to get the final result.

(serve-model-composition-await-warning)=
:::{warning}
You can use the `ray.get(ref)` method to get the return value of remote `ServeHandle` calls. However, calling `ray.get` from inside a deployment is an antipattern. It blocks the deployment from executing any other code until the call is finished. Using `await` lets the deployment process other requests while waiting for the `ServeHandle` call to finish. You should use `await` instead of `ray.get` inside deployments.
:::

You can copy the `hello.py` script above and run it with `serve run`. Make sure to run the command from a directory containing `hello.py`, so it can locate the script:

```console
$ serve run hello:language_classifier
```

You can use this client script to interact with the example:

```{literalinclude} doc_code/model_composition/class_nodes.py
:start-after: __hello_client_start__
:end-before: __hello_client_end__
:language: python
```

While the `serve run` command is running, open a separate terminal window and run this script:

```console
$ python hello_client.py

Hola Dora
```

:::{note}
Composition lets you break apart your application and independently scale each part. For instance, suppose this `LanguageClassifier` application's requests were 75% Spanish and 25% French. You could scale your `SpanishResponder` to have 3 replicas and your `FrenchResponder` to have 1 replica, so you could meet your workload's demand. This flexibility also applies to reserving resources like CPUs and GPUs, as well as any other configurations you can set for each deployment.

With composition, you can avoid application-level bottlenecks when serving models and business logic steps that use different types and amounts of resources.
:::

### ServeHandle Deep Dive

Conceptually, a `ServeHandle` is a client-side load balancer, routing requests to any replicas of a given deployment. Also, it performs buffering internally so it won't overwhelm the replicas.
Using the current number of requests buffered, it informs the autoscaler to scale up the number of replicas.

![architecture-diagram-of-serve-handle](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/serve-handle-explainer.png)

`ServeHandle`s take request parameters and returns a future object of type [`ray.ObjectRef`](objects-in-ray), whose value will be filled with the result object. Because of the internal buffering, the time from submitting a request to getting a `ray.ObjectRef` can vary.

Because of this variability, Serve offers two types of handles to ensure the buffering period is handled efficiently. We offer synchronous and asynchronous versions of the handle:
- `RayServeSyncHandle` directly returns a `ray.ObjectRef`. It blocks the current thread until the request is matched to a replica.
- `RayServeHandle` returns an `asyncio.Task` upon submission. The `asyncio.Task` can be awaited to resolve to a `ray.ObjectRef`. While the current request is buffered, other requests can be processed concurrently.

`serve.run` deploys a deployment graph and returns the entrypoint node’s handle (the node you passed as argument to `serve.run`). The return type is a `RayServeSyncHandle`. This is useful for interacting with and testing the newly created deployment graph.

```{literalinclude} doc_code/handle_guide.py
:start-after: __begin_sync_handle__
:end-before: __end_sync_handle__
:language: python
```

In all other cases, `RayServeHandle` is the default because the API is more performant than its blocking counterpart. For example, when implementing a dynamic dispatch node in deployment graph, the handle is asynchronous.

```{literalinclude} doc_code/handle_guide.py
:start-after: __begin_async_handle__
:end-before: __end_async_handle__
:language: python
```

The result of `handle.remote()` can also be passed directly as an argument to other downstream handles, without having to await on it.

```{literalinclude} doc_code/handle_guide.py
:start-after: __begin_async_handle_chain__
:end-before: __end_async_handle_chain__
:language: python
```

In both types of handles, you can call a specific method by using the `.method_name` accessor. For example:

```{literalinclude} doc_code/handle_guide.py
:start-after: __begin_handle_method__
:end-before: __end_handle_method__
:language: python
```

:::{note}
`ray.ObjectRef` corresponds to the result of a request submission. To retrieve the result, you can use the synchronous Ray Core API `ray.get(ref)` or the async API `await ref`. To wait for the result to be available without retrieving it, you can use the synchronous API `ray.wait([ref])` or the async API `await asyncio.wait([ref])`. You can mix and match these calls, but we recommend using async APIs to increase concurrency.
:::
