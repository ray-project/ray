(serve-model-composition)=

# Model Composition

This section helps you:

* compose multiple deployments containing ML logic or business logic into a single application
* independently scale and configure each of your ML models and business logic steps
* connect your Ray Serve deployments together with the **deployment graph** API

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

```python
await self.spanish_responder.say_hello.remote(name)
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
- `RayServeDeploymentHandle` returns an `asyncio.Task` upon submission. The `asyncio.Task` can be awaited to resolve to a `ray.ObjectRef`. While the current request is buffered, other requests can be processed concurrently.

`serve.run` deploys a deployment graph and returns the entrypoint node’s handle (the node you passed as argument to `serve.run`). The return type is a `RayServeSyncHandle`. This is useful for interacting with and testing the newly created deployment graph.

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_sync_handle__
:end-before: __end_sync_handle__
:language: python
```

In all other cases, `RayServeDeploymentHandle` is the default because the API is more performant than its blocking counterpart. For example, when implementing a dynamic dispatch node in deployment graph, the handle is asynchronous.

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_async_handle__
:end-before: __end_async_handle__
:language: python
```

The result of `deployment_handle.remote()` can also be passed directly as an argument to other downstream handles, without having to await on it.

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_async_handle_chain__
:end-before: __end_async_handle_chain__
:language: python
```

In both types of `ServeHandle`, you can call a specific method by using the `.method_name` accessor. For example:

```{literalinclude} ../serve/doc_code/handle_guide.py
:start-after: __begin_handle_method__
:end-before: __end_handle_method__
:language: python
```

:::{note}
`ray.ObjectRef` corresponds to the result of a request submission. To retrieve the result, you can use the synchronous Ray Core API `ray.get(ref)` or the async API `await ref`. To wait for the result to be available without retrieving it, you can use the synchronous API `ray.wait([ref])` or the async API `await asyncio.wait([ref])`. You can mix and match these calls, but we recommend using async APIs to increase concurrency.
:::

(serve-model-composition-deployment-graph)=
## Deployment Graph API

:::{note}
The call graph is in **alpha**, so its APIs are subject to change.
:::

For more advanced composition patterns, it can be useful to surface the relationships between deployments, instead of hiding them inside individual deployment definitions.

Ray Serve's **deployment graph API** lets you specify how to route requests through your deployments, so you can explicitly create a dependency graph. It also has additional features like HTTP adapters and input routing that help you build more expressive graphs.

### Binding Deployments

The basic building block for all deployment graphs is the `DeploymentNode`. One type of `DeploymentNode` is the `ClassNode`. You can create `ClassNodes` by binding class-based deployments to their constructor's arguments with the `bind` method. This may sound familiar because you've already been doing this whenever you bind and run class-based deployments, such as in the [Calling Deployments using ServeHandles](serve-model-composition-serve-handles) section.

As another example:

```{literalinclude} doc_code/model_composition/class_nodes.py
:start-after: __echo_class_start__
:end-before: __echo_class_end__
:language: python
```

`echo.py` defines three `ClassNodes`: `foo_node`, `bar_node`, and `baz_node`. The nodes are defined by invoking `bind` on the `EchoClass` deployment. They have different behaviors because they use different arguments in the `bind` call.

Note that all three of these nodes were created from the same `EchoClass` deployment. Class deployments are essentially factories for `ClassNodes`. A single class deployment can produce multiple `ClassNodes` through multiple `bind` statements.

There are two options to run a node:

1. `serve.run(node)`: This Python call can be added to your Python script to run a particular node. This call starts a Ray cluster (if one isn't already running), deploys the node to it, and then returns. You can call this function multiple times in the same script on different `DeploymentNodes`. Each time, it tears down any deployments it previously deployed and deploy the passed-in node's deployment. After the script exits, the cluster and any nodes deployed by `serve.run` are torn down.

2. `serve run module:node`: This CLI command starts a Ray cluster and runs the node at the import path `module:node`. It then blocks, allowing you to open a separate terminal window and issue requests to the running deployment. You can stop the `serve run` command with `ctrl-c`.

When you run a node, you are deploying the node's deployment and its bound arguments. Ray Serve creates a deployment in Ray and instantiates your deployment's class using the arguments. By default, you can send requests to your deployment at `http://localhost:8000`. These requests are converted to Starlette `request` objects and passed to your class's `__call__` method.

:::{note}
Additionally, when you run a node, the deployment's configurations (which you can set in the `@serve.deployment` decorator, through an `options` call, or a [Serve config file](serve-in-production-config-file)) still apply to the deployment. You can use this to independently scale and configure your graph's deployments by, for instance, setting different `num_replicas`, `num_cpus`, or `num_gpus` values for different deployments.
:::

You can try this example out using the `serve run` CLI:

```console
$ serve run echo:foo_node
```

Here's a client script that can send requests to your node:

```{literalinclude} doc_code/model_composition/class_nodes.py
:start-after: __echo_client_start__
:end-before: __echo_client_end__
:language: python
```

While the deployment is running with `serve run`, open a separate terminal window and issue a request to it with the `echo_client.py` script:

```
$ python echo_client.py

foo
```

(deployment-graph-call-graph)=
### Building the Call Graph: MethodNodes and FunctionNodes

After defining your `ClassNodes`, you can specify how HTTP requests should be processed using the call graph. As an example, let's look at a deployment graph that implements this chain of arithmetic operations:

```
output = request + 2 - 1 + 3
```

Here's the graph:

(deployment-graph-arithmetic-graph)=
```{literalinclude} doc_code/model_composition/arithmetic.py
:start-after: __graph_start__
:end-before: __graph_end__
:language: python
:linenos: true
```

Lines 31 and 32 bind two `ClassNodes` from the `AddCls` deployment. Line 34 starts the call graph:

```python
with InputNode() as http_request:
    request_number = unpack_request.bind(http_request)
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)
```

The `with` statement (known as a "context manager" in Python) initializes a special Ray Serve-provided object called an `InputNode`. This isn't a `DeploymentNode` like `ClassNodes`, `MethodNodes`, or `FunctionNodes`. Rather, it's the input of the graph. In this case, that input is an HTTP request. In a [later section](deployment-graph-drivers-http-adapters), you'll learn how to change this input using another Ray Serve-provided object called the `DAGDriver`.

(deployment-graph-call-graph-input-node-note)=
:::{note}
The `InputNode` tells Ray Serve where to send the graph input at runtime. In this example, for instance, `http_request` is an `InputNode` object, so you can't call `request` methods like `.json()` on it directly in the context manager. However, during runtime, Ray Serve passes incoming HTTP requests directly into the same functions and methods that `http_request` is passed into, so those functions and methods can call `request` methods like `.json()` on the `request` object that gets passed in.
:::

You can use the `InputNode` to indicate which node(s) the graph input should be passed into by passing the `InputNode` into `bind` calls within the context manager. In this example, the `http_request` is passed to only one node, `unpack_request`. The output of that bind call, `request_number`, is a `FunctionNode`. `FunctionNodes` are produced when deployments containing functions are bound to arguments for that function using `bind`. `request_number` represents the output of `unpack_request` when called on incoming HTTP requests. `unpack_request`, which is defined on line 26, processes the HTTP request's JSON body and returns a number that can be passed into arithmetic operations.

:::{tip}
If you don't want to manually unpack HTTP requests, check out this guide's section on [HTTP adapters](deployment-graph-drivers-http-adapters), which can handle unpacking for you.
:::

The graph then passes `request_number` into a `bind` call on `add_2`'s `add` method. The output of this call, `add_2_output` is a `MethodNode`. `MethodNodes` are produced when `ClassNode` methods are bound to arguments using `bind`. In this case, `add_2_output` represents the result of adding 2 to the number in the request.

The rest of the call graph uses another `FunctionNode` and `MethodNode` to finish the chain of arithmetic. `add_2_output` is bound to the `subtract_one_fn` deployment, producing the `subtract_1_output` `FunctionNode`. Then, the `subtract_1_output` is bound to the `add_3.add` method, producing the `add_3_output` `MethodNode`. This `add_3_output` `MethodNode` represents the final output from the chain of arithmetic operations.

To run the call graph, you need to use a driver. Drivers are deployments that process the call graph that you've written and route incoming requests through your deployments based on that graph. Ray Serve provides a driver called `DAGDriver` used on line 40:

```python
graph = DAGDriver.bind(add_3_output)
```

Generally, the `DAGDriver` needs to be bound to the `FunctionNode` or `MethodNode` representing the final output of a graph. This `bind` call returns a `ClassNode` that you can run in `serve.run` or `serve run`. Running this `ClassNode` also deploys the rest of the graph's deployments.

:::{note}
The `DAGDriver` can also be bound to `ClassNodes`. This is useful if you construct a deployment graph where `ClassNodes` invoke other `ClassNodes`' methods. In this case, you should pass in the "root" `ClassNode` to `DAGDriver` (i.e. the one that you would otherwise pass into `serve.run`). Check out the [Calling Deployments using ServeHandles](serve-model-composition-serve-handles) section for more info.
:::

You can test this example using this client script:

```{literalinclude} doc_code/model_composition/arithmetic.py
:start-after: __graph_client_start__
:end-before: __graph_client_end__
:language: python
```

Start the graph in the terminal:

```console
$ serve run arithmetic:graph
```

In a separate terminal window, run the client script to make requests to the graph:

```console
$ python arithmetic_client.py

9
```

(deployment-graph-drivers-http-adapters)=
### Drivers and HTTP Adapters

Ray Serve provides the `DAGDriver`, which routes HTTP requests through your call graph. As mentioned in [the call graph section](deployment-graph-call-graph), the `DAGDriver` takes in a `DeploymentNode` and it produces a `ClassNode` that you can run.

The `DAGDriver` also has an optional keyword argument: `http_adapter`. [HTTP adapters](serve-http-adapters) are functions that get run on the HTTP request before it's passed into the graph. Ray Serve provides a handful of these adapters, so you can rely on them to conveniently handle the HTTP parsing while focusing your attention on the graph itself.

For instance, you can use the Ray Serve-provided `json_request` adapter to simplify the [arithmetic call graph](deployment-graph-arithmetic-graph) by eliminating the `unpack_request` function. You can replace lines 29 through 38 with this graph:

(http-adapter-arithmetic-example)=
```{literalinclude} doc_code/model_composition/arithmetic.py
:start-after: __adapter_graph_start__
:end-before: __adapter_graph_end__
:language: python
```

Without an `http_adapter`, an `InputNode` [represents an HTTP request](deployment-graph-call-graph-input-node-note), and at runtime, incoming HTTP `request` objects are passed into the same functions and methods that the `InputNode` is passed into. When you set an `http_adapter`, the `InputNode` represents the `http_adapter`'s output.

At runtime:

1. Ray Serve sends each HTTP `request` object to the `DAGDriver`.
2. The `DAGDriver` calls the `http_adapter` function on each request.
3. The `DAGDriver` passes the `http_adapter` output to the same function and methods that the `InputNode` is passed into, kicking off the request's journey through the call graph.

In the example above, the `InputNode` represents the number packaged inside the request's JSON body instead of the HTTP request itself. You can pass the JSON directly into the graph instead of first unpacking it from the request.

See [the guide](serve-http-adapters) on `http_adapters` to learn more.

(deployment-graph-call-graph-testing)=
### Testing the Graph with the Python API

The `serve.run` function returns a handle that you can use to test your graph in Python, without using HTTP requests.

To test your graph,

1. Call `serve.run` on your graph and store the returned handle.
2. Call `handle.predict.remote(input)`. **The `input` argument becomes the input represented by `InputNode`**. Make sure to refactor your call graph accordingly, since it takes in this input directly, instead of an HTTP request. You can use an [HTTP adapter](deployment-graph-drivers-http-adapters) to make sure the graph you're testing matches the one you ultimately deploy.
3. `predict.remote` returns a reference to the result, so the graph can execute asynchronously. Call `ray.get` on this reference to get the final result.

As an example, you can continue rewriting the [arithmetic graph example](http-adapter-arithmetic-example) from above to use `predict.remote`. You can add testing code to the example:

```{literalinclude} doc_code/model_composition/arithmetic.py
:start-after: __test_graph_start__
:end-before: __test_graph_end__
:language: python
```

Note that the graph itself is still the same. The only change is the testing code added after it. You can run this Python script directly now to test the graph:

```
$ python arithmetic.py

9
```

(pydot-visualize-dag)=
### Visualizing the Graph

You can render an illustration of your deployment graph to see its nodes and their connection.

Make sure you have `pydot` and `graphviz` to follow this section:

:::{tab-set}

::::::{tab-item} MacOS
```
pip install -U pydot && brew install graphviz
```
::::::

::::{tab-item} Windows
```
pip install -U pydot && winget install graphviz
```
::::::

::::::{tab-item} Linux
```
pip install -U pydot && sudo apt-get install -y graphviz
```
::::::

:::

Here's an example graph:

```{literalinclude} doc_code/model_composition/deployment_graph_viz.py
:language: python
```

The `ray.dag.vis_utils._dag_to_dot` method takes in a `DeploymentNode` and produces a graph visualization. You can see the string form of the visualization by running the script:

```console
$ python deployment_graph_viz.py

digraph G {
rankdir=LR;
INPUT_ATTRIBUTE_NODE -> forward;
INPUT_NODE -> INPUT_ATTRIBUTE_NODE;
Model -> forward;
}

digraph G {
rankdir=LR;
forward -> combine;
INPUT_ATTRIBUTE_NODE -> forward;
INPUT_NODE -> INPUT_ATTRIBUTE_NODE;
Model -> forward;
forward_1 -> combine;
INPUT_ATTRIBUTE_NODE_1 -> forward_1;
INPUT_NODE -> INPUT_ATTRIBUTE_NODE_1;
Model_1 -> forward_1;
INPUT_ATTRIBUTE_NODE_2 -> combine;
INPUT_NODE -> INPUT_ATTRIBUTE_NODE_2;
}
```

You can render these strings in `graphviz` tools such as [https://dreampuf.github.io/GraphvizOnline](https://dreampuf.github.io/GraphvizOnline).

When the script visualizes `m1_output`, it shows a partial execution path of the entire graph:

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/visualize_partial.svg)

This path includes only the dependencies needed to generate `m1_output`.

On the other hand, when the script visualizes the final graph output, `combine_output`, it captures all nodes used in execution since they're all required to create the final output.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/deployment-graph/visualize_full.svg)

#### Visualizing the Graph with Gradio
Another option is to visualize your deployment graph through Gradio. Check out the [Graph Visualization with Gradio Tutorial](serve-gradio-dag-visualization) to learn how to interactively run your deployment graph through the Gradio UI and see the intermediate outputs of each node in real time as they finish evaluation.

## Next Steps

To learn more about deployment graphs, check out some [deployment graph patterns](serve-deployment-graph-patterns-overview) you can incorporate into your own graph!
