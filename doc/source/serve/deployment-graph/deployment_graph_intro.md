# Intro to Deployment Graphs

```{note}
Note: This feature is in Alpha, so APIs are subject to change.
```

This section should help you:

* compose your Ray Serve deployments together with the **Deployment Graph** API
* serve your applications that use multi-model inference, ensemble models, ML model composition, or mixed business logic/model inference workloads
* independently scale each of your ML models and business logic steps

Ray Serve's **Deployment Graph** API lets you compose your deployments together by describing how to route a request through your deployments. This is particularly useful if you're using ML model composition or mixing business logic and model inference in your application. You can encapsulate each of your models and each of your business logic steps in independent deployments. Then, you can chain these deployments together in a deployment graph.

## DeploymentNodes

The basic building block of deployment graphs is the `DeploymentNode`. There are
three types of `DeploymentNodes`:

* `ClassNode`: a `DeploymentNode` containing a Python class bound to its constructor's arguments
* `MethodNode`: a `DeploymentNode` representing a `ClassNode`'s method bound to arguments that will be used to invoke the method
* `FunctionNode`: a `DeploymentNode` containing a Python function bound to arguments that will be used to invoke the function

The next two sections will discuss how to construct and connect these nodes to form deployment graphs.

## ClassNodes

You can create class nodes by binding class deployments to their constructor's arguments. For example:

```{literalinclude} ../doc_code/deployment_graph_intro/class_nodes.py
:start-after: __echo_class_start__
:end-before: __echo_class_end__
:language: python
```

`echo.py` defines three `ClassNodes`: `foo_node`, `bar_node`, and `baz_node`. The nodes are defined by invoking `bind` on the `EchoClass` deployment. They have different behaviors because they use different arguments in the `bind` call.

Note that all three of these nodes were created from the same `EchoClass` deployment. Class deployments are essentially factories for `ClassNodes`. A single class deployment can produce multiple `ClassNodes` through multiple `bind` statements.

There are two options to run a node:

1. `serve.run(node)`: This Python call can be added to your Python script to run a particular node. This call will start a Ray cluster (if one isn't already running), deploy the node to it, and then return. You can call this function multiple times in the same script on different nodes. Each time, it will tear down any deployments it previously deployed and deploy the passed-in node's deployment. After the script exits, the cluster and any nodes deployed by `serve.run` will be torn down.

2. `serve run module:node`: This CLI command will start a Ray cluster and run the node contained at the import path `module:node`. It will then block, allowing you to open a separate terminal window and issue requests to the running deployment. You can stop the `serve run` command with `ctrl-c`.

When you run a node, you are deploying the node's deployment and its bound arguments. Ray Serve will create a deployment in Ray and instantiate your deployment's class using the arguments. By default, you can send requests to your deployment at `http://localhost:8000`. These requests will be converted to Starlette `request` objects and passed to your class's `__call__` method.

You can copy the `echo.py` script above and run it with `serve run`. Make sure to run the command from a directory containing `echo.py`, so it can locate the script:

```console
$ serve run echo:foo_node
```

Here's a client script that can send requests to your node:

```{literalinclude} ../doc_code/deployment_graph_intro/class_nodes.py
:start-after: __echo_client_start__
:end-before: __echo_client_end__
:language: python
```

While the deployment is running with `serve run`, open a separate terminal window and issue a request to it with the `echo_client.py` script:

```
$ python echo_client.py

foo
```

(deployment-graph-intro-call-graph)=
## The Call Graph: MethodNodes and FunctionNodes

After defining your `ClassNodes`, you can specify how HTTP requests should be routed through them using the call graph. As an example, let's look at a deployment graph that implements this chain of arithmetic operations:

```
output = request + 2 - 1 + 3
```

Here's the graph:

(deployment-graph-intro-arithmetic-graph)=
```{literalinclude} ../doc_code/deployment_graph_intro/arithmetic.py
:start-after: __graph_start__
:end-before: __graph_end__
:language: python
:linenos: true
```

In lines 29 and 30, we bind two `ClassNodes` from the `AddCls` deployment. In line 32, we start our call graph:

```python
with InputNode() as http_request:
    request_number = unpack_request.bind(http_request)
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)
```

The `with` statement (know as a "context manager" in Python) initializes a special Ray Serve-provided object called an `InputNode`. This isn't a `DeploymentNode` like `ClassNodes`, `MethodNodes`, or `FunctionNodes`. Rather, it represents the input of our graph. In this case, that input will be an HTTP request. In [a future section](deployment-graph-drivers-http-adapters-intro), we'll show how you can change this input type using another Ray Serve-provided object called the driver.

:::{note}
It's important to note that the `InputNode` is merely a representation of the future graph input. In this example, for instance, `http_request`'s type is `InputNode`, not an actual HTTP request.
:::

We use the `InputNode` to indicate which node(s) the graph input should be passed to by passing the `InputNode` into `bind` calls within the context manager. In this case, the `http_request` is passed to only one node, `unpack_request`. The output of that bind call, `request_number` is a `FunctionNode`. `FunctionNodes` are produced when deployments containing functions are bound to arguments for that function using `bind`. In this case `request_number` represents the output of `unpack_request` when called on incoming HTTP requests. `unpack_request`, which is defined on line 26, processes the HTTP request's JSON body and returns a number that can be passed into arithmetic operations.

The graph then passes `request_number` into a `bind` call on `add_2`'s `add` method. The output of this call, `add_2_output` is a `MethodNode`. `MethodNodes` are produced when `ClassNode` methods are bound to arguments using `bind`. In this case, `add_2_output` represents the result of adding 2 to the number in the request.

The rest of the call graph uses another `FunctionNode` and `MethodNode` to finish the chain of arithmetic. `add_2_output` is bound to the `subtract_one_fn` deployment, producing the `subtract_1_output` `FunctionNode`. Then, the `subtract_1_output` is bound to the `add_3.add` method, producing the `add_3_output` `MethodNode`. This `add_3_output` `MethodNode` represents the final output from our chain of arithmetic operations.

To run the call graph, you need to use a driver. Drivers are deployments that process the call graph that you've written and route incoming requests through your deployments based on that graph. Ray Serve provides a driver called `DAGDriver` used on line 38:

```python
deployment_graph = DAGDriver.bind(add_3_output)
```

The `DAGDriver` needs to be bound to the `FunctionNode` or `MethodNode` representing the final output of our graph. This `bind` call returns a `ClassNode` that you can run in `serve.run` or `serve run`. Running this `ClassNode` will also deploy the rest of the graph's deployments.

You can test this example using this client script:

```{literalinclude} ../doc_code/deployment_graph_intro/arithmetic.py
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

### Invoking ClassNodes from within other ClassNodes

You can also invoke `ClassNode` methods from other `ClassNodes`. This is especially useful when implementing conditional logic in your graph. You can encode the conditional logic in one of your deployments and invoke another deployment from there.

Here's an example:

```{literalinclude} ../doc_code/deployment_graph_intro/class_nodes.py
:start-after: __hello_start__
:end-before: __hello_end__
:language: python
:linenos: true
```

In line 40, the `LanguageClassifier` deployment takes in the `spanish_responder` and `french_responder` `ClassNodes` as constructor arguments. Its `__call__` method uses the request's values to decide whether to respond in Spanish or French. It then forwards the request's name to the `SpanishResponder` or the `FrenchResponder` on lines 17 and 19. The calls are formatted as:

```python
self.spanish_responder.say_hello.remote(name)
```

This call has a few parts:
* `self.spanish_responder` is the `SpanishResponder` node taken in through the constructor.
* `say_hello` is the `SpanishResponder` method to invoke on this node.
* `remote` indicates that this is a remote call to another deployment. This is required when invoking a deployment's method through another deployment. It needs to be added to the method name.
* `name` is the argument for `say_hello`. You can pass any number of arguments or keyword arguments here.

This call returns a reference to the result– not the result itself. This pattern allows the call to execute asynchronously. To get the actual result, call `ray.get` on the result. This function blocks until the asynchronous call executes and then returns the result. In this example, line 23 calls `ray.get(ref)` and returns the resulting string.

You can try this example out using the `serve run` CLI:

```console
$ serve run hello:language_classifier
```

You can use this client script to interact with the example:

```{literalinclude} ../doc_code/deployment_graph_intro/class_nodes.py
:start-after: __hello_client_start__
:end-before: __hello_client_end__
:language: python
```

While the `serve run` is running, open a separate terminal window and run this script:

```console
$ python hello_client.py

Hola Dora
```

(deployment-graph-drivers-http-adapters-intro)=
## Drivers and HTTP Adapters

Ray Serve provides the `DAGDriver`, which routes HTTP requests through your call graph. As mentioned in [the call graph section](deployment-graph-intro-call-graph), the `DAGDriver` has one required argument: the `FunctionNode` or `MethodNode` representing your call graph's final output.

The `DAGDriver` also has one more optional keyword argument: `http_adapter`. `http_adapters` are functions that get run on the HTTP request before it's passed into the graph. Ray Serve provides a handful of these adapters, so you can rely on them to conveniently handle the HTTP parsing while focusing your attention on the graph itself.

For instance, we can use the Ray Serve-provided `json_request` adapter to simplify our [arithmetic call graph](deployment-graph-intro-arithmetic-graph) by eliminating the `unpack_request` function. Here's the revised call graph and driver:

```python
from ray.serve.http_adapters import json_request

with InputNode() as request_number:
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)

graph = DAGDriver.bind(add_3_output, http_adapter=json_request)
```

Note that the `http_adapter`'s output type becomes what the `InputNode` represents. Without the `json_request` adapter, the `InputNode` represented an HTTP request. With the adapter, it now represents the number packaged inside the request's JSON body. You can work directly with that body's contents in the graph instead of first processing it.

## Next Steps

To learn more about deployment graphs, check out these resources:

* [Deployment graph patterns](serve-deployment-graph-patterns): a cookbook of common graph examples you can incorporate into your own graph
* [End-to-end tutorial](deployment-graph-e2e-tutorial): an in-depth example that walks you through building a large deployment graph
