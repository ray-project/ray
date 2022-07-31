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

## The Call Graph: MethodNodes and FunctionNodes

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

## Drivers and HTTP Adapters

### Parsing the InputNode

## Next Steps

