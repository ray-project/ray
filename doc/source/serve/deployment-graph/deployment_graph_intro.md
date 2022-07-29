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
* `MethodNode`: a `DeploymentNode` representing a `ClassNode`'s method bound to arguments will be used to invoke the method
* `FunctionNode`: a `DeploymentNode` containing a Python function bound to its arguments

The next two sections will discuss how to construct and connects these nodes to form deployment graphs.

## ClassNodes



