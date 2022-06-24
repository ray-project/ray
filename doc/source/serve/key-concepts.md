(key-concepts)=

# Key Concepts

(serve-key-concepts-deployment)=

## Deployment

Deployments are the central concept in Ray Serve.
They allow you to define and update your business logic or models that will handle incoming requests as well as how this is exposed over HTTP or in Python.

A deployment is defined using {mod}`@serve.deployment <ray.serve.api.deployment>` on a Python class (or function for simple use cases).
You can specify arguments to be passed to the constructor when you call `Deployment.deploy()`, shown below.

A deployment consists of a number of *replicas*, which are individual copies of the function or class that are started in separate Ray Actors (processes).

```python
@serve.deployment
class MyFirstDeployment:
  # Take the message to return as an argument to the constructor.
  def __init__(self, msg):
      self.msg = msg

  def __call__(self, request):
      return self.msg

  def other_method(self, arg):
      return self.msg

MyFirstDeployment.deploy("Hello world!")
```

Deployments can be exposed in two ways: over HTTP or in Python via the {ref}`servehandle-api`.
By default, HTTP requests will be forwarded to the `__call__` method of the class (or the function) and a `Starlette Request` object will be the sole argument.
You can also define a deployment that wraps a FastAPI app for more flexible handling of HTTP requests. See {ref}`serve-fastapi-http` for details.

To serve multiple deployments defined by the same class, use the `name` option:

```python
MyFirstDeployment.options(name="hello_service").deploy("Hello!")
MyFirstDeployment.options(name="hi_service").deploy("Hi!")
```

You can also list all available deployments and dynamically get references to them:

```python
>> serve.list_deployments()
{'A': Deployment(name=A,version=None,route_prefix=/A)}
{'MyFirstDeployment': Deployment(name=MyFirstDeployment,version=None,route_prefix=/MyFirstDeployment}

# Returns the same object as the original MyFirstDeployment object.
# This can be used to redeploy, get a handle, etc.
deployment = serve.get_deployment("MyFirstDeployment")
```

## HTTP Ingress
By default, deployments are exposed over HTTP at `http://localhost:8000/<deployment_name>`.
The HTTP path that the deployment is available at can be changed using the `route_prefix` option.
All requests to `/{route_prefix}` and any subpaths will be routed to the deployment (using a longest-prefix match for overlapping route prefixes).

Here's an example:

```python
@serve.deployment(name="http_deployment", route_prefix="/api")
class HTTPDeployment:
  def __call__(self, request):
      return "Hello world!"
```

After creating the deployment, it is now exposed by the HTTP server and handles requests using the specified class.
We can query the model to verify that it's working.

```python
import requests
print(requests.get("http://127.0.0.1:8000/api").text)
```

## ServeHandle

We can also query the deployment using the {mod}`ServeHandle <ray.serve.handle.RayServeHandle>` interface.

```python
# To get a handle from the same script, use the Deployment object directly:
handle = HTTPDeployment.get_handle()

# To get a handle from a different script, reference it by name:
handle = serve.get_deployment("http_deployment").get_handle()

print(ray.get(handle.remote()))
```

As noted above, there are two ways to expose deployments. The first is by using the {mod}`ServeHandle <ray.serve.handle.RayServeHandle>`
interface. This method allows you to access deployments within a Python script or code, making it convenient for a
Python developer. And the second is by using the HTTP request, allowing access to deployments via a web client application.

:::{note}
  Let's look at a simple end-to-end example using both ways to expose and access deployments. Your output may
  vary due to random nature of how the prediction is computed; however, the example illustrates two things:
  1) how to expose and use deployments and 2) how to use replicas, to which requests are sent. Note that each pid
  is a separate replica associated with each deployment name, `rep-1` and `rep-2` respectively.

  ```{literalinclude} doc_code/create_deployment.py
  :end-before: __serve_example_end__
  :language: python
  :start-after: __serve_example_begin__
  ```
:::

## Deployment Graph

Building on top of the Deployment concept, Ray Serve provides a first-class API for composing models into a graph structure.

Here's a simple example combining a preprocess function and model.

```{literalinclude} doc_code/key-concepts-deployment-graph.py
```

## What's Next?
Now you have learned about the key concepts. You can dive into our [User Guides](user-guide) for more details into:
- [Creating, updating, and deleting deployments](managing-deployments)
- [Configuring HTTP ingress and integrating with FastAPI](http-guide)
- [Composing deployments using ServeHandle](handle-guide)
- [Building Deployment Graphs](deployment-graph)
