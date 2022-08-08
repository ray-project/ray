(serve-key-concepts)=

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

my_first_deployment = MyFirstDeployment.bind("Hello world!")
```

Deployments can be exposed in two ways: exposed to an end user over HTTP, or 
exposed to other deployments in Python by including them in the input argument of `.bind()` for other deployments,
e.g. `deployment_1.bind(deployment_2)`.
By default, HTTP requests will be forwarded to the `__call__` method of the class (or the function) and a `Starlette Request` object will be the sole argument.
You can also define a deployment that wraps a FastAPI app for more flexible handling of HTTP requests. See {ref}`serve-fastapi-http` for details.

To deploy multiple deployments that serve the same class or function, use the `name` option:

```python
MyFirstDeployment.options(name="hello_service").bind("Hello!")
MyFirstDeployment.options(name="hi_service").bind("Hi!")
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

After binding the deployment and running `serve.run()`, it is now exposed by the HTTP server and handles requests using the specified class.
We can query the model to verify that it's working.

```python
import requests
print(requests.get("http://127.0.0.1:8000/api").text)
```

(serve-key-concepts-query-deployment)=
## ServeHandle

We can also query the deployment from other deployments using the {mod}`ServeHandle <ray.serve.handle.RayServeHandle>` interface.

```python
deployment_1 = Deployment1.bind()

# deployment_1 will be passed into the constructor of Deployment2 to use as a
# Python handle (a ServeHandle) in the code for Deployment2.
deployment_2 = Deployment2.bind(deployment_1)
```

As noted above, there are two ways to expose deployments. The first is by using the {mod}`ServeHandle <ray.serve.handle.RayServeHandle>`
interface. This method allows you to access deployments from within other deployments in Python code, making it convenient for a
Python developer to compose models together. The second is by using an HTTP request, allowing access to deployments via a web client application.

:::{note}
  Let's look at a simple end-to-end example using ServeHandle to query intermediate deployments. Your output may
  vary due to random nature of how the prediction is computed; however, the example illustrates two things:
  1) how to expose and use deployments and 2) how to use replicas, to which requests are sent. Note that each PID
  is a separate replica associated with each deployment.

  To run this code, first run `ray start --head` to start a single-node Ray cluster on your machine, then run the following script.

  ```{literalinclude} doc_code/create_deployment.py
  :end-before: __serve_example_end__
  :language: python
  :start-after: __serve_example_begin__
  ```
:::

(serve-key-concepts-deployment-graph)=
## Deployment Graph

Building on top of the Deployment concept, Ray Serve provides a first-class API for composing models into a graph structure.

Here's a simple example combining a preprocess function and model.

```{literalinclude} doc_code/key-concepts-deployment-graph.py
```

## What's Next?
Dive into the [User Guides](user-guide) for more details about:
- [Configuring Serve Deployments](serve-configuring-serve-deployments)
- [Configuring HTTP ingress and integrating with FastAPI](http-guide)
- [Composing deployments using ServeHandle](handle-guide)
- [Building Deployment Graphs](deployment-graph)
