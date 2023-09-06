(serve-key-concepts)=

# Key Concepts

(serve-key-concepts-deployment)=

## Deployment

Deployments are the central concept in Ray Serve.
A deployment contains business logic or an ML model to handle incoming requests and can be scaled up to run across a Ray cluster.
At runtime, a deployment consists of a number of *replicas*, which are individual copies of the class or function that are started in separate Ray Actors (processes).
The number of replicas can be scaled up or down (or even autoscaled) to match the incoming request load.

To define a deployment, use the {mod}`@serve.deployment <ray.serve.api.deployment>` decorator on a Python class (or function for simple use cases).
Then, `bind` the deployment with optional arguments to the constructor (see below).
Finally, deploy the resulting "bound deployment" using `serve.run` (or the equivalent `serve run` CLI command, see [Development Workflow](serve-dev-workflow) for details).

```python
from ray import serve
from ray.serve.handle import DeploymentHandle

@serve.deployment
class MyFirstDeployment:
  # Take the message to return as an argument to the constructor.
  def __init__(self, msg):
      self.msg = msg

  def __call__(self):
      return self.msg

my_first_deployment = MyFirstDeployment.bind("Hello world!")
handle: DeploymentHandle = serve.run(my_first_deployment).options(
    use_new_handle_api=True,
)
print(handle.remote().result()) # "Hello world!"
```

(serve-key-concepts-application)=

## Application

An application is the unit of upgrade in a Ray Serve cluster. An application consists of one or more deployments. One of these deployments is considered the [“ingress” deployment](serve-key-concepts-ingress-deployment), which handles all inbound traffic.

Applications can be called via HTTP at the specified `route_prefix` or in Python using a `DeploymentHandle`.
 
(serve-key-concepts-query-deployment)=

## DeploymentHandle (composing deployments)

XXX: LINK TO THE COMPOSITION SECTION!!!

Ray Serve enables flexible model composition and scaling by allowing multiple independent deployments to call into each other.
When binding a deployment, you can include references to _other bound deployments_.
Then, at runtime each of these arguments is converted to a {mod}`DeploymentHandle <ray.serve.handle.DeploymentHandle>` that can be used to query the deployment using a Python-native API.
Below is a basic example where the `Driver` deployment can call into two downstream models.
For a more comprehensive guide, see the [model composition guide](serve-model-composition).

```python
@serve.deployment
class Hello:
    def __call__(self, request) -> str:
        return "Hello"

@serve.deployment
class World:
    def __call__(self, request) -> str:
        return " world!"

@serve.deployment
class Driver:
    def __init__(self, hello_handle, world_handle):
        self._hello_handle = hello_handle.options(
            use_new_handle_api=True,
        )
        self._world_handle = world_handle.options(
            use_new_handle_api=True,
        )

    async def __call__(self, request) -> str:
        hello_response = self._hello_handle.remote(request)
        world_response = self._world_handle.remote(request)
        return (await hello_response) + (await world_response)


hello = Hello.bind()
world = World.bind()

# The deployments passed to the Driver constructor will be replaced with handles.
driver = Driver.bind(hello, world)

# Deploys hello, world, and driver.
handle: DeploymentHandle = serve.run(driver).options(
    use_new_handle_api=True,
)

# `DeploymentHandle`s can also be used to call the ingress deployment of an application.
print(handle.remote().result()) # "Hello world!"
```

(serve-key-concepts-ingress-deployment)=

## Ingress Deployment (HTTP handling)

A Serve application can consist of multiple deployments that can be combined to perform model composition or complex business logic.
However, one deployment is always the "top-level" one that is passed to `serve.run` to deploy the application.
This deployment is called the "ingress deployment" because it serves as the entrypoint for all traffic to the application.
Often, it then routes to other deployments or calls into them using the `ServeHandle` API, and composes the results before returning to the user.

The ingress deployment defines the HTTP handling logic for the application.
By default, the `__call__` method of the class will be called and passed in a `Starlette` request object.
The response will be serialized as JSON, but other `Starlette` response objects can also be returned directly.
Here's an example:

```python
@serve.deployment
class MostBasicIngress:
  async def __call__(self, request: starlette.requests.Request) -> str:
      name = await request.json()["name"]
      return f"Hello {name}"
```

After binding the deployment and running `serve.run()`, it is now exposed by the HTTP server and handles requests using the specified class.
We can query the model to verify that it's working.

```python
import requests
print(requests.get("http://127.0.0.1:8000/", json={"name": "Corey"}).text) # Hello Corey!
```

For more expressive HTTP handling, Serve also comes with a built-in integration with `FastAPI`.
This allows you to use the full expressiveness of FastAPI to define more complex APIs:

```python
from fastapi import FastAPI

app = FastAPI()

@serve.deployment
@serve.ingress(app)
class MostBasicIngress:
  @app.get("/{name}")
  async def say_hi(self, name: str) -> str:
      return f"Hello {name}"
```

## What's Next?
Now that you have learned the key concepts, you can dive into these guides:
- [Scaling and allocating resources](scaling-and-resource-allocation)
- [Configuring HTTP logic and integrating with FastAPI](http-guide)
- [Development workflow for Serve applications](serve-dev-workflow)
- [Composing deployments to perform model composition](serve-model-composition)
