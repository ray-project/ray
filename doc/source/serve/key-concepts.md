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
@serve.deployment
class MyFirstDeployment:
  # Take the message to return as an argument to the constructor.
  def __init__(self, msg):
      self.msg = msg

  def __call__(self):
      return self.msg

my_first_deployment = MyFirstDeployment.bind("Hello world!")
handle = serve.run(my_first_deployment)
print(ray.get(handle.remote())) # "Hello world!"
```

(serve-key-concepts-query-deployment)=

## ServeHandle (composing deployments)

Ray Serve enables flexible model composition and scaling by allowing multiple independent deployments to call into each other.
When binding a deployment, you can include references to _other bound deployments_.
Then, at runtime each of these arguments is converted to a {mod}`ServeHandle <ray.serve.handle.RayServeHandle>` that can be used to query the deployment using a Python-native API.
Below is a basic example where the `Driver` deployment can call into two downstream models.

```python
@serve.deployment
class Driver:
    def __init__(self, model_a_handle, model_b_handle):
        self._model_a_handle = model_a_handle
        self._model_b_handle = model_b_handle

    async def __call__(self, request):
        ref_a = await self._model_a_handle.remote(request)
        ref_b = await self._model_b_handle.remote(request)
        return (await ref_a) + (await ref_b)


model_a = ModelA.bind()
model_b = ModelB.bind()

# model_a and model_b will be passed to the Driver constructor as ServeHandles.
driver = Driver.bind(model_a, model_b)

# Deploys model_a, model_b, and driver.
serve.run(driver)
```

(serve-key-concepts-ingress-deployment)=

## Ingress Deployment (HTTP handling)

A Serve application can consist of multiple deployments that can be combined to perform model composition or complex business logic.
However, there is always one "top-level" deployment, the one that will be passed to `serve.run` or `serve.build` to deploy the application.
This deployment is called the "ingress deployment" because it serves as the entrypoint for all traffic to the application.
Often, it will then route to other deployments or call into them using the `ServeHandle` API and compose the results before returning to the user.

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

(serve-key-concepts-deployment-graph)=

## Deployment Graph

Building on top of the deployment concept, Ray Serve also provides a first-class API for composing multiple models into a graph structure and orchestrating the calls to each deployment automatically.

Here's a simple example combining a preprocess function and model.

```{literalinclude} doc_code/key-concepts-deployment-graph.py
```

## What's Next?
Now that you have learned the key concepts, you can dive into the [User Guide](user-guide):
- [Scaling and allocating resources](scaling-and-resource-allocation)
- [Configuring HTTP logic and integrating with FastAPI](http-guide)
- [Development workflow for Serve applications](dev-workflow)
- [Composing deployments to perform model composition](model_composition)
