# Calling Deployments via HTTP and Python

This section should help you:

- understand how deployments can be called in two ways: from HTTP and from Python
- integrate Ray Serve with an existing web server

```{contents} Calling Deployments via HTTP and Python
```

(serve-http)=

## Calling Deployments via HTTP

### Basic Example

When you create a deployment, it is exposed over HTTP by default at `/{deployment_name}`. You can change the route by specifying the `route_prefix` argument to the {mod}`@serve.deployment <ray.serve.api.deployment>` decorator.

```python
@serve.deployment(route_prefix="/counter")
class Counter:
    def __call__(self, request):
        pass
```

When you make a request to the Serve HTTP server at `/counter`, it will forward the request to the deployment's `__call__` method and provide a [Starlette Request object](https://www.starlette.io/requests/) as the sole argument. The `__call__` method can return any JSON-serializable object or a [Starlette Response object](https://www.starlette.io/responses/) (e.g., to return a custom status code).

Below, we discuss some advanced features for customizing Ray Serve's HTTP functionality.

(serve-fastapi-http)=

### FastAPI HTTP Deployments

If you want to define more complex HTTP handling logic, Serve integrates with [FastAPI](https://fastapi.tiangolo.com/). This allows you to define a Serve deployment using the {mod}`@serve.ingress <ray.serve.api.ingress>` decorator that wraps a FastAPI app with its full range of features. The most basic example of this is shown below, but for more details on all that FastAPI has to offer such as variable routes, automatic type validation, dependency injection (e.g., for database connections), and more, please check out [their documentation](https://fastapi.tiangolo.com/).

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")
serve.start(detached=True)

@serve.deployment(route_prefix="/hello")
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"

MyFastAPIDeployment.deploy()
```

Now if you send a request to `/hello`, this will be routed to the `root` method of our deployment. We can also easily leverage FastAPI to define multiple routes with different HTTP methods:

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")
serve.start(detached=True)

@serve.deployment(route_prefix="/hello")
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"

    @app.post("/{subpath}")
    def root(self, subpath: str):
        return f"Hello from {subpath}!"

MyFastAPIDeployment.deploy()
```

You can also pass in an existing FastAPI app to a deployment to serve it as-is:

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")
serve.start(detached=True)

@app.get("/")
def f():
    return "Hello from the root!"

# ... add more routes, routers, etc. to `app` ...

@serve.deployment(route_prefix="/")
@serve.ingress(app)
class FastAPIWrapper:
    pass

FastAPIWrapper.deploy()
```

This is useful for scaling out an existing FastAPI app with no modifications necessary.
Existing middlewares, automatic OpenAPI documentation generation, and other advanced FastAPI features should work as-is.
You can also combine routes defined this way with routes defined on the deployment:

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")
serve.start(detached=True)

@app.get("/")
def f():
    return "Hello from the root!"

@serve.deployment(route_prefix="/api1")
@serve.ingress(app)
class FastAPIWrapper1:
    @app.get("/subpath")
    def method(self):
        return "Hello 1!"

@serve.deployment(route_prefix="/api2")
@serve.ingress(app)
class FastAPIWrapper2:
    @app.get("/subpath")
    def method(self):
        return "Hello 2!"

FastAPIWrapper1.deploy()
FastAPIWrapper2.deploy()
```

In this example, requests to both `/api1` and `/api2` would return `Hello from the root!` while a request to `/api1/subpath` would return `Hello 1!` and a request to `/api2/subpath` would return `Hello 2!`.

To try it out, save a code snippet in a local python file (i.e. main.py) and in the same directory, run the following commands to start a local Ray cluster on your machine.

```bash
ray start --head
python main.py
```

(serve-http-adapters)=

### HTTP Adapters

HTTP adapters are functions that convert raw HTTP request to Python types that you know and recognize.
Its input arguments should be type annotated. At minimal, it should accept a `starlette.requests.Request` type.
But it can also accept any type that's recognized by the FastAPI's dependency injection framework.

For example, here is an adapter that extra the json content from request.

```python
async def json_resolver(request: starlette.requests.Request):
    return await request.json()
```

Here is an adapter that accept two HTTP query parameters.

```python
def parse_query_args(field_a: int, field_b: str):
    return YourDataClass(field_a, field_b)
```

You can specify different type signatures to facilitate HTTP fields extraction
include
[query parameters](https://fastapi.tiangolo.com/tutorial/query-params/),
[body parameters](https://fastapi.tiangolo.com/tutorial/body/),
and [many other data types](https://fastapi.tiangolo.com/tutorial/extra-data-types/).
For more detail, you can take a look at [FastAPI documentation](https://fastapi.tiangolo.com/).

You can use adapters in different scenarios within Serve:

- Ray AIR `ModelWrapper`
- Serve Deployment Graph `DAGDriver`
- Embedded in Bring Your Own `FastAPI` Application

Let's go over them one by one.

#### Ray AIR `ModelWrapper`

Ray Serve provides a suite of adapters to convert HTTP requests to ML inputs like `numpy` arrays.
You can just use it with [Ray AI Runtime (AIR) model wrapper](air-serve-integration) feature
to one click deploy pre-trained models.

For example, we provide a simple adapter for n-dimensional array.

With [model wrappers](air-serve-integration), you can specify it via the `http_adapter` field.

```python
from ray import serve
from ray.serve.http_adapters import json_to_ndarray
from ray.serve.model_wrappers import ModelWrapperDeployment

ModelWrapperDeployment.options(name="my_model").deploy(
    my_ray_air_predictor,
    my_ray_air_checkpoint,
    http_adapter=json_to_ndarray
)
```

:::{note}
Serve also supports pydantic models as a short-hand for HTTP adapters in model wrappers. Instead of functions,
you can directly pass in a pydantic model class to mean "validate the HTTP body with this schema".
Once validated, the model instance will passed to the predictor.

```python
from pydantic import BaseModel

class User(BaseModel):
    user_id: int
    user_name: str

...
ModelWrapperDeployment.deploy(..., http_adapter=User)
```
:::

#### Serve Deployment Graph `DAGDriver`

In [Serve Deployment Graph](serve-deployment-graph), you can configure
`ray.serve.drivers.DAGDriver` to accept an http adapter via it's `http_adapter` field.

For example, the json request adapters parse JSON in HTTP body:

```python
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.serve.dag import InputNode

with InputNode() as input_node:
    ...
    dag = DAGDriver.bind(other_node, http_adapter=json_request)
```

:::{note}
Serve also supports pydantic models as a short-hand for HTTP adapters in model wrappers. Instead of functions,
you can directly pass in a pydantic model class to mean "validate the HTTP body with this schema".
Once validated, the model instance will passed as `input_node` variable.

```python
from pydantic import BaseModel

class User(BaseModel):
    user_id: int
    user_name: str

...
DAGDriver.bind(other_node, http_adapter=User)
```
:::

#### Embedded in Bring Your Own `FastAPI` Application

You can also bring the adapter to your own FastAPI app using
[Depends](https://fastapi.tiangolo.com/tutorial/dependencies/#import-depends).
The input schema will automatically be part of the generated OpenAPI schema with FastAPI.

```python
from fastapi import FastAPI, Depends
from ray.serve.http_adapters import json_to_ndarray

app = FastAPI()

@app.post("/endpoint")
async def endpoint(np_array = Depends(json_to_ndarray)):
    ...
```

It has the following schema for input:

(serve-ndarray-schema)=

```{eval-rst}
.. autopydantic_model:: ray.serve.http_adapters.NdArray

```

#### List of Built-in Adapters

Here is a list of adapters and please feel free to [contribute more](https://github.com/ray-project/ray/issues/new/choose)!

```{eval-rst}
.. automodule:: ray.serve.http_adapters
    :members: json_to_ndarray, image_to_ndarray, starlette_request, json_request

```

### Configuring HTTP Server Locations

By default, Ray Serve starts a single HTTP server on the head node of the Ray cluster.
You can configure this behavior using the `http_options={"location": ...}` flag
in {mod}`serve.start <ray.serve.start>`:

- "HeadOnly": start one HTTP server on the head node. Serve
  assumes the head node is the node you executed serve.start
  on. This is the default.
- "EveryNode": start one HTTP server per node.
- "NoServer" or `None`: disable HTTP server.

:::{note}
Using the "EveryNode" option, you can point a cloud load balancer to the
instance group of Ray cluster to achieve high availability of Serve's HTTP
proxies.
:::

### Enabling CORS and other HTTP middlewares

Serve supports arbitrary [Starlette middlewares](https://www.starlette.io/middleware/)
and custom middlewares in Starlette format. The example below shows how to enable
[Cross-Origin Resource Sharing (CORS)](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS).
You can follow the same pattern for other Starlette middlewares.

```python
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

client = serve.start(
    http_options={"middlewares": [
        Middleware(
            CORSMiddleware, allow_origins=["*"], allow_methods=["*"])
    ]})
```

(serve-handle-explainer)=

## ServeHandle: Calling Deployments from Python

Ray Serve enables you to query models both from HTTP and Python. This feature
enables seamless [model composition](serve-model-composition). You can
get a `ServeHandle` corresponding to deployment, similar how you can
reach a deployment through HTTP via a specific route. When you issue a request
to a deployment through `ServeHandle`, the request is load balanced across
available replicas in the same way an HTTP request is.

To call a Ray Serve deployment from python, use {mod}`Deployment.get_handle <ray.serve.api.Deployment>`
to get a handle to the deployment, then use
{mod}`handle.remote <ray.serve.handle.RayServeHandle.remote>` to send requests
to that deployment. These requests can pass ordinary args and kwargs that are
passed directly to the method. This returns a Ray `ObjectRef` whose result
can be waited for or retrieved using `ray.wait` or `ray.get`.

```python
@serve.deployment
class Deployment:
    def method1(self, arg):
        return f"Method1: {arg}"

    def __call__(self, arg):
        return f"__call__: {arg}"

Deployment.deploy()

handle = Deployment.get_handle()
ray.get(handle.remote("hi")) # Defaults to calling the __call__ method.
ray.get(handle.method1.remote("hi")) # Call a different method.
```

If you want to use the same deployment to serve both HTTP and ServeHandle traffic, the recommended best practice is to define an internal method that the HTTP handling logic will call:

```python
@serve.deployment(route_prefix="/api")
class Deployment:
    def say_hello(self, name: str):
        return f"Hello {name}!"

    def __call__(self, request):
        return self.say_hello(request.query_params["name"])

Deployment.deploy()
```

Now we can invoke the same logic from both HTTP or Python:

```python
print(requests.get("http://localhost:8000/api?name=Alice"))
# Hello Alice!

handle = Deployment.get_handle()
print(ray.get(handle.say_hello.remote("Alice")))
# Hello Alice!
```

(serve-sync-async-handles)=

### Sync and Async Handles

Ray Serve offers two types of `ServeHandle`. You can use the `Deployment.get_handle(..., sync=True|False)`
flag to toggle between them.

- When you set `sync=True` (the default), a synchronous handle is returned.
  Calling `handle.remote()` should return a Ray `ObjectRef`.
- When you set `sync=False`, an asyncio based handle is returned. You need to
  Call it with `await handle.remote()` to return a Ray ObjectRef. To use `await`,
  you have to run `Deployment.get_handle` and `handle.remote` in Python asyncio event loop.

The async handle has performance advantage because it uses asyncio directly; as compared
to the sync handle, which talks to an asyncio event loop in a thread. To learn more about
the reasoning behind these, checkout our [architecture documentation](serve-architecture).

## Integrating with existing web servers

Ray Serve comes with its own HTTP server out of the box, but if you have an existing
web application, you can still plug in Ray Serve to scale up your compute using the `ServeHandle`.
For a tutorial with sample code, see {ref}`serve-web-server-integration-tutorial`.
