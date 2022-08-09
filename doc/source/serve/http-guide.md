# HTTP with Serve



This section should help you understand how to:

- send HTTP requests to Serve deployments
- use Ray Serve to integrate with FastAPI
- use customized HTTP Adapters


:::{note}
HTTP Proxy HA is enabled by using [REST API](serve-in-production-deploying) or [Kubernetes operator](deploying-serve-on-kubernetes) to start the Ray Serve
:::

(serve-http)=

## Calling Deployments via HTTP

When you deploy a Serve application, the ingress deployment (the one passed to `serve.run`) will be exposed over HTTP. If you want to route to another deployment, you can do so using the [ServeHandle API](serve-model-composition).

```python
@serve.deployment
class Counter:
    def __call__(self, request):
        pass
```

Any request to the Serve HTTP server at `/` is routed to the deployment's `__call__` method with a [Starlette Request object](https://www.starlette.io/requests/) as the sole argument. The `__call__` method can return any JSON-serializable object or a [Starlette Response object](https://www.starlette.io/responses/) (e.g., to return a custom status code).

Below, we discuss some advanced features for customizing Ray Serve's HTTP functionality.

(serve-fastapi-http)=

## FastAPI HTTP Deployments

If you want to define more complex HTTP handling logic, Serve integrates with [FastAPI](https://fastapi.tiangolo.com/). This allows you to define a Serve deployment using the {mod}`@serve.ingress <ray.serve.api.ingress>` decorator that wraps a FastAPI app with its full range of features. The most basic example of this is shown below, but for more details on all that FastAPI has to offer such as variable routes, automatic type validation, dependency injection (e.g., for database connections), and more, please check out [their documentation](https://fastapi.tiangolo.com/).

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")

@serve.deployment(route_prefix="/hello")
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"

serve.run(MyFastAPIDeployment.bind())
```

Now if you send a request to `/hello`, this will be routed to the `root` method of our deployment. We can also easily leverage FastAPI to define multiple routes with different HTTP methods:

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")

@serve.deployment(route_prefix="/hello")
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"

    @app.post("/{subpath}")
    def root(self, subpath: str):
        return f"Hello from {subpath}!"

serve.run(MyFastAPIDeployment.bind())
```

You can also pass in an existing FastAPI app to a deployment to serve it as-is:

```python
import ray

from fastapi import FastAPI
from ray import serve

app = FastAPI()
ray.init(address="auto", namespace="summarizer")

@app.get("/")
def f():
    return "Hello from the root!"

# ... add more routes, routers, etc. to `app` ...

@serve.deployment(route_prefix="/")
@serve.ingress(app)
class FastAPIWrapper:
    pass

serve.run(FastAPIWrapper.bind())
```

This is useful for scaling out an existing FastAPI app with no modifications necessary.
Existing middlewares, automatic OpenAPI documentation generation, and other advanced FastAPI features should work as-is.

To try it out, save a code snippet in a local python file (e.g. `main.py`) and in the same directory, run the following commands to start a local Ray cluster on your machine.

```bash
ray start --head
python main.py
```

(serve-http-adapters)=

## HTTP Adapters

HTTP adapters are functions that convert raw HTTP requests to basic Python types that you know and recognize.

For example, here is an adapter that extracts the JSON content from a request:

```python
async def json_resolver(request: starlette.requests.Request):
    return await request.json()
```

The input arguments to an HTTP adapter should be type-annotated. At a minimum, the adapter should accept a `starlette.requests.Request` type (https://www.starlette.io/requests/#request),
but it can also accept any type that's recognized by [FastAPI's dependency injection framework](https://fastapi.tiangolo.com/tutorial/dependencies/).

Here is an HTTP adapter that accepts two HTTP query parameters:

```python
def parse_query_args(field_a: int, field_b: str):
    return YourDataClass(field_a, field_b)
```

You can specify different type signatures to facilitate the extraction of HTTP fields, including
- [query parameters](https://fastapi.tiangolo.com/tutorial/query-params/),
- [body parameters](https://fastapi.tiangolo.com/tutorial/body/),
and 
- [many other data types](https://fastapi.tiangolo.com/tutorial/extra-data-types/).

For more details, you can take a look at the [FastAPI documentation](https://fastapi.tiangolo.com/).

In addition to above adapters, you also use other adapters. Below we examine at least three:

- Ray AIR `Predictor`
- Serve Deployment Graph `DAGDriver`
- Embedded in Bring Your Own `FastAPI` Application

### Ray AIR `Predictor`

Ray Serve provides a suite of adapters to convert HTTP requests to ML inputs like `numpy` arrays.
You can use them together with the [Ray AI Runtime (AIR) model wrapper](air-serving-guide) feature
to one-click deploy pre-trained models.

As an example, we provide a simple adapter for an *n*-dimensional array.

When using [model wrappers](air-serving-guide), you can specify your HTTP adapter via the `http_adapter` field:

```python
from ray import serve
from ray.serve.http_adapters import json_to_ndarray
from ray.serve import PredictorDeployment

serve.run(PredictorDeployment.options(name="my_model").bind(
    my_ray_air_predictor,
    my_ray_air_checkpoint,
    http_adapter=json_to_ndarray
))
```

:::{note}
`my_ray_air_predictor` and `my_ray_air_checkpoint` are two arguments int `PredictorDeployment` constructor. For detailed usage, please checkout [Ray AI Runtime (AIR) model wrapper](air-serving-guide)
:::

### Serve Deployment Graph `DAGDriver`

When using a [Serve deployment graph](serve-model-composition-deployment-graph), you can configure
`ray.serve.drivers.DAGDriver` to accept an HTTP adapter via its `http_adapter` field.

For example, the `json_request` adapter parses JSON in the HTTP body:

```python
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.dag.input_node import InputNode

with InputNode() as input_node:
    # ...
    dag = DAGDriver.bind(other_node, http_adapter=json_request)
```

### Embedded in your existing `FastAPI` Application

You can also bring the adapter to your own FastAPI app using
[Depends](https://fastapi.tiangolo.com/tutorial/dependencies/#import-depends).
The input schema automatically become part of the generated OpenAPI schema with FastAPI.

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

### Pydantic models as adapters

Serve also supports [pydantic models](https://pydantic-docs.helpmanual.io/usage/models/) as a shorthand for HTTP adapters in model wrappers. Instead of using a function to define your HTTP adapter as in the examples above,
you can directly pass in a pydantic model class to effectively tell Ray Serve to validate the HTTP body with this schema.
Once validated, the model instance will passed to the predictor.

```python
from pydantic import BaseModel

class User(BaseModel):
    user_id: int
    user_name: str

# ...

PredictorDeployment.deploy(..., http_adapter=User)
# Or:
DAGDriver.bind(other_node, http_adapter=User)

```
### List of Built-in Adapters

Here is a list of adapters; please feel free to [contribute more](https://github.com/ray-project/ray/issues/new/choose)!

```{eval-rst}
.. automodule:: ray.serve.http_adapters
    :members: json_to_ndarray, image_to_ndarray, starlette_request, json_request, pandas_read_json, json_to_multi_ndarray

```

