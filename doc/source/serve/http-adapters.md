
(serve-http-adapters)=

# HTTP Adapters

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

You can use adapters in different scenarios within Serve, which we will go over one by one:

- Ray AIR `Predictor`
- Serve Deployment Graph `DAGDriver`
- Embedded in Bring Your Own `FastAPI` Application

## Ray AIR `Predictor`

Ray Serve provides a suite of adapters to convert HTTP requests to ML inputs like `numpy` arrays.
You can use them together with the [Ray AI Runtime (AIR) model wrapper](air-serving-guide) feature
to one-click deploy pre-trained models.

As an example, we provide a simple adapter for an *n*-dimensional array.

When using [model wrappers](air-serving-guide), you can specify your HTTP adapter via the `http_adapter` field:

```python
from ray import serve
from ray.serve.http_adapters import json_to_ndarray
from ray.serve import PredictorDeployment

PredictorDeployment.options(name="my_model").deploy(
    my_ray_air_predictor,
    my_ray_air_checkpoint,
    http_adapter=json_to_ndarray
)
```

## Serve Deployment Graph `DAGDriver`

When using a [Serve Deployment Graph](serve-deployment-graph), you can configure
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

## Embedded in your existing `FastAPI` Application

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

## Pydantic models as adapters

Serve also supports [pydantic models](https://pydantic-docs.helpmanual.io/usage/models/) as a shorthand for HTTP adapters in model wrappers. Instead of using a function to define your HTTP adapter as in the examples above,
you can directly pass in a pydantic model class to effectively tell Ray Serve "validate the HTTP body with this schema."
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
## List of Built-in Adapters

Here is a list of adapters; please feel free to [contribute more](https://github.com/ray-project/ray/issues/new/choose)!

```{eval-rst}
.. automodule:: ray.serve.http_adapters
    :members: json_to_ndarray, image_to_ndarray, starlette_request, json_request, pandas_read_json, json_to_multi_ndarray

```
