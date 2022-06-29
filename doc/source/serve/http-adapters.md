
(serve-http-adapters)=

# HTTP Adapters

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

## Ray AIR `ModelWrapper`

Ray Serve provides a suite of adapters to convert HTTP requests to ML inputs like `numpy` arrays.
You can use it with [Ray AI Runtime (AIR) model wrapper](air-serving-guide) feature
to one click deploy pre-trained models.

For example, we provide a simple adapter for n-dimensional array.

With [model wrappers](air-serving-guide), you can specify it via the `http_adapter` field.

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

## Serve Deployment Graph `DAGDriver`

In [Serve Deployment Graph](serve-deployment-graph), you can configure
`ray.serve.drivers.DAGDriver` to accept an http adapter via it's `http_adapter` field.

For example, the json request adapters parse JSON in HTTP body:

```python
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.dag.input_node import InputNode

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

## Embedded in Bring Your Own `FastAPI` Application

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

## List of Built-in Adapters

Here is a list of adapters and please feel free to [contribute more](https://github.com/ray-project/ray/issues/new/choose)!

```{eval-rst}
.. automodule:: ray.serve.http_adapters
    :members: json_to_ndarray, image_to_ndarray, starlette_request, json_request, pandas_read_json, json_to_multi_ndarray

```