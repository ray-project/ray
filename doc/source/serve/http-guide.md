(serve-set-up-fastapi-http)=
# Set Up FastAPI and HTTP

This section helps you understand how to:
- send HTTP requests to Serve deployments
- use Ray Serve to integrate with FastAPI
- use customized HTTP adapters
- choose which feature to use for your use case

## Choosing the right HTTP feature

Serve offers a layered approach to expose your model with the right HTTP API.

Considering your use case, you can choose the right level of abstraction:
- If you are comfortable working with the raw request object, use [`starlette.request.Requests` API](serve-http).
- If you want a fully fledged API server with validation and doc generation, use the [FastAPI integration](serve-fastapi-http).
- If you just want a pre-defined HTTP schema, use the [`DAGDriver` with `http_adapter`](serve-http-adapters).


(serve-http)=
## Calling Deployments via HTTP
When you deploy a Serve application, the [ingress deployment](serve-key-concepts-ingress-deployment) (the one passed to `serve.run`) is exposed over HTTP.

```{literalinclude} doc_code/http_guide/http_guide.py
:start-after: __begin_starlette__
:end-before: __end_starlette__
:language: python
```

Requests to the Serve HTTP server at `/` are routed to the deployment's `__call__` method with a [Starlette Request object](https://www.starlette.io/requests/) as the sole argument. The `__call__` method can return any JSON-serializable object or a [Starlette Response object](https://www.starlette.io/responses/) (e.g., to return a custom status code or custom headers).

Often for ML models, you just need the API to accept a `numpy` array. You can use Serve's `DAGDriver` to simplify the request parsing.

```{literalinclude} doc_code/http_guide/http_guide.py
:start-after: __begin_dagdriver__
:end-before: __end_dagdriver__
:language: python
```

```{note}
Serve provides a library of HTTP adapters to help you avoid boilerplate code. The [later section](serve-http-adapters) dives deeper into how these works.
```

(serve-fastapi-http)=
## FastAPI HTTP Deployments

If you want to define more complex HTTP handling logic, Serve integrates with [FastAPI](https://fastapi.tiangolo.com/). This allows you to define a Serve deployment using the {mod}`@serve.ingress <ray.serve.api.ingress>` decorator that wraps a FastAPI app with its full range of features. The most basic example of this is shown below, but for more details on all that FastAPI has to offer such as variable routes, automatic type validation, dependency injection (e.g., for database connections), and more, please check out [their documentation](https://fastapi.tiangolo.com/).

```{literalinclude} doc_code/http_guide/http_guide.py
:start-after: __begin_fastapi__
:end-before: __end_fastapi__
:language: python
```

Now if you send a request to `/hello`, this will be routed to the `root` method of our deployment. We can also easily leverage FastAPI to define multiple routes with different HTTP methods:

```{literalinclude} doc_code/http_guide/http_guide.py
:start-after: __begin_fastapi_multi_routes__
:end-before: __end_fastapi_multi_routes__
:language: python
```

You can also pass in an existing FastAPI app to a deployment to serve it as-is:

```{literalinclude} doc_code/http_guide/http_guide.py
:start-after: __begin_byo_fastapi__
:end-before: __end_byo_fastapi__
:language: python
```

This is useful for scaling out an existing FastAPI app with no modifications necessary.
Existing middlewares, **automatic OpenAPI documentation generation**, and other advanced FastAPI features should work as-is.

### WebSockets

Serve supports WebSockets via FastAPI:

```{literalinclude} doc_code/http_guide/websockets_example.py
:start-after: __websocket_serve_app_start__
:end-before: __websocket_serve_app_end__
:language: python
```

Decorate the function that handles WebSocket requests with `@app.websocket`. Read more about FastAPI WebSockets in the [FastAPI documentation](https://fastapi.tiangolo.com/advanced/websockets/).

Query the deployment using the `websockets` package (`pip install websockets`):

```{literalinclude} doc_code/http_guide/websockets_example.py
:start-after: __websocket_serve_client_start__
:end-before: __websocket_serve_client_end__
:language: python
```

(serve-http-streaming-response)=
## Streaming Responses

Some applications must stream incremental results back to the caller.
This is common for text generation using large language models (LLMs) or video processing applications.
The full forward pass may take multiple seconds, so providing incremental results as they're available provides a much better user experience.

To use HTTP response streaming, return a [StreamingResponse](https://www.starlette.io/responses/#streamingresponse) that wraps a generator from your HTTP handler.
This is supported for basic HTTP ingress deployments using a `__call__` method and when using the [FastAPI integration](serve-fastapi-http).

The code below defines a Serve application that incrementally streams numbers up to a provided `max`.
The client-side code is also updated to handle the streaming outputs.
This code uses the `stream=True` option to the [requests](https://requests.readthedocs.io/en/latest/user/advanced/#streaming-requests) library.

```{literalinclude} doc_code/http_guide/streaming_example.py
:start-after: __begin_example__
:end-before: __end_example__
:language: python
```

Save this code in `stream.py` and run it:

```bash
$ python stream.py
[2023-05-25 10:44:23]  INFO ray._private.worker::Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
(ServeController pid=40401) INFO 2023-05-25 10:44:25,296 controller 40401 deployment_state.py:1259 - Deploying new version of deployment default_StreamingResponder.
(HTTPProxyActor pid=40403) INFO:     Started server process [40403]
(ServeController pid=40401) INFO 2023-05-25 10:44:25,333 controller 40401 deployment_state.py:1498 - Adding 1 replica to deployment default_StreamingResponder.
Got result 0.0s after start: '0'
Got result 0.1s after start: '1'
Got result 0.2s after start: '2'
Got result 0.3s after start: '3'
Got result 0.4s after start: '4'
Got result 0.5s after start: '5'
Got result 0.6s after start: '6'
Got result 0.7s after start: '7'
Got result 0.8s after start: '8'
Got result 0.9s after start: '9'
(ServeReplica:default_StreamingResponder pid=41052) INFO 2023-05-25 10:49:52,230 default_StreamingResponder default_StreamingResponder#qlZFCa yomKnJifNJ / default replica.py:634 - __CALL__ OK 1017.6ms
```

### Handling client disconnects

In some cases, you may want to cease processing a request when the client disconnects before the full stream has been returned.
If you pass an async generator to `StreamingResponse`, it will be cancelled and raise an `asyncio.CancelledError` when the client disconnects.
Note that you must `await` at some point in the generator for the cancellation to occur.

In the example below, the generator streams responses forever until the client disconnects, then it prints that it was cancelled and exits. Save this code in `stream.py` and run it:


```{literalinclude} doc_code/http_guide/streaming_example.py
:start-after: __begin_cancellation__
:end-before: __end_cancellation__
:language: python
```

```bash
$ python stream.py
[2023-07-10 16:08:41]  INFO ray._private.worker::Started a local Ray instance. View the dashboard at http://127.0.0.1:8265
(ServeController pid=50801) INFO 2023-07-10 16:08:42,296 controller 40401 deployment_state.py:1259 - Deploying new version of deployment default_StreamingResponder.
(HTTPProxyActor pid=50803) INFO:     Started server process [50803]
(ServeController pid=50805) INFO 2023-07-10 16:08:42,963 controller 50805 deployment_state.py:1586 - Adding 1 replica to deployment default_StreamingResponder.
Got result 0.0s after start: '0'
Got result 0.1s after start: '1'
Got result 0.2s after start: '2'
Got result 0.3s after start: '3'
Got result 0.4s after start: '4'
Got result 0.5s after start: '5'
Got result 0.6s after start: '6'
Got result 0.7s after start: '7'
Got result 0.8s after start: '8'
Got result 0.9s after start: '9'
Got result 1.0s after start: '10'
Client disconnecting
(ServeReplica:default_StreamingResponder pid=50842) Cancelled! Exiting.
(ServeReplica:default_StreamingResponder pid=50842) INFO 2023-07-10 16:08:45,756 default_StreamingResponder default_StreamingResponder#cmpnmF ahteNDQSWx / default replica.py:691 - __CALL__ OK 1019.1ms
```

(serve-http-adapters)=

## HTTP Adapters
HTTP adapters are functions that convert raw HTTP requests to basic Python types that you know and recognize.

For example, here is an adapter that extracts the JSON content from a request:

```{testcode}
import starlette


async def json_resolver(request: starlette.requests.Request):
    return await request.json()
```

```{testoutput}
:hide:

...
```

The input arguments to an HTTP adapter should be type-annotated. At a minimum, the adapter should accept a `starlette.requests.Request` type (https://www.starlette.io/requests/#request),
but it can also accept any type that's recognized by [FastAPI's dependency injection framework](https://fastapi.tiangolo.com/tutorial/dependencies/).

Here is an HTTP adapter that accepts two HTTP query parameters:

```{testcode}
class YourDataClass:
    def __call__(field_a: int, field_b: str):
        return f"field_a: {field_a}, field_b: {field_b}"


def parse_query_args(field_a: int, field_b: str):
    return YourDataClass(field_a, field_b)
```

```{testoutput}
:hide:

...
```

You can specify different type signatures to facilitate the extraction of HTTP fields, including
- [query parameters](https://fastapi.tiangolo.com/tutorial/query-params/),
- [body parameters](https://fastapi.tiangolo.com/tutorial/body/),
- [many other data types](https://fastapi.tiangolo.com/tutorial/extra-data-types/).

For more details, you can take a look at the [FastAPI documentation](https://fastapi.tiangolo.com/).

In addition to above adapters, you also use other adapters. Below we examine at least two:

- Serve Deployment Graph `DAGDriver`
- Embedded in Bring Your Own `FastAPI` Application

### Serve Deployment Graph `DAGDriver`

When using a [Serve deployment graph](serve-deployment-graphs), you can configure
`ray.serve.drivers.DAGDriver` to accept an HTTP adapter via its `http_adapter` field.

For example, the `json_request` adapter parses JSON in the HTTP body:

```{testcode}
from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.dag.input_node import InputNode


@serve.deployment()
async def func1(number: float) -> float:
    return number + 5


with InputNode() as input_node:
    other_node = func1.bind(input_node)
    dag = DAGDriver.bind(other_node, http_adapter=json_request)
```

```{testoutput}
:hide:

...
```

### Embedded in your existing `FastAPI` Application

You can also bring the adapter to your own FastAPI app using
[Depends](https://fastapi.tiangolo.com/tutorial/dependencies/#import-depends).
The input schema automatically become part of the generated OpenAPI schema with FastAPI.

```{testcode}
from fastapi import FastAPI, Depends
from ray.serve.http_adapters import json_to_ndarray

app = FastAPI()

@app.post("/endpoint")
async def endpoint(np_array = Depends(json_to_ndarray)):
    return np_array[0]
```

```{testoutput}
:hide:

...
```

### Pydantic models as adapters

Serve also supports [pydantic models](https://pydantic-docs.helpmanual.io/usage/models/) as a shorthand for HTTP adapters in model wrappers. Instead of using a function to define your HTTP adapter as in the examples above,
you can directly pass in a pydantic model class to effectively tell Ray Serve to validate the HTTP body with this schema.
Once validated, the model instance will passed to the predictor.

```{testcode}
from pydantic import BaseModel
from ray.serve.drivers import DAGDriver


@serve.deployment()
async def func1(number: float) -> float:
    return number + 5


class User(BaseModel):
    user_id: int
    user_name: str

other_node = func1.bind(input_node)
DAGDriver.bind(other_node, http_adapter=User)
```

```{testoutput}
:hide:

...
```

### List of built-in adapters

Here is a list of adapters; please feel free to [contribute more](https://github.com/ray-project/ray/issues/new/choose)!

(serve-ndarray-schema)=

```{eval-rst}
.. automodule:: ray.serve.http_adapters
    :members: json_to_ndarray, image_to_ndarray, starlette_request, json_request, pandas_read_json, json_to_multi_ndarray

```
