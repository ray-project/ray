(serve-set-up-fastapi-http)=
# Set Up FastAPI and HTTP

This section helps you understand how to:
- Send HTTP requests to Serve deployments
- Use Ray Serve to integrate with FastAPI
- Use customized HTTP adapters
- Choose which feature to use for your use case
- Set up keep alive timeout

## Choosing the right HTTP feature

Serve offers a layered approach to expose your model with the right HTTP API.

Considering your use case, you can choose the right level of abstraction:
- If you are comfortable working with the raw request object, use [`starlette.request.Requests` API](serve-http).
- If you want a fully fledged API server with validation and doc generation, use the [FastAPI integration](serve-fastapi-http).


(serve-http)=
## Calling Deployments via HTTP
When you deploy a Serve application, the [ingress deployment](serve-key-concepts-ingress-deployment) (the one passed to `serve.run`) is exposed over HTTP.

```{literalinclude} doc_code/http_guide/http_guide.py
:start-after: __begin_starlette__
:end-before: __end_starlette__
:language: python
```

Requests to the Serve HTTP server at `/` are routed to the deployment's `__call__` method with a [Starlette Request object](https://www.starlette.io/requests/) as the sole argument. The `__call__` method can return any JSON-serializable object or a [Starlette Response object](https://www.starlette.io/responses/) (e.g., to return a custom status code or custom headers). A Serve app's route prefix can be changed from `/` to another string by setting `route_prefix` in `serve.run()` or the Serve config file.

(serve-request-cancellation-http)=
### Request cancellation

When processing a request takes longer than the [end-to-end timeout](serve-performance-e2e-timeout) or an HTTP client disconnects before receiving a response, Serve cancels the in-flight request:

- If the proxy hasn't yet sent the request to a replica, Serve simply drops the request.
- If the request has been sent to a replica, Serve attempts to interrupt the replica and cancel the request. The `asyncio.Task` running the handler on the replica is cancelled, raising an `asyncio.CancelledError` the next time it enters an `await` statement. See [the asyncio docs](https://docs.python.org/3/library/asyncio-task.html#task-cancellation) for more info. Handle this exception in a try-except block to customize your deployment's behavior when a request is cancelled:

```{literalinclude} doc_code/http_guide/disconnects.py
:start-after: __start_basic_disconnect__
:end-before: __end_basic_disconnect__
:language: python
```

If no `await` statements are left in the deployment's code before the request completes, the replica processes the request as usual, sends the response back to the proxy, and the proxy discards the response. Use `await` statements for blocking operations in a deployment, so Serve can cancel in-flight requests without waiting for the blocking operation to complete.

Cancellation cascades to any downstream deployment handle, task, or actor calls that were spawned in the deployment's request-handling method. These can handle the `asyncio.CancelledError` in the same way as the ingress deployment.

To prevent an async call from being interrupted by `asyncio.CancelledError`, use `asyncio.shield()`:

```{literalinclude} doc_code/http_guide/disconnects.py
:start-after: __start_shielded_disconnect__
:end-before: __end_shielded_disconnect__
:language: python
```

When the request is cancelled, a cancellation error is raised inside the `SnoringSleeper` deployment's `__call__()` method. However, the cancellation is not raised inside the `snore()` call, so `ZZZ` is printed even if the request is cancelled. Note that `asyncio.shield` cannot be used on a `DeploymentHandle` call to prevent the downstream handler from being cancelled. You need to explicitly handle the cancellation error in that handler as well.

(serve-fastapi-http)=
## FastAPI HTTP Deployments

If you want to define more complex HTTP handling logic, Serve integrates with [FastAPI](https://fastapi.tiangolo.com/). This allows you to define a Serve deployment using the {mod}`@serve.ingress <ray.serve.ingress>` decorator that wraps a FastAPI app with its full range of features. The most basic example of this is shown below, but for more details on all that FastAPI has to offer such as variable routes, automatic type validation, dependency injection (e.g., for database connections), and more, please check out [their documentation](https://fastapi.tiangolo.com/).

:::{note}
A Serve application that's integrated with FastAPI still respects the `route_prefix` set through Serve. The routes are that registered through the FastAPI `app` object are layered on top of the route prefix. For instance, if your Serve application has `route_prefix = /my_app` and you decorate a method with `@app.get("/fetch_data")`, then you can call that method by sending a GET request to the path `/my_app/fetch_data`.
:::
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
This code uses the `stream=True` option to the [requests](https://requests.readthedocs.io/en/latest/user/advanced.html#streaming-requests) library.

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
(ProxyActor pid=40403) INFO:     Started server process [40403]
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

### Terminating the stream when a client disconnects

In some cases, you may want to cease processing a request when the client disconnects before the full stream has been returned.
If you pass an async generator to `StreamingResponse`, it is cancelled and raises an `asyncio.CancelledError` when the client disconnects.
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
(ProxyActor pid=50803) INFO:     Started server process [50803]
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


(serve-http-guide-keep-alive-timeout)=
## Set keep alive timeout

Serve uses a Uvicorn HTTP server internally to serve HTTP requests. By default, Uvicorn
keeps HTTP connections alive for 5 seconds between requests. Modify the keep-alive
timeout by setting the `keep_alive_timeout_s` in the `http_options` field of the Serve
config files. This config is global to your Ray cluster, and you can't update it during
runtime. You can also set the `RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S` environment variable to
set the keep alive timeout. `RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S` takes
precedence over the `keep_alive_timeout_s` config if both are set. See
Uvicorn's keep alive timeout [guide](https://www.uvicorn.org/server-behavior/#timeouts) for more information.
