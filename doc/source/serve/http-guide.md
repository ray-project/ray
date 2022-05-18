# Calling Deployments via HTTP


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

