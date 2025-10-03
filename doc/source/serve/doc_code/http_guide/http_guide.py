# flake8: noqa

# __begin_starlette__
import starlette.requests
import requests
from ray import serve


@serve.deployment
class Counter:
    def __call__(self, request: starlette.requests.Request):
        return request.query_params


serve.run(Counter.bind())
resp = requests.get("http://localhost:8000?a=b&c=d")
assert resp.json() == {"a": "b", "c": "d"}
# __end_starlette__

# __begin_fastapi__
import ray
import requests
from fastapi import FastAPI
from ray import serve

app = FastAPI()


@serve.deployment
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"


serve.run(MyFastAPIDeployment.bind(), route_prefix="/hello")
resp = requests.get("http://localhost:8000/hello")
assert resp.json() == "Hello, world!"
# __end_fastapi__


# __begin_fastapi_multi_routes__
import ray
import requests
from fastapi import FastAPI
from ray import serve

app = FastAPI()


@serve.deployment
@serve.ingress(app)
class MyFastAPIDeployment:
    @app.get("/")
    def root(self):
        return "Hello, world!"

    @app.post("/{subpath}")
    def root(self, subpath: str):
        return f"Hello from {subpath}!"


serve.run(MyFastAPIDeployment.bind(), route_prefix="/hello")
resp = requests.post("http://localhost:8000/hello/Serve")
assert resp.json() == "Hello from Serve!"
# __end_fastapi_multi_routes__

# __begin_byo_fastapi__
import ray
import requests
from fastapi import FastAPI
from ray import serve

app = FastAPI()


@app.get("/")
def f():
    return "Hello from the root!"


@serve.deployment
@serve.ingress(app)
class FastAPIWrapper:
    pass


serve.run(FastAPIWrapper.bind(), route_prefix="/")
resp = requests.get("http://localhost:8000/")
assert resp.json() == "Hello from the root!"
# __end_byo_fastapi__


# __begin_fastapi_factory_pattern__
import requests
from fastapi import FastAPI
from ray import serve
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor


@serve.deployment
class ChildDeployment:
    def __call__(self):
        return "Hello from the child deployment!"


def fastapi_factory():
    """Factory-style FastAPI app used as Serve ingress.

    We build the FastAPI app inside a factory and pass the callable to
    @serve.ingress.
    """
    app = FastAPI()

    # In an object-based ingress (where the FastAPI app is stored on the
    # deployment instance), Ray would need to serialize the app and its
    # instrumentation. Some instrumentors (like FastAPIInstrumentor) are not
    # picklable, which can cause serialization failures. Creating and
    # instrumenting the app here sidesteps that issue.
    FastAPIInstrumentor.instrument_app(app)

    @app.get("/")
    async def root():
        # Handlers defined inside this factory don't have access to the
        # ParentDeployment instance (i.e., there's no `self` here), so we
        # can't call `self.child`. Instead, fetch a handle by deployment name.
        handle = serve.get_deployment_handle("ChildDeployment", app_name="default")
        return {"message": await handle.remote()}

    return app


@serve.deployment
@serve.ingress(fastapi_factory)
class ParentDeployment:
    def __init__(self, child):
        self.child = child


serve.run(ParentDeployment.bind(ChildDeployment.bind()))

resp = requests.get("http://localhost:8000/")
assert resp.json() == {"message": "Hello from the child deployment!"}
# __end_fastapi_factory_pattern__
