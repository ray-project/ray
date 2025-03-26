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
