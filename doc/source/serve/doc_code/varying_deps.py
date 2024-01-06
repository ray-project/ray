import requests
from starlette.requests import Request

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class Ingress:
    def __init__(
        self, ver_25_handle: DeploymentHandle, ver_26_handle: DeploymentHandle
    ):
        self.ver_25_handle = ver_25_handle
        self.ver_26_handle = ver_26_handle

    async def __call__(self, request: Request):
        if request.query_params["version"] == "25":
            return await self.ver_25_handle.remote()
        else:
            return await self.ver_26_handle.remote()


@serve.deployment
def requests_version():
    return requests.__version__


ver_25 = requests_version.options(
    name="25",
    ray_actor_options={"runtime_env": {"pip": ["requests==2.25.1"]}},
).bind()
ver_26 = requests_version.options(
    name="26",
    ray_actor_options={"runtime_env": {"pip": ["requests==2.26.0"]}},
).bind()

app = Ingress.bind(ver_25, ver_26)
serve.run(app)

assert requests.get("http://127.0.0.1:8000/?version=25").text == "2.25.1"
assert requests.get("http://127.0.0.1:8000/?version=26").text == "2.26.0"
