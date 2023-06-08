import pytest
import requests
from fastapi import FastAPI
import starlette

from ray import serve
from ray.serve._private.constants import RAY_SERVE_REQUEST_ID


@pytest.mark.parametrize("deploy_type", ["basic", "fastapi", "starlette_resp"])
def test_http_request_id(ray_shutdown, deploy_type):
    """Test request id in http headers."""

    # Non FastAPI
    if deploy_type == "fastapi":
        app = FastAPI()

        @serve.deployment
        @serve.ingress(app)
        class Model:
            @app.get("/")
            def say_hi(self) -> int:
                return 1

    elif deploy_type == "basic":

        @serve.deployment
        class Model:
            def __call__(self) -> int:
                return 1

    else:

        @serve.deployment
        class Model:
            def __call__(self) -> int:
                return starlette.responses.Response("1", media_type="application/json")

    serve.run(Model.bind())

    resp = requests.get(
        "http://localhost:8000", headers={RAY_SERVE_REQUEST_ID: "123-234"}
    )
    assert resp.status_code == 200
    assert resp.json() == 1
    assert RAY_SERVE_REQUEST_ID in resp.headers
    assert resp.headers[RAY_SERVE_REQUEST_ID] == "123-234"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
