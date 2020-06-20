import requests
import sys

from ray import serve


def test_nonblocking():
    serve.init()

    def function(flask_request):
        return {"method": flask_request.method}

    serve.create_backend("nonblocking:v1", function)
    serve.create_endpoint(
        "nonblocking", backend="nonblocking:v1", route="/nonblocking")

    resp = requests.get("http://127.0.0.1:8000/nonblocking").json()["method"]
    assert resp == "GET"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", "-s", __file__]))
