import requests
import sys

from ray import serve


def test_nonblocking():
    serve.init()
    serve.create_endpoint("nonblocking", "/nonblocking")

    def function(flask_request):
        return {"method": flask_request.method}

    serve.create_backend("nonblocking:v1", function)
    serve.set_traffic("nonblocking", {"nonblocking:v1": 1.0})

    resp = requests.get("http://127.0.0.1:8000/nonblocking").json()["method"]
    assert resp == "GET"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
