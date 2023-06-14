import pytest
import requests
import starlette
import sys

from ray import serve


def test_callback(serve_instance, capsys):
    """Test callback function works in http proxy and controller

    Using: python/ray/serve/tests/resources/callback.py
    """

    @serve.deployment
    class Model:
        def __call__(self, request: starlette.requests.Request):
            headers = request.scope.get("headers")
            for k, v in headers:
                if k == b"custom_header_key":
                    return v
            return "Not found custom headers"

    serve.run(Model.bind())
    resp = requests.get("http://localhost:8000/")
    assert resp.text == "custom_header_value"

    captured = capsys.readouterr()
    assert "MyCustom message: hello" in captured.err


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
