import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from ray.serve._private.constants import (
    SERVE_INGRESS_ROUTER_REQUEST_METHOD_HEADER,
    SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER,
    SERVE_INGRESS_ROUTER_REQUEST_QUERY_HEADER,
)
from ray.serve._private.http_util import make_buffered_asgi_receive
from ray.serve._private.ingress_request_router import IngressRequestRouter


class FakeHandle:
    def _init(self):
        self.routing_request = None

    @asynccontextmanager
    async def choose_replica(self, request, *, _reserve):
        assert not _reserve
        self.routing_request = request
        yield SimpleNamespace(
            _replica=SimpleNamespace(
                replica_id=SimpleNamespace(to_full_id_str=lambda: "replica")
            )
        )


@pytest.mark.asyncio
async def test_routes_with_reconstructed_original_request():
    body = b'{"prompt":"hello"}'
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/internal/route",
        "raw_path": b"/internal/route",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (b"host", b"router"),
            (b"content-type", b"application/json"),
            (b"x-client-header", b"value"),
            (SERVE_INGRESS_ROUTER_REQUEST_METHOD_HEADER.encode(), b"PUT"),
            (
                SERVE_INGRESS_ROUTER_REQUEST_PATH_HEADER.encode(),
                b"/items/hello%20world",
            ),
            (
                SERVE_INGRESS_ROUTER_REQUEST_QUERY_HEADER.encode(),
                b"tenant=a&tenant=b",
            ),
        ],
        "client": ("127.0.0.1", 1234),
        "server": ("127.0.0.1", 8000),
    }

    handle = FakeHandle()
    router = IngressRequestRouter.func_or_class(handle)
    await router(Request(scope, make_buffered_asgi_receive(body)))
    reconstructed = handle.routing_request

    assert reconstructed.method == "PUT"
    assert reconstructed.url.path == "/items/hello world"
    assert reconstructed.scope["raw_path"] == b"/items/hello%20world"
    assert reconstructed.query_params.getlist("tenant") == ["a", "b"]
    assert reconstructed.headers["x-client-header"] == "value"
    assert not any(name.startswith("x-serve-router-") for name in reconstructed.headers)
    assert await reconstructed.body() == body


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
