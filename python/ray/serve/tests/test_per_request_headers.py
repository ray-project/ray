import asyncio

import httpx
import pytest

from ray import serve
from ray._common.test_utils import SignalActor, wait_for_condition
from ray.serve._private.constants import (
    RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT,
    RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT,
    RAY_SERVE_ENABLE_HA_PROXY,
    SERVE_HTTP_REQUEST_DISCONNECT_DISABLED_HEADER,
    SERVE_HTTP_REQUEST_TIMEOUT_S_HEADER,
)
from ray.serve._private.test_utils import get_application_url
from ray.serve.config import HTTPOptions
from ray.serve.tests.conftest import *  # noqa

import psutil


@pytest.mark.asyncio
@pytest.mark.parametrize("set_http_options_timeout", [False, True])
@pytest.mark.parametrize("timeout_header", [None, "abc", "0", "1"])
@pytest.mark.parametrize("disconnect_header", [None, "?0", "?1"])
async def test_http_request_timeout_disconnect_headers(
    ray_instance,
    ray_shutdown,
    set_http_options_timeout,
    disconnect_header,
    timeout_header,
):
    """Test cases of request timeout and disconnect header."""
    http_request_timeout_s = None
    if set_http_options_timeout:
        http_request_timeout_s = 1

    serve.start(
        http_options=HTTPOptions(
            host="0.0.0.0",
            request_timeout_s=http_request_timeout_s,
        ),
    )

    signal = SignalActor.remote()

    @serve.deployment
    class Model:
        async def __call__(self):
            await signal.wait.remote()

    serve.run(Model.bind())

    http_url = get_application_url("HTTP")

    headers = {}
    if timeout_header is not None:
        headers[
            SERVE_HTTP_REQUEST_TIMEOUT_S_HEADER.encode("utf-8")
        ] = timeout_header.encode("utf-8")
    if disconnect_header is not None:
        headers[
            SERVE_HTTP_REQUEST_DISCONNECT_DISABLED_HEADER.encode("utf-8")
        ] = disconnect_header.encode("utf-8")

    expect_timeout = (
        timeout_header == "1"
        or set_http_options_timeout is True
        and timeout_header != "0"
    )
    expect_disconnect = not expect_timeout and disconnect_header != "?1"

    async with httpx.AsyncClient() as client:
        response_task = asyncio.create_task(
            client.get(http_url, headers=headers, timeout=2)
        )
        await asyncio.sleep(0.01)

        if not expect_timeout and not expect_disconnect:
            signal.send.remote()

        if expect_disconnect:
            with pytest.raises(httpx.ReadTimeout):
                await response_task
        else:
            response = await response_task
            if expect_timeout:
                assert response.status_code == 408
            else:
                assert response.status_code == 200


@pytest.mark.skipif(
    not RAY_SERVE_ENABLE_HA_PROXY,
    reason="RAY_SERVE_ENABLE_HA_PROXY not set.",
)
def test_haproxy_default_host_is_all_interfaces(ray_shutdown):
    """When HAProxy is enabled, the default HTTPOptions.host is 0.0.0.0
    so HAProxy on other nodes can reach the replica backend ports.
    """
    serve.start(http_options=HTTPOptions())

    @serve.deployment
    class App:
        async def __call__(self):
            return "ok"

    serve.run(App.bind())

    def _direct_ingress_listeners():
        return [
            c
            for c in psutil.net_connections(kind="tcp")
            if c.status == psutil.CONN_LISTEN
            and RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT
            <= c.laddr.port
            <= RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT
        ]

    wait_for_condition(lambda: len(_direct_ingress_listeners()) > 0, timeout=30)

    for conn in _direct_ingress_listeners():
        assert conn.laddr.ip == "0.0.0.0", (
            f"direct ingress port {conn.laddr.port} bound to {conn.laddr.ip!r}, "
            "expected '0.0.0.0'"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
