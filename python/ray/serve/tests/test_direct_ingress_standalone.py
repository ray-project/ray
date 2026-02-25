import asyncio

import httpx
import pytest

from ray import serve
from ray._common.test_utils import SignalActor
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
    SERVE_HTTP_REQUEST_DISCONNECT_DISABLED_HEADER,
    SERVE_HTTP_REQUEST_TIMEOUT_S_HEADER,
)
from ray.serve._private.test_utils import get_application_url
from ray.serve.config import HTTPOptions
from ray.serve.tests.conftest import *  # noqa


@pytest.fixture
def _skip_if_ff_not_enabled():
    if not RAY_SERVE_ENABLE_DIRECT_INGRESS:
        pytest.skip(
            reason="RAY_SERVE_ENABLE_DIRECT_INGRESS not set.",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("set_http_options_timeout", [False, True])
@pytest.mark.parametrize("timeout_header", [None, "abc", "0", "1"])
@pytest.mark.parametrize("disconnect_header", [None, "?0", "?1"])
async def test_http_request_timeout_disconnect_headers(
    _skip_if_ff_not_enabled,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
