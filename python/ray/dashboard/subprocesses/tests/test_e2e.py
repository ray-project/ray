import asyncio
import sys

import aiohttp.web
import pytest

from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
from ray.dashboard.subprocesses.message import (
    ErrorMessage,
    RequestMessage,
    ResponseMessage,
)
from ray.dashboard.subprocesses.module import SubprocessModuleConfig
from ray.dashboard.subprocesses.routes import SubprocessRouteTable
from ray.dashboard.subprocesses.tests.utils import TestModule


@pytest.mark.asyncio
async def test_handle_can_health_check():
    loop = asyncio.get_event_loop()

    subprocess_handle = SubprocessModuleHandle(
        loop, TestModule, SubprocessModuleConfig(session_name="test", config={})
    )
    subprocess_handle.start_dispatch_parent_bound_messages_thread()
    response = await subprocess_handle.health_check()
    assert response.status == 200
    assert response.body == b"ok!"


def test_module_side_handler():
    """
    Tests that parent <-> child communication works. This does not involve aiohttp
    routing.
    """
    loop = asyncio.get_event_loop()

    subprocess = SubprocessModuleHandle(
        loop, TestModule, SubprocessModuleConfig(session_name="test", config={})
    )
    # No parent bound listening thread, manually check the queue.
    subprocess.send_message(
        RequestMessage(id="request_for_test", method_name="test", body=b"")
    )
    response = subprocess.parent_bound_queue.get()
    assert isinstance(response, ResponseMessage)
    assert response.id == "request_for_test"
    assert response.status == 200
    assert response.body == b"Hello, World from GET /test, run_finished: True"

    subprocess.send_message(
        RequestMessage(
            id="request_for_echo", method_name="echo", body=b"a new dashboard"
        )
    )
    response = subprocess.parent_bound_queue.get()
    assert isinstance(response, ResponseMessage)
    assert response.id == "request_for_echo"
    assert response.status == 200
    assert response.body == b"Hello, World from POST /echo from a new dashboard"

    subprocess.send_message(
        RequestMessage(id="request_for_error", method_name="make_error", body=b"")
    )
    response = subprocess.parent_bound_queue.get()
    assert isinstance(response, ErrorMessage)
    assert response.id == "request_for_error"
    assert isinstance(response.error, ValueError)
    assert str(response.error) == "This is an error"


# @pytest.mark.asyncio is not compatible with aiohttp_client.
async def test_http_server(aiohttp_client):
    """
    Tests that the http server works. It must
    1. bind a SubprocessModuleHandle
    2. add_routes
    3. run
    """
    loop = asyncio.get_event_loop()
    subprocess = SubprocessModuleHandle(
        loop, TestModule, SubprocessModuleConfig(session_name="test", config={})
    )
    subprocess.start_dispatch_parent_bound_messages_thread()

    SubprocessRouteTable.bind(subprocess)

    app = aiohttp.web.Application()
    app.add_routes(routes=SubprocessRouteTable.bound_routes())

    client = await aiohttp_client(app)

    # Test HTTP
    response = await client.get("/test")
    assert response.status == 200
    assert await response.text() == "Hello, World from GET /test, run_finished: True"

    response = await client.post("/echo", data="a new dashboard")
    assert response.status == 200
    assert await response.text() == "Hello, World from POST /echo from a new dashboard"

    response = await client.put("/error")
    assert response.status == 500
    assert "Internal Server Error" in await response.text()

    response = await client.put("/error_403")
    assert response.status == 403
    assert "you shall not pass" in await response.text()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
