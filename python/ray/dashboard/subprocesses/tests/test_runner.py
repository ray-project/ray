import asyncio

import aiohttp.web

from ray.dashboard.subprocesses.message import (
    ErrorMessage,
    HealthCheckMessage,
    HealthCheckResponseMessage,
    RequestMessage,
    ResponseMessage,
)
from ray.dashboard.subprocesses.module import (
    SubprocessModule,
    SubprocessModuleConfig,
    SubprocessRouteTable,
)
from ray.dashboard.subprocesses.runner import SubprocessModuleHandle


class TestModule(SubprocessModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_finished = False

    async def run(self):
        print("TestModule is running")
        self.run_finished = True

    @SubprocessRouteTable.get("/test")
    async def test(self, request_body: bytes) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            text="Hello, World from GET /test, run_finished: " + str(self.run_finished)
        )

    @SubprocessRouteTable.post("/echo")
    async def echo(self, request_body: bytes) -> aiohttp.web.Response:
        # await works
        await asyncio.sleep(0.1)
        return aiohttp.web.Response(
            body=b"Hello, World from POST /echo from " + request_body
        )

    @SubprocessRouteTable.put("/error")
    async def make_error(self, request_body: bytes) -> aiohttp.web.Response:
        raise ValueError("This is an error")

    @SubprocessRouteTable.put("/error_403")
    async def make_error_403(self, request_body: bytes) -> aiohttp.web.Response:
        raise aiohttp.web.HTTPForbidden(reason="you shall not pass")

    # TODO: streaming response.


def test_handle_can_health_check():
    loop = asyncio.get_event_loop()

    subprocess_handle = SubprocessModuleHandle(
        loop, TestModule, SubprocessModuleConfig(session_name="test", config={})
    )
    subprocess_handle.send_message(HealthCheckMessage(id="test"))
    # No parent bound listening thread, manually check the queue.
    response = subprocess_handle.parent_bound_queue.get()
    assert isinstance(response, HealthCheckResponseMessage)
    assert response.id == "test"


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


# @pytest.mark.asyncio
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


# Note: due to limitations you can't run this as __main__. Please use
# pytest -sv test_runner.py
