import asyncio
import re
import sys
import pathlib
from typing import List

from ray.dashboard.optional_deps import aiohttp
import pytest

from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
from ray.dashboard.subprocesses.module import SubprocessModule, SubprocessModuleConfig
from ray.dashboard.subprocesses.routes import SubprocessRouteTable
from ray.dashboard.subprocesses.tests.utils import TestModule, TestModule1
import ray._private.ray_constants as ray_constants
from ray._private.test_utils import async_wait_for_condition
import ray.dashboard.consts as dashboard_consts

# This test requires non-minimal Ray.


@pytest.fixture
def default_module_config(tmp_path) -> SubprocessModuleConfig:
    """
    Creates a tmpdir to hold the logs.
    """
    yield SubprocessModuleConfig(
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=str(tmp_path),
        logging_filename=dashboard_consts.DASHBOARD_LOG_FILENAME,
        logging_rotate_bytes=ray_constants.LOGGING_ROTATE_BYTES,
        logging_rotate_backup_count=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        socket_dir=str(tmp_path),
    )


@pytest.mark.asyncio
async def test_handle_can_health_check(default_module_config):
    loop = asyncio.get_event_loop()

    subprocess_handle = SubprocessModuleHandle(loop, TestModule, default_module_config)
    subprocess_handle.start_module()
    subprocess_handle.wait_for_module_ready()
    response = await subprocess_handle._health_check()
    assert response.status == 200
    assert response.body == b"success"


async def start_http_server_app(
    default_module_config, modules: List[type(SubprocessModule)]
):
    loop = asyncio.get_event_loop()
    handles = [
        SubprocessModuleHandle(loop, module, default_module_config)
        for module in modules
    ]
    # Parallel start all modules.
    for handle in handles:
        handle.start_module()
    # Wait for all modules to be ready.
    for handle in handles:
        handle.wait_for_module_ready()
        SubprocessRouteTable.bind(handle)

    app = aiohttp.web.Application()
    app.add_routes(routes=SubprocessRouteTable.bound_routes())
    return app


# @pytest.mark.asyncio is not compatible with aiohttp_client.
async def test_http_server(aiohttp_client, default_module_config):
    """
    Tests that the http server works. It must
    1. bind a SubprocessModuleHandle
    2. add_routes
    3. run
    """
    app = await start_http_server_app(default_module_config, [TestModule])
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


async def test_load_multiple_modules(aiohttp_client, default_module_config):
    """
    Tests multiple modules can be loaded.
    """
    app = await start_http_server_app(default_module_config, [TestModule, TestModule1])
    client = await aiohttp_client(app)

    response = await client.get("/test")
    assert response.status == 200
    assert await response.text() == "Hello, World from GET /test, run_finished: True"

    response = await client.get("/test1")
    assert response.status == 200
    assert await response.text() == "Hello from TestModule1"


async def test_streamed_iota(aiohttp_client, default_module_config):
    # TODO(ryw): also test streams that raise exceptions.
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    response = await client.post("/streamed_iota", data=b"10")
    assert response.status == 200
    assert await response.text() == "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"


async def test_streamed_error(aiohttp_client, default_module_config):
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    response = await client.post("/streamed_401", data=b"")
    assert response.status == 401
    assert await response.text() == "401: Unauthorized although I am not a teapot"


async def test_websocket_bytes_res(aiohttp_client, default_module_config):
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    res = []
    async with client.ws_connect("/websocket_one_to_five_bytes") as ws:
        async for msg in ws:
            assert msg.type == aiohttp.WSMsgType.BINARY
            res.append(msg.data)
    assert res == [b"1\n", b"2\n", b"3\n", b"4\n", b"5\n"]


async def test_websocket_bytes_str(aiohttp_client, default_module_config):
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    res = []
    async with client.ws_connect("/websocket_one_to_five_strs") as ws:
        async for msg in ws:
            assert msg.type == aiohttp.WSMsgType.TEXT
            res.append(msg.data)
    assert res == ["1\n", "2\n", "3\n", "4\n", "5\n"]


async def test_kill_self(aiohttp_client, default_module_config):
    """
    If a module died, all pending requests should be failed, and the module should be
    restarted. After the restart, subsequent requests should be successful.
    """
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    long_running_task = asyncio.create_task(client.post("/run_forever", data=b""))
    # Wait for 1s for the long running request to start.
    await asyncio.sleep(1)

    response = await client.post("/kill_self", data=b"")
    assert response.status == 500
    assert (
        await response.text()
        == "500 Internal Server Error\n\nServer got itself in trouble"
    )

    # Long running request should get a 500.
    long_running_response = await long_running_task
    assert long_running_response.status == 500
    assert (
        await long_running_response.text()
        == "500 Internal Server Error\n\nServer got itself in trouble"
    )

    async def verify():
        response = await client.post(
            "/echo", data=b"a restarted dashboard", timeout=0.5
        )
        assert response.status == 200
        assert (
            await response.text()
            == "Hello, World from POST /echo from a restarted dashboard"
        )
        return True

    await async_wait_for_condition(verify)


async def test_logging_in_module(aiohttp_client, default_module_config):
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    response = await client.post(
        "/logging_in_module", data=b"Not all those who wander are lost"
    )
    assert response.status == 200
    assert await response.text() == "done!"

    # Assert the log file name and read the log file
    log_file_path = (
        pathlib.Path(default_module_config.log_dir) / "dashboard_TestModule.log"
    )
    with log_file_path.open("r") as f:
        log_file_content = f.read()

    # Assert on the log format and the content.
    log_pattern = (
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\tINFO ([\w\.]+):\d+ -- (.*)"
    )
    matches = re.findall(log_pattern, log_file_content)

    # Expected format: [(file_name, content), ...]
    expected_logs = [
        ("utils.py", "TestModule is initing"),
        ("utils.py", "TestModule is done initing"),
        ("utils.py", "In /logging_in_module, Not all those who wander are lost."),
    ]
    assert all(
        (file_name, content) in matches for (file_name, content) in expected_logs
    ), f"Expected to contain {expected_logs}, got {matches}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
