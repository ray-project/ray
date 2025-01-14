import asyncio
import re
import sys
from typing import List

import aiohttp.web
import pytest

from ray.dashboard.subprocesses.handle import SubprocessModuleHandle
from ray.dashboard.subprocesses.message import (
    ErrorMessage,
    RequestMessage,
    UnaryResponseMessage,
)
from ray.dashboard.subprocesses.module import SubprocessModule, SubprocessModuleConfig
from ray.dashboard.subprocesses.routes import SubprocessRouteTable
from ray.dashboard.subprocesses.tests.utils import TestModule
import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts


@pytest.fixture
def default_module_config(tmp_path) -> SubprocessModuleConfig:
    """
    Creates a tmpdir to hold the logs.
    """
    yield SubprocessModuleConfig(
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=tmp_path,
        logging_filename=dashboard_consts.DASHBOARD_LOG_FILENAME,
        logging_rotate_bytes=ray_constants.LOGGING_ROTATE_BYTES,
        logging_rotate_backup_count=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
    )


@pytest.mark.asyncio
async def test_handle_can_health_check(default_module_config):
    loop = asyncio.get_event_loop()

    subprocess_handle = SubprocessModuleHandle(loop, TestModule, default_module_config)
    subprocess_handle.start()
    response = await subprocess_handle.health_check()
    assert response.status == 200
    assert response.body == b"ok!"


def test_module_side_handler(default_module_config):
    """
    Tests that parent <-> child communication works. This does not involve aiohttp
    routing.
    """
    loop = asyncio.get_event_loop()

    subprocess = SubprocessModuleHandle(loop, TestModule, default_module_config)
    # No parent bound listening thread, manually check the queue.
    subprocess._send_message(
        RequestMessage(id="request_for_test", method_name="test", body=b"")
    )
    response = subprocess.parent_bound_queue.get()
    assert isinstance(response, UnaryResponseMessage)
    assert response.id == "request_for_test"
    assert response.status == 200
    assert response.body == b"Hello, World from GET /test, run_finished: True"

    subprocess._send_message(
        RequestMessage(
            id="request_for_echo", method_name="echo", body=b"a new dashboard"
        )
    )
    response = subprocess.parent_bound_queue.get()
    assert isinstance(response, UnaryResponseMessage)
    assert response.id == "request_for_echo"
    assert response.status == 200
    assert response.body == b"Hello, World from POST /echo from a new dashboard"

    subprocess._send_message(
        RequestMessage(id="request_for_error", method_name="make_error", body=b"")
    )
    response = subprocess.parent_bound_queue.get()
    assert isinstance(response, ErrorMessage)
    assert response.id == "request_for_error"
    assert isinstance(response.error, ValueError)
    assert str(response.error) == "This is an error"


async def start_http_server_app(
    default_module_config, modules: List[type(SubprocessModule)]
):
    loop = asyncio.get_event_loop()
    handles = [
        SubprocessModuleHandle(loop, module, default_module_config)
        for module in modules
    ]
    for handle in handles:
        handle.start()
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


async def test_streamed_iota(aiohttp_client, default_module_config):
    # TODO(ryw): also test streams that raise exceptions.
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    response = await client.post("/streamed_iota", data=b"10")
    assert response.status == 200
    assert await response.text() == "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"


async def test_logging_in_module(aiohttp_client, default_module_config):
    app = await start_http_server_app(default_module_config, [TestModule])
    client = await aiohttp_client(app)

    response = await client.post("/logging_in_module", data=b"")
    assert response.status == 200
    assert await response.text() == "done!"

    # Assert the log file name and read the log file
    log_file_path = default_module_config.log_dir / "dashboard-TestModule.log"
    with open(log_file_path, "r") as f:
        log_file_content = f.read()

    # Assert on the log format and the content.
    log_pattern = (
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\tINFO ([\w\.]+:\d+) -- (.*)"
    )
    matches = re.findall(log_pattern, log_file_content)

    # Expected format: [(file_name:line_no, content), ...]
    expected_logs = [
        ("utils.py:24", "TestModule is running"),
        ("utils.py:61", "In /logging_in_module, Not all those who wander are lost."),
    ]

    assert matches == expected_logs, f"Expected {expected_logs}, got {matches}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
