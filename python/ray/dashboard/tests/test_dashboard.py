import asyncio
import collections
import copy
import ipaddress
import json
import logging
import os
import socket
import subprocess
import sys
import time
import warnings
from unittest.mock import MagicMock
from urllib.parse import quote_plus

import pytest
import requests
from click.testing import CliRunner
from requests.exceptions import ConnectionError, HTTPError

import ray
import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.modules
import ray.dashboard.utils as dashboard_utils
import ray.scripts.scripts as scripts
from ray._common.network_utils import build_address, parse_address
from ray._common.ray_constants import (
    LOGGING_ROTATE_BACKUP_COUNT,
    LOGGING_ROTATE_BYTES,
)
from ray._common.test_utils import wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
)
from ray._private.test_utils import (
    fetch_prometheus_metrics,
    format_web_url,
    get_error_message,
    init_error_pubsub,
    run_string_as_driver,
    wait_until_server_available,
    wait_until_succeeded_without_exception,
)
from ray.core.generated import common_pb2
from ray.dashboard import dashboard
from ray.dashboard.head import DashboardHead
from ray.dashboard.utils import DashboardHeadModule
from ray.experimental.internal_kv import _initialize_internal_kv
from ray.util.state import StateApiClient
from ray.util.state.common import ListApiOptions, StateResource
from ray.util.state.exception import ServerUnavailable

import psutil

try:
    import aiohttp.web

    import ray.dashboard.optional_utils as dashboard_optional_utils

    head_routes = dashboard_optional_utils.DashboardHeadRouteTable
    from ray.dashboard.subprocesses.module import SubprocessModule
except Exception:
    pass

logger = logging.getLogger(__name__)


def make_gcs_client(address_info):
    address = address_info["gcs_address"]
    gcs_client = ray._raylet.GcsClient(address=address)
    return gcs_client


def cleanup_test_files():
    module_path = ray.dashboard.modules.__path__[0]
    filename = os.path.join(module_path, "test_for_bad_import.py")
    logger.info("Remove test file: %s", filename)
    try:
        os.remove(filename)
    except Exception:
        pass


def prepare_test_files():
    module_path = ray.dashboard.modules.__path__[0]
    filename = os.path.join(module_path, "test_for_bad_import.py")
    logger.info("Prepare test file: %s", filename)
    with open(filename, "w") as f:
        f.write(">>>")


cleanup_test_files()


def search_agent(processes):
    for p in processes:
        try:
            for c in p.cmdline():
                if os.path.join("dashboard", "agent.py") in c:
                    return p
        except Exception:
            pass


def check_agent_register(raylet_proc, agent_pid):
    # Check if agent register is OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = search_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"_system_config": {"agent_register_timeout_ms": 5000}}],
    indirect=True,
)
def test_basic(ray_start_regular):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    address_info = ray_start_regular
    node_id = address_info["node_id"]
    gcs_client = make_gcs_client(address_info)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)

    all_processes = ray._private.worker._global_node.all_processes
    assert ray_constants.PROCESS_TYPE_DASHBOARD in all_processes
    dashboard_proc_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
    dashboard_proc = psutil.Process(dashboard_proc_info.process.pid)
    assert dashboard_proc.status() in [
        psutil.STATUS_RUNNING,
        psutil.STATUS_SLEEPING,
        psutil.STATUS_DISK_SLEEP,
    ]
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    logger.info("Test agent register is OK.")
    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    assert dashboard_proc.status() in [psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING]
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # Check kv keys are set.
    logger.info("Check kv keys are set.")
    dashboard_address = ray.experimental.internal_kv._internal_kv_get(
        ray_constants.DASHBOARD_ADDRESS, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
    )
    assert dashboard_address is not None
    key = f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id}"
    agent_addr = ray.experimental.internal_kv._internal_kv_get(
        key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
    )
    assert agent_addr is not None


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This specifically tests the minimal installation.",
)
def test_missing_imports(shutdown_only):
    """
    Test dashboard fails when packages are missing but inclusion
    was explicitly specified by the user.
    """
    with pytest.raises(Exception):
        ray.init(include_dashboard=True)


def test_raylet_and_agent_share_fate(shutdown_only):
    """Test raylet and agent share fate."""

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.terminate()
    raylet_proc.wait()
    agent_proc.wait(15)

    # No error should be reported for graceful termination.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 0, errors

    ray.shutdown()

    ray_context = ray.init()
    all_processes = ray._private.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)
    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The raylet should be dead if agent exits.
    agent_proc.kill()
    agent_proc.wait()
    raylet_proc.wait(15)

    worker_node_id = ray_context.address_info["node_id"]
    worker_node_info = [
        node for node in ray.nodes() if node["NodeID"] == worker_node_id
    ][0]
    assert not worker_node_info["Alive"]
    assert worker_node_info["DeathReason"] == common_pb2.NodeDeathInfo.Reason.Value(
        "UNEXPECTED_TERMINATION"
    )
    assert (
        "failed and raylet fate-shares with it."
        in worker_node_info["DeathReasonMessage"]
    )


@pytest.mark.parametrize("parent_health_check_by_pipe", [True, False])
def test_agent_report_unexpected_raylet_death(
    monkeypatch, shutdown_only, parent_health_check_by_pipe
):
    """Test agent reports Raylet death if it is not SIGTERM."""

    monkeypatch.setenv(
        "RAY_enable_pipe_based_agent_to_parent_health_check",
        parent_health_check_by_pipe,
    )

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)

    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 1, errors
    err = errors[0]
    assert err["type"] == ray_constants.RAYLET_DIED_ERROR
    assert "Termination is unexpected." in err["error_message"], err["error_message"]
    assert "Raylet logs:" in err["error_message"], err["error_message"]
    assert (
        os.path.getsize(os.path.join(node.get_session_dir_path(), "logs", "raylet.out"))
        < 1 * 1024**2
    )


@pytest.mark.parametrize("parent_health_check_by_pipe", [True, False])
def test_agent_report_unexpected_raylet_death_large_file(
    monkeypatch, shutdown_only, parent_health_check_by_pipe
):
    """Test agent reports Raylet death if it is not SIGTERM."""

    monkeypatch.setenv(
        "RAY_enable_pipe_based_agent_to_parent_health_check",
        parent_health_check_by_pipe,
    )

    ray.init()
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # Append to the Raylet log file with data >> 1 MB.
    with open(
        os.path.join(node.get_session_dir_path(), "logs", "raylet.out"), "a"
    ) as f:
        f.write("test data\n" * 1024**2)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)

    # Reading and publishing logs should still work.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 1, errors
    err = errors[0]
    assert err["type"] == ray_constants.RAYLET_DIED_ERROR
    assert "Termination is unexpected." in err["error_message"], err["error_message"]
    assert "Raylet logs:" in err["error_message"], err["error_message"]


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.parametrize(
    "ray_start_with_dashboard",
    [
        {"dashboard_host": "127.0.0.1"},
        {"dashboard_host": "localhost"},
    ],
    indirect=True,
)
def test_dashboard_address_local(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_ip = parse_address(webui_url)[0]
    assert not ipaddress.ip_address(webui_ip).is_unspecified
    assert webui_ip == "127.0.0.1"


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.parametrize(
    "ray_start_with_dashboard",
    [
        {"dashboard_host": "0.0.0.0"},
        {"dashboard_host": "::"},
    ],
    indirect=True,
)
def test_dashboard_address_global(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_ip = parse_address(webui_url)[0]
    assert not ipaddress.ip_address(webui_ip).is_unspecified
    assert webui_ip == ray_start_with_dashboard["node_ip_address"]


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_http_get(enable_test_module, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    target_url = webui_url + "/test/dump"

    timeout_seconds = 30
    start_time = time.time()
    while True:
        time.sleep(3)
        try:
            response = requests.get(
                webui_url + "/test/http_get?url=" + quote_plus(target_url)
            )
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True

            # Get agent ip and http port
            node_id_hex = ray_start_with_dashboard["node_id"]
            agent_addr = ray.experimental.internal_kv._internal_kv_get(
                f"{dashboard_consts.DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX}{node_id_hex}",
                namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            )
            assert agent_addr is not None
            node_ip, http_port, _ = json.loads(agent_addr)

            response = requests.get(
                f"http://{build_address(node_ip, http_port)}"
                f"/test/http_get_from_agent?url={quote_plus(target_url)}"
            )
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_browser_no_post_no_put(enable_test_module, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timeout_seconds = 30
    start_time = time.time()
    while True:
        time.sleep(3)
        try:
            # Starting and getting jobs should be fine from API clients
            response = requests.post(
                webui_url + "/api/jobs/", json={"entrypoint": "ls"}
            )
            response.raise_for_status()
            response = requests.get(webui_url + "/api/jobs/")
            response.raise_for_status()

            # Starting job should be blocked for browsers
            response = requests.post(
                webui_url + "/api/jobs/",
                json={"entrypoint": "ls"},
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/119.0.0.0 Safari/537.36"
                    )
                },
            )
            with pytest.raises(HTTPError):
                response.raise_for_status()

            # Getting jobs should be fine for browsers
            response = requests.get(webui_url + "/api/jobs/")
            response.raise_for_status()
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_method_route_table(enable_test_module):
    head_cls_list = dashboard_utils.get_all_modules(dashboard_utils.DashboardHeadModule)
    agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule
    )
    test_head_cls = None
    for cls in head_cls_list:
        if cls.__name__ == "TestHead":
            test_head_cls = cls
            break
    assert test_head_cls is not None
    test_agent_cls = None
    for cls in agent_cls_list:
        if cls.__name__ == "TestAgent":
            test_agent_cls = cls
            break
    assert test_agent_cls is not None

    def _has_route(route, method, path):
        if isinstance(route, aiohttp.web.RouteDef):
            if route.method == method and route.path == path:
                return True
        return False

    def _has_static(route, path, prefix):
        if isinstance(route, aiohttp.web.StaticDef):
            if route.path == path and route.prefix == prefix:
                return True
        return False

    # Check agent routes
    unbound_agent_routes = dashboard_optional_utils.DashboardAgentRouteTable.routes()
    assert any(_has_route(r, "HEAD", "/test/route_head") for r in unbound_agent_routes)
    assert any(_has_route(r, "POST", "/test/route_post") for r in unbound_agent_routes)
    assert any(
        _has_route(r, "PATCH", "/test/route_patch") for r in unbound_agent_routes
    )

    # Check head routes
    unbound_head_routes = dashboard_optional_utils.DashboardHeadRouteTable.routes()
    assert any(_has_route(r, "GET", "/test/route_get") for r in unbound_head_routes)
    assert any(_has_route(r, "PUT", "/test/route_put") for r in unbound_head_routes)
    assert any(
        _has_route(r, "DELETE", "/test/route_delete") for r in unbound_head_routes
    )
    assert any(_has_route(r, "*", "/test/route_view") for r in unbound_head_routes)

    # Test bind()
    bound_agent_routes = (
        dashboard_optional_utils.DashboardAgentRouteTable.bound_routes()
    )
    assert len(bound_agent_routes) == 0
    dashboard_optional_utils.DashboardAgentRouteTable.bind(
        test_agent_cls.__new__(test_agent_cls)
    )
    bound_agent_routes = (
        dashboard_optional_utils.DashboardAgentRouteTable.bound_routes()
    )
    assert any(_has_route(r, "POST", "/test/route_post") for r in bound_agent_routes)
    assert all(not _has_route(r, "PUT", "/test/route_put") for r in bound_agent_routes)

    # Static def should be in bound routes.
    head_routes.static("/test/route_static", "/path")
    bound_head_routes = dashboard_optional_utils.DashboardHeadRouteTable.bound_routes()
    assert any(_has_static(r, "/path", "/test/route_static") for r in bound_head_routes)

    # Test duplicated routes should raise exception.
    try:

        @head_routes.get("/test/route_get")
        def _duplicated_route(req):
            pass

        raise Exception("Duplicated routes should raise exception.")
    except Exception as ex:
        message = str(ex)
        assert "/test/route_get" in message
        assert "test_head.py" in message

    # Test exception in handler
    post_handler = None
    for r in bound_agent_routes:
        if _has_route(r, "POST", "/test/route_post"):
            post_handler = r.handler
            break
    assert post_handler is not None

    r = get_or_create_event_loop().run_until_complete(post_handler())
    assert r.status == 500
    resp = json.loads(r.body)
    assert resp["result"] is False
    assert "Traceback" in resp["msg"]


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_async_loop_forever():
    counter = [0]

    @dashboard_utils.async_loop_forever(interval_seconds=0.1)
    async def foo():
        counter[0] += 1
        raise Exception("Test exception")

    loop = get_or_create_event_loop()
    loop.create_task(foo())
    loop.call_later(1, loop.stop)
    loop.run_forever()
    assert counter[0] > 2

    counter2 = [0]
    task = None

    @dashboard_utils.async_loop_forever(interval_seconds=0.1, cancellable=True)
    async def bar():
        nonlocal task
        counter2[0] += 1
        if counter2[0] > 2:
            task.cancel()

    loop = asyncio.new_event_loop()
    task = loop.create_task(bar())
    with pytest.raises(asyncio.CancelledError):
        loop.run_until_complete(task)
    assert counter2[0] == 3


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_dashboard_module_decorator(enable_test_module):
    head_cls_list = dashboard_utils.get_all_modules(dashboard_utils.DashboardHeadModule)
    agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule
    )

    assert any(cls.__name__ == "TestHead" for cls in head_cls_list)
    assert any(cls.__name__ == "TestAgent" for cls in agent_cls_list)

    test_code = """
import os
import ray.dashboard.utils as dashboard_utils

os.environ.pop("RAY_DASHBOARD_MODULE_TEST", None)
head_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardHeadModule)
agent_cls_list = dashboard_utils.get_all_modules(
        dashboard_utils.DashboardAgentModule)
print(head_cls_list)
print(agent_cls_list)
assert all(cls.__name__ != "TestHead" for cls in head_cls_list)
assert all(cls.__name__ != "TestAgent" for cls in agent_cls_list)
print("success")
"""
    run_string_as_driver(test_code)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_aiohttp_cache(enable_test_module, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    webui_url = ray_start_with_dashboard["webui_url"]
    webui_url = format_web_url(webui_url)

    timestamps = set()
    for _ in range(10):
        response = requests.get(webui_url + "/test/aiohttp_cache/t1?value=1")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        timestamps.add(timestamp)
    assert len(timestamps) == 1

    timestamps.clear()
    for x in range(10):
        response = requests.get(webui_url + "/test/aiohttp_cache/t1?value=1&nocache=1")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        timestamps.add(timestamp)
    assert len(timestamps) == 10

    timestamps.clear()
    for x in range(10):
        response = requests.get(webui_url + f"/test/aiohttp_cache/tt{x}?value=1")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        timestamps.add(timestamp)
    assert len(timestamps) == 10

    timestamps.clear()
    for x in range(10):
        response = requests.get(webui_url + f"/test/aiohttp_cache/tt?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        timestamps.add(timestamp)
    assert len(timestamps) == 10

    timestamps.clear()
    response = requests.get(webui_url + "/test/aiohttp_cache/raise_exception")
    with pytest.raises(Exception):
        response.raise_for_status()
    result = response.json()
    assert result["result"] is False
    assert "KeyError" in result["msg"]

    timestamps.clear()
    for x in range(10):
        response = requests.get(webui_url + f"/test/aiohttp_cache_lru/tt{x % 4}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        timestamps.add(timestamp)
    assert len(timestamps) == 4

    timestamps.clear()
    data = collections.defaultdict(set)
    for x in [0, 1, 2, 3, 4, 5, 2, 1, 0, 3]:
        response = requests.get(webui_url + f"/test/aiohttp_cache_lru/t1?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        data[x].add(timestamp)
        timestamps.add(timestamp)
    assert len(timestamps) == 8
    assert len(data[3]) == 2
    assert len(data[0]) == 2


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_get_cluster_status(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    address_info = ray_start_with_dashboard
    webui_url = address_info["webui_url"]
    webui_url = format_web_url(webui_url)

    # Check that the cluster_status endpoint works without the underlying data
    # from the GCS, but returns nothing.
    def get_cluster_status():
        response = requests.get(f"{webui_url}/api/cluster_status")
        response.raise_for_status()
        print(response.json())
        assert response.json()["result"]
        assert "autoscalingStatus" in response.json()["data"]
        assert "autoscalingError" in response.json()["data"]
        assert response.json()["data"]["autoscalingError"] is None
        assert "clusterStatus" in response.json()["data"]
        assert "loadMetricsReport" in response.json()["data"]["clusterStatus"]

    assert wait_until_succeeded_without_exception(
        get_cluster_status, (requests.RequestException,)
    )

    gcs_client = make_gcs_client(address_info)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    ray.experimental.internal_kv._internal_kv_put(
        DEBUG_AUTOSCALING_STATUS_LEGACY, "hello"
    )
    ray.experimental.internal_kv._internal_kv_put(DEBUG_AUTOSCALING_ERROR, "world")

    response = requests.get(f"{webui_url}/api/cluster_status")
    response.raise_for_status()
    assert response.json()["result"]
    assert "autoscalingStatus" in response.json()["data"]
    assert response.json()["data"]["autoscalingStatus"] == "hello"
    assert "autoscalingError" in response.json()["data"]
    assert response.json()["data"]["autoscalingError"] == "world"
    assert "clusterStatus" in response.json()["data"]
    assert "loadMetricsReport" in response.json()["data"]["clusterStatus"]


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.parametrize(
    "call_ray_start",
    [
        """ray start --no-monitor --head --num-cpus 1 \
--system-config={"enable_autoscaler_v2":true}""",
        """ray start --head --num-cpus 1""",
    ],
    indirect=True,
)
def test_get_nodes_summary(call_ray_start):

    # The sleep is needed since it seems a previous shutdown could be not yet
    # done when the next test starts. This prevents a previous cluster to be
    # connected the current test session.
    time.sleep(5)
    address_info = ray.init(address=call_ray_start)
    webui_url = address_info["webui_url"]
    webui_url = format_web_url(webui_url)

    def get_nodes_summary():
        response = requests.get(f"{webui_url}/nodes?view=summary")
        response.raise_for_status()
        response = response.json()
        print(response)

        assert response["data"]["nodeLogicalResources"]
        assert "0.0/1.0 CPU" in "".join(
            response["data"]["nodeLogicalResources"].values()
        )

    assert wait_until_succeeded_without_exception(
        get_nodes_summary,
        (requests.RequestException,),
        timeout_ms=10 * 1000,
        retry_interval_ms=1000,
    )


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_immutable_types():
    d = {str(i): i for i in range(1000)}
    d["list"] = list(range(1000))
    d["list"][0] = {str(i): i for i in range(1000)}
    d["dict"] = {str(i): i for i in range(1000)}
    immutable_dict = dashboard_utils.make_immutable(d)
    assert type(immutable_dict) is dashboard_utils.ImmutableDict
    assert immutable_dict == dashboard_utils.ImmutableDict(d)
    assert immutable_dict == d
    assert dashboard_utils.ImmutableDict(immutable_dict) == immutable_dict
    assert (
        dashboard_utils.ImmutableList(immutable_dict["list"]) == immutable_dict["list"]
    )
    assert "512" in d
    assert "512" in d["list"][0]
    assert "512" in d["dict"]

    # Test type conversion
    assert type(dict(immutable_dict)["list"]) is dashboard_utils.ImmutableList
    assert type(list(immutable_dict["list"])[0]) is dashboard_utils.ImmutableDict

    # Test json dumps / loads
    json_str = json.dumps(immutable_dict, cls=dashboard_utils.CustomEncoder)
    deserialized_immutable_dict = json.loads(json_str)
    assert type(deserialized_immutable_dict) is dict
    assert type(deserialized_immutable_dict["list"]) is list
    assert immutable_dict.mutable() == deserialized_immutable_dict
    dashboard_optional_utils.rest_response(
        dashboard_utils.HTTPStatusCode.OK, "OK", data=immutable_dict
    )
    dashboard_optional_utils.rest_response(
        dashboard_utils.HTTPStatusCode.OK, "OK", **immutable_dict
    )

    # Test copy
    copy_of_immutable = copy.copy(immutable_dict)
    assert copy_of_immutable == immutable_dict
    deepcopy_of_immutable = copy.deepcopy(immutable_dict)
    assert deepcopy_of_immutable == immutable_dict

    # Test get default immutable
    immutable_default_value = immutable_dict.get("not exist list", [1, 2])
    assert type(immutable_default_value) is dashboard_utils.ImmutableList

    # Test recursive immutable
    assert type(immutable_dict["list"]) is dashboard_utils.ImmutableList
    assert type(immutable_dict["dict"]) is dashboard_utils.ImmutableDict
    assert type(immutable_dict["list"][0]) is dashboard_utils.ImmutableDict

    # Test exception
    with pytest.raises(TypeError):
        dashboard_utils.ImmutableList((1, 2))

    with pytest.raises(TypeError):
        dashboard_utils.ImmutableDict([1, 2])

    with pytest.raises(TypeError):
        immutable_dict["list"] = []

    with pytest.raises(AttributeError):
        immutable_dict.update({1: 3})

    with pytest.raises(TypeError):
        immutable_dict["list"][0] = 0

    with pytest.raises(AttributeError):
        immutable_dict["list"].extend([1, 2])

    with pytest.raises(AttributeError):
        immutable_dict["list"].insert(1, 2)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1" or os.environ.get("RAY_DEFAULT") == "1",
    reason="This test is not supposed to work for minimal or default installation.",
)
def test_http_proxy(enable_test_module, start_http_proxy, shutdown_only):
    # C++ config `grpc_enable_http_proxy` only initializes once, so we have to
    # run driver as a separate process to make sure the correct config value
    # is initialized.
    script = """
import ray
import time
import requests
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
import logging

logger = logging.getLogger(__name__)

address_info = ray.init(num_cpus=1, include_dashboard=True)
assert wait_until_server_available(address_info["webui_url"]) is True

webui_url = address_info["webui_url"]
webui_url = format_web_url(webui_url)

timeout_seconds = 10
start_time = time.time()
while True:
    time.sleep(1)
    try:
        response = requests.get(
            webui_url + "/test/dump", proxies={"http": None, "https": None}
        )
        response.raise_for_status()
        try:
            response.json()
            assert response.ok
        except Exception as ex:
            logger.info("failed response: %s", response.text)
            raise ex
        break
    except (AssertionError, requests.exceptions.ConnectionError) as e:
        logger.info("Retry because of %s", e)
    finally:
        if time.time() > start_time + timeout_seconds:
            raise Exception("Timed out while testing.")
"""
    env = start_http_proxy
    run_string_as_driver(script, dict(os.environ, **env))


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_dashboard_port_conflict(ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True
    address_info = ray_start_with_dashboard
    gcs_client = make_gcs_client(address_info)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)
    host, port = parse_address(address_info["webui_url"])
    temp_dir = "/tmp/ray"
    session_dir = "/tmp/ray/session_latest"
    log_dir = "/tmp/ray/session_latest/logs"
    dashboard_cmd = [
        sys.executable,
        dashboard.__file__,
        f"--host={host}",
        f"--port={port}",
        f"--temp-dir={temp_dir}",
        f"--log-dir={log_dir}",
        f"--gcs-address={address_info['gcs_address']}",
        f"--cluster-id-hex={gcs_client.cluster_id.hex()}",
        f"--session-dir={session_dir}",
        "--node-ip-address=127.0.0.1",
    ]
    logger.info("The dashboard should be exit: %s", dashboard_cmd)
    dashboard_process = subprocess.Popen(dashboard_cmd)
    dashboard_process.wait(5)

    dashboard_cmd.append("--port-retries=10")
    conflicting_dashboard_process = subprocess.Popen(dashboard_cmd)

    timeout_seconds = 10
    start_time = time.time()
    while True:
        time.sleep(1)
        try:
            dashboard_url = ray.experimental.internal_kv._internal_kv_get(
                ray_constants.DASHBOARD_ADDRESS,
                namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
            )
            if dashboard_url:
                new_port = int(dashboard_url.split(b":")[-1])
                assert new_port > int(port)
                break
        except AssertionError as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")
    dashboard_process.kill()
    conflicting_dashboard_process.kill()
    dashboard_process.wait()
    conflicting_dashboard_process.wait()


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_gcs_check_alive(
    fast_gcs_failure_detection, ray_start_cluster, call_ray_stop_only
):
    # call_ray_stop_only is used to ensure a clean environment (especially
    # killing dashboard agent in time) before the next test runs.
    cluster = ray_start_cluster
    head = cluster.add_node(num_cpus=0)
    assert wait_until_server_available(head.address_info["webui_url"]) is True

    dashboard_info = head.all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
    dashboard_proc = psutil.Process(dashboard_info.process.pid)
    gcs_server_info = head.all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0]
    gcs_server_proc = psutil.Process(gcs_server_info.process.pid)

    assert dashboard_proc.status() in [
        psutil.STATUS_RUNNING,
        psutil.STATUS_SLEEPING,
        psutil.STATUS_DISK_SLEEP,
    ]

    gcs_server_proc.kill()
    gcs_server_proc.wait()

    assert dashboard_proc.wait(10) == 1


@pytest.mark.skipif(
    os.environ.get("RAY_DEFAULT") != "1",
    reason="This test only works for default installation.",
)
def test_dashboard_does_not_depend_on_serve():
    """Check that the dashboard can start without Serve."""
    ray.shutdown()

    with pytest.raises(ImportError):
        from ray import serve  # noqa: F401

    ctx = ray.init()

    # Ensure standard dashboard features, like component_activities, still work
    response = requests.get(f"http://{ctx.dashboard_url}/api/component_activities")
    assert response.status_code == 200

    assert "driver" in response.json()

    agent_url = (
        ctx.address_info["node_ip_address"]
        + ":"
        + str(ctx.address_info["dashboard_agent_listen_port"])
    )

    # Check that Serve-dependent features fail
    try:
        response = requests.get(f"http://{agent_url}/api/serve/applications/")
        print(f"response status code: {response.status_code}, expected: 501")
        assert response.status_code == 501
    except requests.ConnectionError as e:
        # Fail to connect to service is fine.
        print(e)


@pytest.mark.skipif(
    os.environ.get("RAY_DEFAULT") != "1",
    reason="This test only works for default installation.",
)
def test_agent_does_not_depend_on_serve(shutdown_only):
    """Check that the dashboard agent can start without Serve."""
    ray.shutdown()

    with pytest.raises(ImportError):
        from ray import serve  # noqa: F401

    ray.init(include_dashboard=True)

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    logger.info("Agent works.")

    agent_url = build_address(node.node_ip_address, node.dashboard_agent_listen_port)

    # Check that Serve-dependent features fail
    try:
        response = requests.get(f"http://{agent_url}/api/serve/applications/")
        print(f"response status code: {response.status_code}, expected: 501")
        assert response.status_code == 501
    except requests.ConnectionError as e:
        # Fail to connect to service is fine.
        print(e)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(15)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1" or os.environ.get("RAY_DEFAULT") == "1",
    reason="This test is not supposed to work for minimal or default installation.",
)
def test_agent_port_conflict(shutdown_only):
    ray.shutdown()

    # ocuppy the port with a socket.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    wait_for_condition(
        lambda: s.connect_ex(
            ("localhost", ray_constants.DEFAULT_DASHBOARD_AGENT_LISTEN_PORT)
        )
        != 0
    )

    # start ray and the agent http server should fail
    # to start due to port conflict, but the agent still starts.
    ray.init(include_dashboard=True)
    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test only works for minimal installation.",
)
def test_dashboard_requests_fail_on_missing_deps(ray_start_regular):
    """Check that requests from client fail with minimal installation"""
    response = None

    with pytest.raises(ServerUnavailable):
        client = StateApiClient()
        response = client.list(
            StateResource.NODES, options=ListApiOptions(), raise_on_missing_output=False
        )

    # Response should not be populated
    assert response is None


@pytest.mark.skipif(
    os.environ.get("RAY_DEFAULT") != "1",
    reason="This test only works for default installation.",
)
@pytest.mark.asyncio
async def test_dashboard_module_load(tmpdir):
    """Verify if the head module can load only selected modules."""
    head = DashboardHead(
        http_host="127.0.0.1",
        http_port=8265,
        http_port_retries=1,
        node_ip_address="127.0.0.1",
        gcs_address="127.0.0.1:6379",
        cluster_id_hex=ray.ClusterID.from_random().hex(),
        log_dir=str(tmpdir),
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        logging_filename=dashboard_consts.DASHBOARD_LOG_FILENAME,
        logging_rotate_bytes=LOGGING_ROTATE_BYTES,
        logging_rotate_backup_count=LOGGING_ROTATE_BACKUP_COUNT,
        temp_dir=str(tmpdir),
        session_dir=str(tmpdir),
        minimal=False,
        serve_frontend=True,
    )

    # Test basic.
    loaded_modules_expected = {"UsageStatsHead", "JobHead"}
    dashboard_head_modules, subprocess_module_handles = head._load_modules(
        modules_to_load=loaded_modules_expected
    )
    assert {type(m).__name__ for m in dashboard_head_modules} == loaded_modules_expected
    assert len(subprocess_module_handles) == 0

    # Test modules that don't exist.
    loaded_modules_expected = {"StateHea"}
    with pytest.raises(AssertionError):
        head._load_modules(modules_to_load=loaded_modules_expected)

    # Test the base case.
    # It is needed to pass assertion check from one of modules.
    gcs_client = MagicMock()
    _initialize_internal_kv(gcs_client)
    loaded_dashboard_head_modules_expected = {
        m.__name__ for m in dashboard_utils.get_all_modules(DashboardHeadModule)
    }
    loaded_subprocess_module_handles_expected = {
        m.__name__ for m in dashboard_utils.get_all_modules(SubprocessModule)
    }
    dashboard_head_modules, subprocess_module_handles = head._load_modules()
    assert {
        type(m).__name__ for m in dashboard_head_modules
    } == loaded_dashboard_head_modules_expected
    assert {
        m.module_cls.__name__ for m in subprocess_module_handles
    } == loaded_subprocess_module_handles_expected


@pytest.mark.skipif(
    sys.version_info >= (3, 10, 0),
    reason=(
        "six >= 1.16 and urllib3 >= 1.26.5 "
        "(it has its own forked six internally that's version 1.12) "
        "are required to pass this test on Python 3.10. "
        "It's because six < 1.16 doesn't have a `find_spec` API, "
        "which is required from Python 3.10 "
        "(otherwise, it warns that it fallbacks to use `find_modules` "
        "that is deprecated from Python 3.10). "
        "This test failure doesn't affect the user at all "
        "and it is too much to introduce version restriction and new "
        "dependencies requirement just for this test. "
        "So instead of fixing it, we just skip it."
    ),
)
def test_dashboard_module_no_warnings(enable_test_module):
    # Disable log_once so we will get all warnings
    from ray.util import debug

    old_val = debug._logged
    debug._logged = set()
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            dashboard_utils.get_all_modules(dashboard_utils.DashboardHeadModule)
            dashboard_utils.get_all_modules(dashboard_utils.DashboardAgentModule)
    finally:
        debug._disabled = old_val


def test_dashboard_not_included_ray_init(shutdown_only, capsys):
    addr = ray.init(include_dashboard=False, dashboard_port=8265)
    dashboard_url = addr["webui_url"]
    assert "View the dashboard" not in capsys.readouterr().err
    assert not dashboard_url

    # Warm up.
    @ray.remote
    def f():
        pass

    ray.get(f.remote())

    with pytest.raises(ConnectionError):
        # Since the dashboard doesn't start, it should raise ConnectionError
        # becasue we cannot estabilish a connection.
        requests.get("http://localhost:8265")


def test_dashboard_not_included_ray_start(shutdown_only, capsys):
    runner = CliRunner()
    try:
        runner.invoke(
            scripts.start,
            ["--head", "--include-dashboard=False", "--dashboard-port=8265"],
        )
        addr = ray.init("auto")
        dashboard_url = addr["webui_url"]
        assert not dashboard_url

        assert "view the dashboard at" not in capsys.readouterr().err

        # Warm up.
        @ray.remote
        def f():
            pass

        ray.get(f.remote())

        with pytest.raises(ConnectionError):
            # Since the dashboard doesn't start, it should raise ConnectionError
            # becasue we cannot estabilish a connection.
            requests.get("http://localhost:8265")
    finally:
        runner.invoke(scripts.stop, ["--force"])


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") != "1",
    reason="This test only works for minimal installation.",
)
def test_dashboard_not_included_ray_minimal(shutdown_only, capsys):
    addr = ray.init(dashboard_port=8265)
    dashboard_url = addr["webui_url"]
    assert "View the dashboard" not in capsys.readouterr().err
    assert not dashboard_url

    # Warm up.
    @ray.remote
    def f():
        pass

    ray.get(f.remote())

    with pytest.raises(ConnectionError):
        # Since the dashboard doesn't start, it should raise ConnectionError
        # becasue we cannot estabilish a connection.
        requests.get("http://localhost:8265")


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
@pytest.mark.asyncio
async def test_dashboard_exports_metric_on_event_loop_lag(
    enable_test_module, ray_start_with_dashboard
):
    """
    When the event loop is blocked, the dashboard should export a metric.
    Uses aiohttp to send concurrent requests to block the event loop.
    As the number of blocking call goes up, the event loop lag converges to ~5s on my
    laptop. We assert it to be >1s to be safe.
    """
    from typing import Dict, List

    import aiohttp
    from prometheus_client.samples import Sample

    ray_context = ray_start_with_dashboard
    assert wait_until_server_available(ray_context["webui_url"]) is True
    webui_url = format_web_url(ray_context["webui_url"])
    blocking_url = webui_url + "/test/block_event_loop?seconds=1"

    async def make_blocking_call():
        async with aiohttp.ClientSession() as session:
            async with session.get(blocking_url) as resp:
                resp.raise_for_status()
                return await resp.text()

    # Blocks the event loop for 1 second for 10 times.
    tasks = [make_blocking_call() for _ in range(10)]
    await asyncio.gather(*tasks)

    # Fetch the metrics from the dashboard.
    addr = ray_context["node_ip_address"]
    prom_addresses = [build_address(addr, dashboard_consts.DASHBOARD_METRIC_PORT)]

    def check_lag_metrics():
        metrics_samples: Dict[str, List[Sample]] = fetch_prometheus_metrics(
            prom_addresses
        )
        lag_metric_samples = metrics_samples["ray_dashboard_event_loop_lag_seconds"]
        assert len(lag_metric_samples) > 0
        assert any(sample.value > 1 for sample in lag_metric_samples)
        return True

    wait_for_condition(check_lag_metrics)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
