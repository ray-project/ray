import os
import sys
import copy
import json
import time
import logging
import asyncio
import ipaddress
import subprocess
import collections

import numpy as np
import ray
import psutil
import pytest
import requests

from ray import ray_constants
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
    run_string_as_driver,
    wait_until_succeeded_without_exception,
)
from ray.ray_constants import DEBUG_AUTOSCALING_STATUS_LEGACY, DEBUG_AUTOSCALING_ERROR
from ray.dashboard import dashboard
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.modules

try:
    import aiohttp.web
    import ray.dashboard.optional_utils as dashboard_optional_utils

    routes = dashboard_optional_utils.ClassMethodRouteTable
except Exception:
    pass

logger = logging.getLogger(__name__)


def make_gcs_client(address_info):
    address = address_info["gcs_address"]
    gcs_client = ray._private.gcs_utils.GcsClient(address=address)
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
    "ray_start_with_dashboard",
    [{"_system_config": {"agent_register_timeout_ms": 5000}}],
    indirect=True,
)
def test_basic(ray_start_with_dashboard):
    """Dashboard test that starts a Ray cluster with a dashboard server running,
    then hits the dashboard API and asserts that it receives sensible data."""
    address_info = ray_start_with_dashboard
    node_id = address_info["node_id"]
    gcs_client = make_gcs_client(address_info)
    ray.experimental.internal_kv._initialize_internal_kv(gcs_client)

    all_processes = ray.worker._global_node.all_processes
    assert ray_constants.PROCESS_TYPE_DASHBOARD in all_processes
    assert ray_constants.PROCESS_TYPE_REPORTER not in all_processes
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
    dashboard_rpc_address = ray.experimental.internal_kv._internal_kv_get(
        dashboard_consts.DASHBOARD_RPC_ADDRESS,
        namespace=ray_constants.KV_NAMESPACE_DASHBOARD,
    )
    assert dashboard_rpc_address is not None
    key = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}{node_id}"
    agent_ports = ray.experimental.internal_kv._internal_kv_get(
        key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
    )
    assert agent_ports is not None


def test_raylet_and_agent_share_fate(shutdown_only):
    """Test raylet and agent share fate."""

    ray.init(include_dashboard=True)

    all_processes = ray.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.kill()
    raylet_proc.wait()
    agent_proc.wait(5)

    ray.shutdown()

    ray.init(include_dashboard=True)
    all_processes = ray.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)
    wait_for_condition(lambda: search_agent(raylet_proc.children()))
    agent_proc = search_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The raylet should be dead if agent exits.
    agent_proc.kill()
    agent_proc.wait()
    raylet_proc.wait(5)


@pytest.mark.parametrize(
    "ray_start_with_dashboard",
    [
        {"dashboard_host": "127.0.0.1"},
        {"dashboard_host": "0.0.0.0"},
        {"dashboard_host": "::"},
    ],
    indirect=True,
)
def test_dashboard_address(ray_start_with_dashboard):
    webui_url = ray_start_with_dashboard["webui_url"]
    if os.environ.get("RAY_MINIMAL") == "1":
        # In the minimal installation, webui url shouldn't be configured.
        assert webui_url == ""
    else:
        webui_ip = webui_url.split(":")[0]
        print(ipaddress.ip_address(webui_ip))
        print(webui_ip)
        assert not ipaddress.ip_address(webui_ip).is_unspecified
        assert webui_ip in ["127.0.0.1", ray_start_with_dashboard["node_ip_address"]]


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
            response = requests.get(webui_url + "/test/http_get?url=" + target_url)
            response.raise_for_status()
            try:
                dump_info = response.json()
            except Exception as ex:
                logger.info("failed response: %s", response.text)
                raise ex
            assert dump_info["result"] is True
            dump_data = dump_info["data"]
            assert len(dump_data["agents"]) == 1
            node_id, ports = next(iter(dump_data["agents"].items()))
            ip = ray_start_with_dashboard["node_ip_address"]
            http_port, grpc_port = ports

            response = requests.get(
                f"http://{ip}:{http_port}" f"/test/http_get_from_agent?url={target_url}"
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
def test_class_method_route_table(enable_test_module):
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

    all_routes = dashboard_optional_utils.ClassMethodRouteTable.routes()
    assert any(_has_route(r, "HEAD", "/test/route_head") for r in all_routes)
    assert any(_has_route(r, "GET", "/test/route_get") for r in all_routes)
    assert any(_has_route(r, "POST", "/test/route_post") for r in all_routes)
    assert any(_has_route(r, "PUT", "/test/route_put") for r in all_routes)
    assert any(_has_route(r, "PATCH", "/test/route_patch") for r in all_routes)
    assert any(_has_route(r, "DELETE", "/test/route_delete") for r in all_routes)
    assert any(_has_route(r, "*", "/test/route_view") for r in all_routes)

    # Test bind()
    bound_routes = dashboard_optional_utils.ClassMethodRouteTable.bound_routes()
    assert len(bound_routes) == 0
    dashboard_optional_utils.ClassMethodRouteTable.bind(
        test_agent_cls.__new__(test_agent_cls)
    )
    bound_routes = dashboard_optional_utils.ClassMethodRouteTable.bound_routes()
    assert any(_has_route(r, "POST", "/test/route_post") for r in bound_routes)
    assert all(not _has_route(r, "PUT", "/test/route_put") for r in bound_routes)

    # Static def should be in bound routes.
    routes.static("/test/route_static", "/path")
    bound_routes = dashboard_optional_utils.ClassMethodRouteTable.bound_routes()
    assert any(_has_static(r, "/path", "/test/route_static") for r in bound_routes)

    # Test duplicated routes should raise exception.
    try:

        @routes.get("/test/route_get")
        def _duplicated_route(req):
            pass

        raise Exception("Duplicated routes should raise exception.")
    except Exception as ex:
        message = str(ex)
        assert "/test/route_get" in message
        assert "test_head.py" in message

    # Test exception in handler
    post_handler = None
    for r in bound_routes:
        if _has_route(r, "POST", "/test/route_post"):
            post_handler = r.handler
            break
    assert post_handler is not None

    loop = asyncio.get_event_loop()
    r = loop.run_until_complete(post_handler())
    assert r.status == 200
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

    loop = asyncio.get_event_loop()
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

os.environ.pop("RAY_DASHBOARD_MODULE_TEST")
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

    timeout_seconds = 5
    start_time = time.time()
    value1_timestamps = []
    while True:
        time.sleep(1)
        try:
            for x in range(10):
                response = requests.get(webui_url + "/test/aiohttp_cache/t1?value=1")
                response.raise_for_status()
                timestamp = response.json()["data"]["timestamp"]
                value1_timestamps.append(timestamp)
            assert len(collections.Counter(value1_timestamps)) > 1
            break
        except (AssertionError, requests.exceptions.ConnectionError) as e:
            logger.info("Retry because of %s", e)
        finally:
            if time.time() > start_time + timeout_seconds:
                raise Exception("Timed out while testing.")

    sub_path_timestamps = []
    for x in range(10):
        response = requests.get(webui_url + f"/test/aiohttp_cache/tt{x}?value=1")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        sub_path_timestamps.append(timestamp)
    assert len(collections.Counter(sub_path_timestamps)) == 10

    volatile_value_timestamps = []
    for x in range(10):
        response = requests.get(webui_url + f"/test/aiohttp_cache/tt?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        volatile_value_timestamps.append(timestamp)
    assert len(collections.Counter(volatile_value_timestamps)) == 10

    response = requests.get(webui_url + "/test/aiohttp_cache/raise_exception")
    response.raise_for_status()
    result = response.json()
    assert result["result"] is False
    assert "KeyError" in result["msg"]

    volatile_value_timestamps = []
    for x in range(10):
        response = requests.get(webui_url + f"/test/aiohttp_cache_lru/tt{x % 4}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        volatile_value_timestamps.append(timestamp)
    assert len(collections.Counter(volatile_value_timestamps)) == 4

    volatile_value_timestamps = []
    data = collections.defaultdict(set)
    for x in [0, 1, 2, 3, 4, 5, 2, 1, 0, 3]:
        response = requests.get(webui_url + f"/test/aiohttp_cache_lru/t1?value={x}")
        response.raise_for_status()
        timestamp = response.json()["data"]["timestamp"]
        data[x].add(timestamp)
        volatile_value_timestamps.append(timestamp)
    assert len(collections.Counter(volatile_value_timestamps)) == 8
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
def test_immutable_types():
    d = {str(i): i for i in range(1000)}
    d["list"] = list(range(1000))
    d["list"][0] = {str(i): i for i in range(1000)}
    d["dict"] = {str(i): i for i in range(1000)}
    immutable_dict = dashboard_utils.make_immutable(d)
    assert type(immutable_dict) == dashboard_utils.ImmutableDict
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
    assert type(dict(immutable_dict)["list"]) == dashboard_utils.ImmutableList
    assert type(list(immutable_dict["list"])[0]) == dashboard_utils.ImmutableDict

    # Test json dumps / loads
    json_str = json.dumps(immutable_dict, cls=dashboard_optional_utils.CustomEncoder)
    deserialized_immutable_dict = json.loads(json_str)
    assert type(deserialized_immutable_dict) == dict
    assert type(deserialized_immutable_dict["list"]) == list
    assert immutable_dict.mutable() == deserialized_immutable_dict
    dashboard_optional_utils.rest_response(True, "OK", data=immutable_dict)
    dashboard_optional_utils.rest_response(True, "OK", **immutable_dict)

    # Test copy
    copy_of_immutable = copy.copy(immutable_dict)
    assert copy_of_immutable == immutable_dict
    deepcopy_of_immutable = copy.deepcopy(immutable_dict)
    assert deepcopy_of_immutable == immutable_dict

    # Test get default immutable
    immutable_default_value = immutable_dict.get("not exist list", [1, 2])
    assert type(immutable_default_value) == dashboard_utils.ImmutableList

    # Test recursive immutable
    assert type(immutable_dict["list"]) == dashboard_utils.ImmutableList
    assert type(immutable_dict["dict"]) == dashboard_utils.ImmutableDict
    assert type(immutable_dict["list"][0]) == dashboard_utils.ImmutableDict

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

    d2 = dashboard_utils.ImmutableDict({1: np.zeros([3, 5])})
    with pytest.raises(TypeError):
        print(d2[1])

    d3 = dashboard_utils.ImmutableList([1, np.zeros([3, 5])])
    with pytest.raises(TypeError):
        print(d3[1])


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
    host, port = address_info["webui_url"].split(":")
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
        f"--session-dir={session_dir}",
    ]
    logger.info("The dashboard should be exit: %s", dashboard_cmd)
    p = subprocess.Popen(dashboard_cmd)
    p.wait(5)

    dashboard_cmd.append("--port-retries=10")
    subprocess.Popen(dashboard_cmd)

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


@pytest.mark.skipif(
    os.environ.get("RAY_MINIMAL") == "1",
    reason="This test is not supposed to work for minimal installation.",
)
def test_gcs_check_alive(fast_gcs_failure_detection, ray_start_with_dashboard):
    assert wait_until_server_available(ray_start_with_dashboard["webui_url"]) is True

    all_processes = ray.worker._global_node.all_processes
    dashboard_info = all_processes[ray_constants.PROCESS_TYPE_DASHBOARD][0]
    dashboard_proc = psutil.Process(dashboard_info.process.pid)
    gcs_server_info = all_processes[ray_constants.PROCESS_TYPE_GCS_SERVER][0]
    gcs_server_proc = psutil.Process(gcs_server_info.process.pid)

    assert dashboard_proc.status() in [
        psutil.STATUS_RUNNING,
        psutil.STATUS_SLEEPING,
        psutil.STATUS_DISK_SLEEP,
    ]

    gcs_server_proc.kill()
    gcs_server_proc.wait()

    # The dashboard exits by os._exit(-1)
    assert dashboard_proc.wait(10) == 255


@pytest.mark.skipif(
    os.environ.get("RAY_DEFAULT") != "1",
    reason="This test only works for default installation.",
)
def test_dashboard_does_not_depend_on_serve():
    """Check that the dashboard can start without Serve."""

    with pytest.raises(ImportError):
        from ray import serve  # noqa: F401

    ctx = ray.init(include_dashboard=True)

    # Ensure standard dashboard features, like snapshot, still work
    response = requests.get(f"http://{ctx.dashboard_url}/api/snapshot")
    assert response.status_code == 200
    assert response.json()["result"] is True
    assert "snapshot" in response.json()["data"]

    # Check that Serve-dependent features fail
    response = requests.get(f"http://{ctx.dashboard_url}/api/serve/deployments/")
    assert response.status_code == 500
    assert "ModuleNotFoundError" in response.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
