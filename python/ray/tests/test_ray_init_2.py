import logging
import os
import sys
import unittest.mock
import tempfile
import shutil
from unittest.mock import patch

import pytest

import ray
from ray._private.ray_constants import RAY_OVERRIDE_DASHBOARD_URL, DEFAULT_RESOURCES
import ray._private.services
from ray._private.services import get_node_ip_address
from ray.dashboard.utils import ray_address_to_api_server_url
from ray._private.test_utils import (
    get_current_unused_port,
    run_string_as_driver,
    wait_for_condition,
)
from ray.util.client.ray_client_helpers import ray_start_client_server


def test_ray_init_context(shutdown_only):
    ctx = ray.init()
    assert ray.is_initialized()
    # Check old context fields can be accessed like a dict
    assert ctx["session_dir"] is not None
    assert ctx["node_id"] is not None
    with pytest.raises(KeyError):
        ctx["xyz"]
    # Check that __contains__ works
    assert "session_dir" in ctx
    assert "abcdefg" not in ctx
    # Check that get works
    assert ctx.get("session_dir") is not None
    assert ctx.get("gfedcba") is None
    ctx.disconnect()
    assert not ray.is_initialized()


def test_with_ray_init(shutdown_only):
    @ray.remote
    def f():
        return 42

    with ray.init() as ctx:
        assert ray.is_initialized()
        assert 42 == ray.get(f.remote())
        # Check old context fields can be accessed like a dict
        assert ctx["session_dir"] is not None
        assert ctx["node_id"] is not None
        with pytest.raises(KeyError):
            ctx["xyz"]
        # Check that __contains__ works
        assert "session_dir" in ctx
        assert "abcdefg" not in ctx
        # Check that get works
        assert ctx.get("session_dir") is not None
        assert ctx.get("gfedcba") is None
    assert not ray.is_initialized()


def test_ray_init_invalid_keyword(shutdown_only):
    with pytest.raises(RuntimeError) as excinfo:
        ray.init("localhost", logginglevel="<- missing underscore")
    assert "logginglevel" in str(excinfo.value)


def test_ray_init_invalid_keyword_with_client(shutdown_only):
    with pytest.raises(RuntimeError) as excinfo:
        ray.init("ray://127.0.0.0", logginglevel="<- missing underscore")
    assert "logginglevel" in str(excinfo.value)


def test_ray_init_valid_keyword_with_client(shutdown_only):
    with ray_start_client_server() as given_connection:
        given_connection.disconnect()
        # logging_level should be passed to the server
        with ray.init("ray://localhost:50051", logging_level=logging.INFO):
            pass


def test_env_var_override():
    with unittest.mock.patch.dict(
        os.environ, {"RAY_NAMESPACE": "envName"}
    ), ray_start_client_server() as given_connection:
        given_connection.disconnect()

        with ray.init("ray://localhost:50051"):
            assert ray.get_runtime_context().namespace == "envName"


def test_env_var_no_override():
    # init() argument has precedence over environment variables
    with unittest.mock.patch.dict(
        os.environ, {"RAY_NAMESPACE": "envName"}
    ), ray_start_client_server() as given_connection:
        given_connection.disconnect()

        with ray.init("ray://localhost:50051", namespace="argumentName"):
            assert ray.get_runtime_context().namespace == "argumentName"


@pytest.mark.parametrize(
    "override_url",
    [
        None,
        "https://external_dashboard_url",
        "https://external_dashboard_url/path1/?query_param1=val1&query_param2=val2",
        "new_external_dashboard_url",
    ],
)
def test_hosted_external_dashboard_url(override_url, shutdown_only, monkeypatch):
    """
    Test setting external dashboard URL through environment variable.
    """
    with monkeypatch.context() as m:
        if override_url:
            m.setenv(
                RAY_OVERRIDE_DASHBOARD_URL,
                override_url,
            )

        expected_localhost_url = "127.0.0.1:8265"
        if not override_url:
            # No external dashboard url
            expected_dashboard_url = "127.0.0.1:8265"
        elif "://" in override_url:
            # External dashboard url with https protocol included
            expected_dashboard_url = override_url[override_url.index("://") + 3 :]
        else:
            # External dashboard url with no protocol
            expected_dashboard_url = override_url

        info = ray.init(dashboard_port=8265)
        assert info.dashboard_url == expected_dashboard_url
        assert info.address_info["webui_url"] == expected_dashboard_url
        assert ray._private.worker._global_node.webui_url == expected_localhost_url
        assert (
            ray_address_to_api_server_url("auto") == "http://" + expected_localhost_url
        )


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25553 --port 0"],
    indirect=True,
)
def test_hosted_external_dashboard_url_with_ray_client(
    set_override_dashboard_url, call_ray_start
):
    """
    Test setting external dashboard URL through environment variable
    with Ray client.
    """
    info = ray.init("ray://localhost:25553")
    assert info.dashboard_url == "external_dashboard_url"


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25553 --port 0"],
    indirect=True,
)
def test_hosted_external_dashboard_url_with_connecting_to_existing_cluster(
    set_override_dashboard_url, call_ray_start
):
    """
    Test setting external dashboard URL through environment variable
    when connecting to existing Ray cluster
    """
    info = ray.init()
    assert info.dashboard_url == "external_dashboard_url"
    assert info.address_info["webui_url"] == "external_dashboard_url"
    assert ray._private.worker._global_node.webui_url == "127.0.0.1:8265"
    assert ray_address_to_api_server_url("auto") == "http://" + "127.0.0.1:8265"


def test_shutdown_and_reset_global_worker(shutdown_only):
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=["a"]))
    ray.shutdown()
    ray.init()

    @ray.remote
    class A:
        def f(self):
            return 100

    a = A.remote()
    ray.get(a.f.remote())


def test_tmpdir_env_var(shutdown_only):
    result = run_string_as_driver(
        """
import ray
context = ray.init()
assert context["session_dir"].startswith("/tmp/qqq"), context
print("passed")
""",
        env=dict(os.environ, **{"RAY_TMPDIR": "/tmp/qqq"}),
    )
    assert "passed" in result, result


def test_ports_assignment(ray_start_cluster):
    # Make sure value error is raised when there are the same ports.

    cluster = ray_start_cluster
    with pytest.raises(ValueError):
        cluster.add_node(dashboard_port=30000, metrics_export_port=30000)

    pre_selected_ports = {
        "redis_port": 30000,
        "object_manager_port": 30001,
        "node_manager_port": 30002,
        "gcs_server_port": 30003,
        "ray_client_server_port": 30004,
        "dashboard_port": 30005,
        "metrics_agent_port": 30006,
        "metrics_export_port": 30007,
        "runtime_env_agent_port": 30008,
    }

    # Make sure we can start a node properly.
    head_node = cluster.add_node(**pre_selected_ports)
    cluster.wait_for_nodes()
    cluster.remove_node(head_node)

    # Make sure the wrong worker list will raise an exception.
    with pytest.raises(ValueError, match="[30000, 30001, 30002, 30003]"):
        head_node = cluster.add_node(
            **pre_selected_ports, worker_port_list="30000,30001,30002,30003"
        )

    # Make sure the wrong min & max worker will raise an exception
    with pytest.raises(ValueError, match="from 25000 to 35000"):
        head_node = cluster.add_node(
            **pre_selected_ports, min_worker_port=25000, max_worker_port=35000
        )


def test_non_default_ports_visible_on_init(shutdown_only):
    import subprocess

    ports = {
        "dashboard_agent_grpc_port": get_current_unused_port(),
        "metrics_export_port": get_current_unused_port(),
        "dashboard_agent_listen_port": get_current_unused_port(),
        "port": get_current_unused_port(),  # gcs_server_port
        "node_manager_port": get_current_unused_port(),
    }
    # Start a ray head node with customized ports.
    cmd = "ray start --head --block".split(" ")
    for port_name, port in ports.items():
        # replace "_" with "-"
        port_name = port_name.replace("_", "-")
        cmd += ["--" + port_name, str(port)]

    print(" ".join(cmd))
    proc = subprocess.Popen(cmd)

    # From the connected node
    def verify():
        # Connect to the node and check ports
        print(ray.init("auto", ignore_reinit_error=True))

        node = ray.worker.global_worker.node
        assert node.metrics_agent_port == ports["dashboard_agent_grpc_port"]
        assert node.metrics_export_port == ports["metrics_export_port"]
        assert node.dashboard_agent_listen_port == ports["dashboard_agent_listen_port"]
        assert str(ports["port"]) in node.gcs_address
        assert node.node_manager_port == ports["node_manager_port"]
        return True

    try:
        wait_for_condition(verify, timeout=15, retry_interval_ms=2000)
    finally:
        proc.terminate()
        proc.wait()
        subprocess.check_output("ray stop --force", shell=True)


def test_get_and_write_node_ip_address(shutdown_only):
    ray.init()
    node_ip = ray.util.get_node_ip_address()
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    cached_node_ip_address = ray._private.services.get_cached_node_ip_address(
        session_dir
    )
    assert cached_node_ip_address == node_ip


@pytest.mark.skipif(sys.platform != "linux", reason="skip except linux")
def test_ray_init_from_workers(ray_start_cluster):
    cluster = ray_start_cluster
    # add first node
    node1 = cluster.add_node(node_ip_address="127.0.0.2")
    # add second node
    node2 = cluster.add_node(node_ip_address="127.0.0.3")
    address = cluster.address
    password = cluster.redis_password
    assert address.split(":")[0] == "127.0.0.2"
    assert node1.node_manager_port != node2.node_manager_port
    info = ray.init(address, _redis_password=password, _node_ip_address="127.0.0.3")
    assert info["node_ip_address"] == "127.0.0.3"

    node_info = ray._private.services.get_node_to_connect_for_driver(
        cluster.gcs_address, "127.0.0.3"
    )
    assert node_info["node_manager_port"] == node2.node_manager_port


def test_default_resource_not_allowed_error(shutdown_only):
    """
    Make sure when the default resources are passed to `resources`
    it raises an exception with a good error message.
    """
    for resource in DEFAULT_RESOURCES:
        with pytest.raises(
            AssertionError,
            match=(
                f"`{resource}` cannot be a custom resource because "
                "it is one of the default resources"
            ),
        ):
            ray.init(resources={resource: 100000})


def test_get_ray_address_from_environment(monkeypatch):
    monkeypatch.setenv("RAY_ADDRESS", "")
    assert (
        ray._private.services.get_ray_address_from_environment("addr", None) == "addr"
    )
    monkeypatch.setenv("RAY_ADDRESS", "env_addr")
    assert (
        ray._private.services.get_ray_address_from_environment("addr", None)
        == "env_addr"
    )


# https://github.com/ray-project/ray/issues/36431
def test_temp_dir_must_be_absolute(shutdown_only):
    # This test fails with a relative path _temp_dir.
    with pytest.raises(ValueError):
        ray.init(_temp_dir="relative_path")


def test_driver_node_ip_address_auto_configuration(monkeypatch, ray_start_cluster):
    """Simulate the ray is started with node-ip-address (privately assigned IP).

    At this time, the driver should automatically use the node-ip-address given
    to ray start.
    """
    with patch(
        "ray._private.ray_constants.ENABLE_RAY_CLUSTER"
    ) as enable_cluster_constant:
        # Without this, it will always use localhost (for MacOS and Windows).
        enable_cluster_constant.return_value = True
        ray_start_ip = get_node_ip_address()

        with patch(
            "ray._private.services.node_ip_address_from_perspective"
        ) as mocked_node_ip_address:  # noqa
            # Mock the node_ip_address_from_perspective to return a different IP
            # address than the one that will be passed to ray start.
            mocked_node_ip_address.return_value = "134.31.31.31"
            print("IP address passed to ray start:", ray_start_ip)
            print("Mock local IP address:", get_node_ip_address())

            cluster = ray_start_cluster
            cluster.add_node(node_ip_address=ray_start_ip)

            # If the IP address is not correctly configured, it will hang.
            ray.init(address=cluster.address)
            [node_info] = ray.nodes()
            assert node_info["NodeManagerAddress"] == ray_start_ip
            assert node_info["NodeID"] == ray.get_runtime_context().get_node_id()


@pytest.fixture
def short_tmp_path():
    path = tempfile.mkdtemp(dir="/tmp")
    yield path
    shutil.rmtree(path)


def test_temp_dir_with_node_ip_address(ray_start_cluster, short_tmp_path):
    cluster = ray_start_cluster
    cluster.add_node(temp_dir=short_tmp_path)
    ray.init(address=cluster.address)
    assert short_tmp_path == ray._private.worker._global_node.get_temp_dir_path()


def test_can_create_actor_in_multiple_sessions(shutdown_only):
    """Validates a bugfix that, if you create an actor in driver, then you shutdown and
    restart and create the actor in task, it fails.
    https://github.com/ray-project/ray/issues/44380
    """

    # To avoid interference with other tests, we need a fresh cluster.
    assert not ray.is_initialized()

    @ray.remote
    class A:
        def __init__(self):
            print("A.__init__")

    @ray.remote
    def make_actor_in_task():
        a = A.remote()
        return a

    ray.init()
    A.remote()
    ray.shutdown()

    ray.init()
    ray.get(make_actor_in_task.remote())


def test_can_create_task_in_multiple_sessions(shutdown_only):
    """Validates a bugfix that, if you create a task in driver, then you shutdown and
    restart and create the task in task, it hangs.
    https://github.com/ray-project/ray/issues/44380
    """

    # To avoid interference with other tests, we need a fresh cluster.
    assert not ray.is_initialized()

    @ray.remote
    def the_task():
        print("the task")
        return "the task"

    @ray.remote
    def run_task_in_task():
        return ray.get(the_task.remote())

    ray.init()
    assert ray.get(the_task.remote()) == "the task"
    ray.shutdown()

    ray.init()
    assert ray.get(run_task_in_task.remote()) == "the task"


if __name__ == "__main__":
    import sys

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
