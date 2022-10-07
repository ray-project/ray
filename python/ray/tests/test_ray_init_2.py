import logging
import os
import sys
import unittest.mock

import pytest

import ray
from ray._private.ray_constants import RAY_OVERRIDE_DASHBOARD_URL
import ray._private.services
from ray.dashboard.utils import ray_address_to_api_server_url
from ray._private.test_utils import run_string_as_driver
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
        address, cluster.gcs_address, "127.0.0.3", redis_password=password
    )
    assert node_info.node_manager_port == node2.node_manager_port


if __name__ == "__main__":
    import sys

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
