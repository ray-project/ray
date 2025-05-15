import os
import subprocess
import sys
import warnings
from unittest.mock import Mock, patch

import pytest

import ray
import ray.client_builder as client_builder
import ray.util.client.server.server as ray_client_server
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
    wait_for_condition,
)
from ray.util.state import list_workers


@pytest.mark.parametrize(
    "address",
    [
        "localhost:1234",
        "localhost:1234/url?params",
        "1.2.3.4/cluster-1?test_param=param1?",
        "",
    ],
)
def test_split_address(address):
    assert client_builder._split_address(address) == ("ray", address)

    specified_module = f"ray://{address}"
    assert client_builder._split_address(specified_module) == ("ray", address)

    specified_other_module = f"module://{address}"
    assert client_builder._split_address(specified_other_module) == ("module", address)
    non_url_compliant_module = f"module_test://{address}"
    assert client_builder._split_address(non_url_compliant_module) == (
        "module_test",
        address,
    )


@pytest.mark.parametrize(
    "address", ["localhost", "1.2.3.4:1200", "ray://1.2.3.4:1200", "local", None]
)
def test_client(address):
    builder = client_builder.client(address)
    assert isinstance(builder, client_builder.ClientBuilder)
    if address in ("local", None):
        assert isinstance(builder, client_builder._LocalClientBuilder)
    else:
        assert type(builder) is client_builder.ClientBuilder
        assert builder.address == address.replace("ray://", "")


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_local_clusters():
    """
    This tests the various behaviors of connecting to local clusters:

    * Using `ray.client("local").connect() ` should always create a new
      cluster.
    * Using `ray.cleint().connectIO` should create a new cluster if it doesn't
      connect to an existing one.
    * Using `ray.client().connect()` should only connect to a cluster if it
      was created with `ray start --head`, not from a python program.

    It does tests if two calls are in the same cluster by trying to create an
    actor with the same name in the same namespace, which will error and cause
    the script have a non-zero exit, which throws an exception.
    """
    driver_template = """
import ray
info = ray.client({address}).namespace("").connect()

@ray.remote
class Foo:
    def ping(self):
        return "pong"

a = Foo.options(name="abc", lifetime="detached").remote()
ray.get(a.ping.remote())

import time
while True:
    time.sleep(30)

"""
    blocking_local_script = driver_template.format(address="'local'", blocking=True)
    blocking_noaddr_script = driver_template.format(address="", blocking=True)

    # This should start a cluster.
    p1 = run_string_as_driver_nonblocking(blocking_local_script)
    # ray.client("local").connect() should start a second cluster.
    p2 = run_string_as_driver_nonblocking(blocking_local_script)
    # ray.client().connect() shouldn't connect to a cluster started by
    # ray.client("local").connect() so it should create a third one.
    p3 = run_string_as_driver_nonblocking(blocking_noaddr_script)
    # ray.client().connect() shouldn't connect to a cluster started by
    # ray.client().connect() so it should create a fourth one.
    p4 = run_string_as_driver_nonblocking(blocking_noaddr_script)

    wait_for_condition(
        lambda: len(ray._private.services.find_gcs_addresses()) == 4,
        retry_interval_ms=1000,
    )

    p1.kill()
    p2.kill()
    p3.kill()
    p4.kill()
    # Prevent flakiness since fatesharing takes some time.
    subprocess.check_output("ray stop --force", shell=True)

    # Since there's a cluster started with `ray start --head`
    # we should connect to it instead.
    subprocess.check_output("ray start --head", shell=True)
    # The assertion in the driver should cause the script to fail if we start
    # a new cluster instead of connecting.
    run_string_as_driver(
        """
import ray
ray.client().connect()
assert len(ray._private.services.find_gcs_addresses()) == 1
    """
    )
    # ray.client("local").connect() should always create a new cluster even if
    # there's one running.
    p1 = run_string_as_driver_nonblocking(blocking_local_script)
    wait_for_condition(
        lambda: len(ray._private.services.find_gcs_addresses()) == 2,
        retry_interval_ms=1000,
    )
    p1.kill()
    subprocess.check_output("ray stop --force", shell=True)


def test_non_existent_modules():
    exception = None
    try:
        ray.client("badmodule://address")
    except RuntimeError as e:
        exception = e

    assert exception is not None, "Bad Module did not raise RuntimeException"
    assert "does not exist" in str(exception)


def test_module_lacks_client_builder():
    mock_importlib = Mock()

    def mock_import_module(module_string):
        if module_string == "ray":
            return ray
        else:
            # Mock() does not have a `ClientBuilder` in its scope
            return Mock()

    mock_importlib.import_module = mock_import_module
    with patch("ray.client_builder.importlib", mock_importlib):
        assert isinstance(ray.client(""), ray.ClientBuilder)
        assert isinstance(ray.client("ray://"), ray.ClientBuilder)
        exception = None
        try:
            ray.client("othermodule://")
        except AssertionError as e:
            exception = e
        assert (
            exception is not None
        ), "Module without ClientBuilder did not raise AssertionError"
        assert "does not have ClientBuilder" in str(exception)


@pytest.mark.skipif(sys.platform == "win32", reason="RC Proxy is Flaky on Windows.")
def test_disconnect(call_ray_stop_only, set_enable_auto_connect):
    subprocess.check_output(
        "ray start --head --ray-client-server-port=25555", shell=True
    )
    with ray.client("localhost:25555").namespace("n1").connect():
        # Connect via Ray Client
        namespace = ray.get_runtime_context().namespace
        assert namespace == "n1"
        assert ray.util.client.ray.is_connected()

    with pytest.raises(ray.exceptions.RaySystemError):
        ray.put(300)

    with ray.client(None).namespace("n1").connect():
        # Connect Directly via Driver
        namespace = ray.get_runtime_context().namespace
        assert namespace == "n1"
        assert not ray.util.client.ray.is_connected()

    with pytest.raises(ray.exceptions.RaySystemError):
        ray.put(300)

    ctx = ray.client("localhost:25555").namespace("n1").connect()
    # Connect via Ray Client
    namespace = ray.get_runtime_context().namespace
    assert namespace == "n1"
    assert ray.util.client.ray.is_connected()
    ctx.disconnect()
    # Check idempotency
    ctx.disconnect()
    with pytest.raises(ray.exceptions.RaySystemError):
        ray.put(300)


@pytest.mark.skipif(sys.platform == "win32", reason="RC Proxy is Flaky on Windows.")
def test_address_resolution(call_ray_stop_only):
    subprocess.check_output(
        "ray start --head --ray-client-server-port=50055", shell=True
    )

    with ray.client("localhost:50055").connect():
        assert ray.util.client.ray.is_connected()

    try:
        os.environ["RAY_ADDRESS"] = "local"
        with ray.client("localhost:50055").connect():
            # client(...) takes precedence of RAY_ADDRESS=local
            assert ray.util.client.ray.is_connected()

        # This tries to call `ray.init(address="local") which creates a new Ray
        # instance.
        with ray.client(None).connect():
            wait_for_condition(
                lambda: len(ray._private.services.find_gcs_addresses()) == 2,
                retry_interval_ms=1000,
            )

    finally:
        if os.environ.get("RAY_ADDRESS"):
            del os.environ["RAY_ADDRESS"]
        ray.shutdown()


def mock_connect(*args, **kwargs):
    """
    Force exit instead of actually attempting to connect
    """
    raise ConnectionError


def has_client_deprecation_warn(warning: Warning, expected_replacement: str) -> bool:
    """
    Returns true if expected_replacement is in the message of the passed
    warning, and that the warning mentions deprecation.
    """
    start = "Starting a connection through `ray.client` will be deprecated"
    message = str(warning.message)
    if start not in message:
        return False
    if expected_replacement not in message:
        return False
    return True


@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --num-cpus=2 --min-worker-port=0 --max-worker-port=0 "
        "--port 0 --ray-client-server-port=50056"
    ],
    indirect=True,
)
def test_task_use_prestarted_worker(call_ray_start):
    ray.init("ray://localhost:50056")

    assert len(list_workers(filters=[("worker_type", "!=", "DRIVER")])) == 2

    @ray.remote(num_cpus=2)
    def f():
        return 42

    assert ray.get(f.remote()) == 42

    assert len(list_workers(filters=[("worker_type", "!=", "DRIVER")])) == 2


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
