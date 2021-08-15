import os
import pytest
import subprocess
import sys
from unittest.mock import patch, Mock

import ray
import ray.util.client.server.server as ray_client_server
import ray.client_builder as client_builder
from ray.test_utils import run_string_as_driver_nonblocking,\
    wait_for_condition, run_string_as_driver

from ray.cluster_utils import Cluster


@pytest.mark.parametrize("address", [
    "localhost:1234", "localhost:1234/url?params",
    "1.2.3.4/cluster-1?test_param=param1?"
])
def test_split_address(address):
    assert client_builder._split_address(address) == ("ray", address)

    specified_module = f"ray://{address}"
    assert client_builder._split_address(specified_module) == ("ray", address)

    specified_other_module = f"module://{address}"
    assert client_builder._split_address(specified_other_module) == ("module",
                                                                     address)
    non_url_compliant_module = f"module_test://{address}"
    assert client_builder._split_address(non_url_compliant_module) == (
        "module_test", address)


@pytest.mark.parametrize(
    "address",
    ["localhost", "1.2.3.4:1200", "ray://1.2.3.4:1200", "local", None])
def test_client(address):
    builder = client_builder.client(address)
    assert isinstance(builder, client_builder.ClientBuilder)
    if address in ("local", None):
        assert isinstance(builder, client_builder._LocalClientBuilder)
    else:
        assert type(builder) == client_builder.ClientBuilder
        assert builder.address == address.replace("ray://", "")


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_namespace():
    """
    Most of the "checks" in this test case rely on the fact that
    `run_string_as_driver` will throw an exception if the driver string exits
    with a non-zero exit code (e.g. when the driver scripts throws an
    exception). Since all of these drivers start named, detached actors, the
    most likely failure case would be a collision of named actors if they're
    put in the same namespace.

    This test checks that:
    * When two drivers don't specify a namespace, they are placed in different
      anonymous namespaces.
    * When two drivers specify a namespace, they collide.
    * The namespace name (as provided by the runtime context) is correct.
    """
    cluster = Cluster()
    cluster.add_node(num_cpus=4, ray_client_server_port=50055)
    cluster.wait_for_nodes(1)

    template = """
import ray
ray.client("localhost:50055").namespace({namespace}).connect()

@ray.remote
class Foo:
    def ping(self):
        return "pong"

a = Foo.options(lifetime="detached", name="abc").remote()
ray.get(a.ping.remote())
print(ray.get_runtime_context().namespace)
    """

    anon_driver = template.format(namespace="None")
    run_string_as_driver(anon_driver)
    # This second run will fail if the actors don't run in separate anonymous
    # namespaces.
    run_string_as_driver(anon_driver)

    run_in_namespace = template.format(namespace="'namespace'")
    script_namespace = run_string_as_driver(run_in_namespace)
    # The second run fails because the actors are run in the same namespace.
    with pytest.raises(subprocess.CalledProcessError):
        run_string_as_driver(run_in_namespace)

    assert script_namespace.strip() == "namespace"
    subprocess.check_output("ray stop --force", shell=True)


def test_connect_to_cluster(ray_start_regular_shared):
    server = ray_client_server.serve("localhost:50055")
    with ray.client("localhost:50055").connect() as client_context:
        assert client_context.dashboard_url == ray.worker.get_dashboard_url()
        python_version = ".".join([str(x) for x in list(sys.version_info)[:3]])
        assert client_context.python_version == python_version
        assert client_context.ray_version == ray.__version__
        assert client_context.ray_commit == ray.__commit__
        protocol_version = ray.util.client.CURRENT_PROTOCOL_VERSION
        assert client_context.protocol_version == protocol_version

    server.stop(0)
    subprocess.check_output("ray stop --force", shell=True)


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
    blocking_local_script = driver_template.format(
        address="'local'", blocking=True)
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
        lambda: len(ray._private.services.find_redis_address()) == 4,
        retry_interval_ms=1000)

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
    run_string_as_driver("""
import ray
ray.client().connect()
assert len(ray._private.services.find_redis_address()) == 1
    """)
    # ray.client("local").connect() should always create a new cluster even if
    # there's one running.
    p1 = run_string_as_driver_nonblocking(blocking_local_script)
    wait_for_condition(
        lambda: len(ray._private.services.find_redis_address()) == 2,
        retry_interval_ms=1000)
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
        assert exception is not None, ("Module without ClientBuilder did not "
                                       "raise AssertionError")
        assert "does not have ClientBuilder" in str(exception)


@pytest.mark.skipif(
    sys.platform == "win32", reason="RC Proxy is Flaky on Windows.")
def test_disconnect(call_ray_stop_only, set_enable_auto_connect):
    subprocess.check_output(
        "ray start --head --ray-client-server-port=25555", shell=True)
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


@pytest.mark.skipif(
    sys.platform == "win32", reason="RC Proxy is Flaky on Windows.")
def test_address_resolution(call_ray_stop_only):
    subprocess.check_output(
        "ray start --head --ray-client-server-port=50055", shell=True)

    with ray.client("localhost:50055").connect():
        assert ray.util.client.ray.is_connected()

    try:
        os.environ["RAY_ADDRESS"] = "local"
        with ray.client("localhost:50055").connect():
            # client(...) takes precedence of RAY_ADDRESS=local
            assert ray.util.client.ray.is_connected()

        with pytest.raises(Exception):
            # This tries to call `ray.init(address="local") which
            # breaks.`
            ray.client(None).connect()

    finally:
        if os.environ.get("RAY_ADDRESS"):
            del os.environ["RAY_ADDRESS"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
