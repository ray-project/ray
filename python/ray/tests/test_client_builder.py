import os
import subprocess
import sys
import warnings
from unittest.mock import Mock, patch

import pytest

import ray
from ray._common.test_utils import wait_for_condition
import ray.client_builder as client_builder
import ray.util.client.server.server as ray_client_server
from ray._private.test_utils import (
    run_string_as_driver,
    run_string_as_driver_nonblocking,
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


def test_namespace(ray_start_cluster):
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
    cluster = ray_start_cluster
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
print("Current namespace:", ray.get_runtime_context().namespace)
    """

    anon_driver = template.format(namespace="None")
    run_string_as_driver(anon_driver)
    # This second run will fail if the actors don't run in separate anonymous
    # namespaces.
    run_string_as_driver(anon_driver)

    run_in_namespace = template.format(namespace="'namespace'")
    script_output = run_string_as_driver(run_in_namespace)
    # The second run fails because the actors are run in the same namespace.
    with pytest.raises(subprocess.CalledProcessError):
        run_string_as_driver(run_in_namespace)

    assert "Current namespace: namespace" in script_output
    subprocess.check_output("ray stop --force", shell=True)


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_start_local_cluster():
    """This tests that ray.client() starts a new local cluster when appropriate.

    * Using `ray.client("local").connect() ` should always create a new
      cluster.
    * Using `ray.client().connect()` should create a new cluster if it doesn't
      connect to an existing one that was started via `ray start --head`..
    """
    driver_template = """
import ray
info = ray.client({address}).connect()
print("NODE_ID:", ray.get_runtime_context().get_node_id())

# Block.
while True:
    time.sleep(1)
"""

    def _get_node_id(p: subprocess.Popen) -> str:
        l = p.stdout.readline().decode("ascii").strip()
        assert "NODE_ID" in l
        return l[len("NODE_ID: ") :]

    p1, p2, p3 = None, None, None
    unbuffered = {"PYTHONUNBUFFERED": "1"}
    try:
        # ray.client() should start a cluster if none is running.
        p1 = run_string_as_driver_nonblocking(
            driver_template.format(address=""), env=unbuffered
        )
        p1_node_id = _get_node_id(p1)

        # ray.client("local") should always start a cluster.
        p2 = run_string_as_driver_nonblocking(driver_template.format(address="'local'"))
        p2_node_id = _get_node_id(p2)

        # ray.client() shouldn't connect to a cluster started by ray.client() or
        # ray.client("local").
        p3 = run_string_as_driver_nonblocking(driver_template.format(address=""))
        p3_node_id = _get_node_id(p3)

        # Check that all three drivers started their own local clusters.
        assert len({p1_node_id, p2_node_id, p3_node_id}) == 3
    finally:
        # Kill processes concurrently.
        if p1 is not None:
            p1.kill()
        if p2 is not None:
            p2.kill()
        if p3 is not None:
            p3.kill()

        # Wait for processes to exit.
        if p1 is not None:
            p1.wait()
        if p2 is not None:
            p2.wait()
        if p3 is not None:
            p3.wait()


@pytest.mark.skipif(sys.platform == "win32", reason="Flaky on Windows.")
def test_connect_to_local_cluster(call_ray_start):
    """This tests that ray.client connects to a local cluster when appropriate.

    * Using `ray.client("local").connect() ` should always create a new
      cluster even if one is running.
    * Using `ray.client().connect()` should connect to a local cluster that was
      started with `ray start --head`.
    """
    driver_template = """
import ray
info = ray.client({address}).connect()
print("NODE_ID:", ray.get_runtime_context().get_node_id())
"""

    def _get_node_id(p: subprocess.Popen) -> str:
        l = p.stdout.readline().decode("ascii").strip()
        assert "NODE_ID" in l
        return l[len("NODE_ID: ") :]

    existing_node_id = ray.get_runtime_context().get_node_id()

    p1, p2 = None, None
    unbuffered = {"PYTHONUNBUFFERED": "1"}
    try:
        # ray.client() should connect to the running cluster.
        p1 = run_string_as_driver_nonblocking(
            driver_template.format(address=""), env=unbuffered
        )
        assert _get_node_id(p1) == existing_node_id

        # ray.client("local") should always start a cluster.
        p2 = run_string_as_driver_nonblocking(driver_template.format(address="'local'"))
        assert _get_node_id(p2) != existing_node_id
    finally:
        # Kill processes concurrently.
        if p1 is not None:
            p1.kill()
        if p2 is not None:
            p2.kill()

        # Wait for processes to exit.
        if p1 is not None:
            p1.wait()
        if p2 is not None:
            p2.wait()


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


@pytest.mark.skipif(
    sys.platform == "win32", reason="pip not supported in Windows runtime envs."
)
@pytest.mark.filterwarnings(
    "default:Starting a connection through `ray.client` will be deprecated"
)
def test_client_deprecation_warn():
    """
    Tests that calling ray.client directly raises a deprecation warning with
    a copy pasteable replacement for the client().connect() call converted
    to ray.init style.
    """
    # Test warning when local client mode is used
    with warnings.catch_warnings(record=True) as w:
        ray.client().connect()
        assert any(has_client_deprecation_warn(warning, "ray.init()") for warning in w)
        ray.shutdown()

    with warnings.catch_warnings(record=True) as w:
        ray.client().namespace("nmspc").env({"pip": ["requests"]}).connect()
    expected = (
        'ray.init(namespace="nmspc", runtime_env=<your_runtime_env>)'  # noqa E501
    )
    assert any(
        has_client_deprecation_warn(warning, expected) for warning in w  # noqa E501
    )
    ray.shutdown()

    server = ray_client_server.serve("localhost:50055")

    # Test warning when namespace and runtime env aren't specified
    with warnings.catch_warnings(record=True) as w:
        with ray.client("localhost:50055").connect():
            pass
    assert any(
        has_client_deprecation_warn(warning, 'ray.init("ray://localhost:50055")')
        for warning in w
    )

    # Test warning when just namespace specified
    with warnings.catch_warnings(record=True) as w:
        with ray.client("localhost:50055").namespace("nmspc").connect():
            pass
    assert any(
        has_client_deprecation_warn(
            warning, 'ray.init("ray://localhost:50055", namespace="nmspc")'
        )
        for warning in w
    )

    # Test that passing namespace through env doesn't add namespace to the
    # replacement
    with warnings.catch_warnings(record=True) as w, patch.dict(
        os.environ, {"RAY_NAMESPACE": "aksdj"}
    ):
        with ray.client("localhost:50055").connect():
            pass
    assert any(
        has_client_deprecation_warn(warning, 'ray.init("ray://localhost:50055")')
        for warning in w
    )

    # Skip actually connecting on these, since updating the runtime env is
    # time consuming
    with patch("ray.util.client_connect.connect", mock_connect):
        # Test warning when just runtime_env specified
        with warnings.catch_warnings(record=True) as w:
            try:
                ray.client("localhost:50055").env({"pip": ["requests"]}).connect()
            except ConnectionError:
                pass
        expected = 'ray.init("ray://localhost:50055", runtime_env=<your_runtime_env>)'  # noqa E501
        assert any(has_client_deprecation_warn(warning, expected) for warning in w)

        # Test warning works if both runtime env and namespace specified
        with warnings.catch_warnings(record=True) as w:
            try:
                ray.client("localhost:50055").namespace("nmspc").env(
                    {"pip": ["requests"]}
                ).connect()
            except ConnectionError:
                pass
        expected = 'ray.init("ray://localhost:50055", namespace="nmspc", runtime_env=<your_runtime_env>)'  # noqa E501
        assert any(has_client_deprecation_warn(warning, expected) for warning in w)

        # We don't expect namespace to appear in the warning message, since
        # it was configured through an env var
        with warnings.catch_warnings(record=True) as w, patch.dict(
            os.environ, {"RAY_NAMESPACE": "abcdef"}
        ):
            try:
                ray.client("localhost:50055").env({"pip": ["requests"]}).connect()
            except ConnectionError:
                pass
        expected = 'ray.init("ray://localhost:50055", runtime_env=<your_runtime_env>)'  # noqa E501
        assert any(has_client_deprecation_warn(warning, expected) for warning in w)

    # cleanup
    server.stop(0)
    subprocess.check_output("ray stop --force", shell=True)


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
    sys.exit(pytest.main(["-sv", __file__]))
