import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest.mock
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import pytest

import ray
from ray._common.network_utils import build_address, parse_address
from ray._private import ray_constants
from ray._private.test_utils import external_redis_test_enabled
from ray.client_builder import ClientContext
from ray.cluster_utils import Cluster
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.util.client.common import ClientObjectRef
from ray.util.client.ray_client_helpers import ray_start_client_server
from ray.util.client.worker import Worker


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform == "win32",
    reason="Flaky when run on windows CI",
)
@pytest.mark.parametrize("input", [None, "auto"])
def test_ray_address(input, call_ray_start):
    address = call_ray_start
    with unittest.mock.patch.dict(os.environ, {"RAY_ADDRESS": address}):
        res = ray.init(input)
        # Ensure this is not a client.connect()
        assert not isinstance(res, ClientContext)
        assert res.address_info["gcs_address"] == address
        ray.shutdown()

    addr = f"localhost:{parse_address(address)[-1]}"
    with unittest.mock.patch.dict(os.environ, {"RAY_ADDRESS": addr}):
        res = ray.init(input)
        # Ensure this is not a client.connect()
        assert not isinstance(res, ClientContext)
        assert res.address_info["gcs_address"] == address
        ray.shutdown()


@pytest.mark.parametrize("address", [None, "auto"])
def test_ray_init_no_local_instance(shutdown_only, address):
    # Starts a new Ray instance.
    if address is None:
        ray.init(address=address)
    else:
        # Throws an error if we explicitly want to connect to an existing
        # instance and none exists.
        with pytest.raises(ConnectionError):
            ray.init(address=address)


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform == "win32",
    reason="Flaky when run on windows CI",
)
@pytest.mark.parametrize("address", [None, "auto"])
def test_ray_init_existing_instance(call_ray_start, address):
    ray_address = call_ray_start
    # If no address is specified, we will default to an existing cluster.
    res = ray.init(address=address)
    assert res.address_info["gcs_address"] == ray_address
    ray.shutdown()

    # Start a second local Ray instance.
    try:
        subprocess.check_output("ray start --head", shell=True)
        # If there are multiple local instances, connect to the latest.
        res = ray.init(address=address)
        assert res.address_info["gcs_address"] != ray_address
        ray.shutdown()

        # If there are multiple local instances and we specify an address
        # explicitly, it works.
        with unittest.mock.patch.dict(os.environ, {"RAY_ADDRESS": ray_address}):
            res = ray.init(address=address)
            assert res.address_info["gcs_address"] == ray_address
    finally:
        ray.shutdown()
        subprocess.check_output("ray stop --force", shell=True)


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform == "win32",
    reason="Flaky when run on windows CI",
)
def test_ray_init_existing_instance_via_blocked_ray_start():
    """Run a blocked ray start command and check that ray.init() connects to it."""
    blocked_start_cmd = subprocess.Popen(
        ["ray", "start", "--head", "--block", "--num-cpus", "1999"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def _wait_for_startup_msg():
        for line in blocked_start_cmd.stdout:
            l = line.decode("utf-8")
            print(l)
            if "Ray runtime started." in l:
                return

    try:
        # Wait for the blocked start command's output to indicate that the local Ray
        # instance has been started successfully. This is done in a background thread
        # because there is no direct way to read the process' stdout with a timeout.
        tp = ThreadPoolExecutor(max_workers=1)
        fut = tp.submit(_wait_for_startup_msg)
        fut.result(timeout=30)

        # Verify that `ray.init()` connects to the existing cluster
        # (verified by checking the resources specified to the `ray start` command).
        ray.init()
        assert ray.cluster_resources().get("CPU", 0) == 1999
    finally:
        ray.shutdown()
        blocked_start_cmd.terminate()
        blocked_start_cmd.wait()
        tp.shutdown()
        subprocess.check_output("ray stop --force", shell=True)


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform == "win32",
    reason="Flaky when run on windows CI",
)
@pytest.mark.parametrize("address", [None, "auto"])
def test_ray_init_existing_instance_crashed(address):
    ray._private.utils.write_ray_address("localhost:6379")
    try:
        # If no address is specified, we will default to an existing cluster.
        ray_constants.NUM_REDIS_GET_RETRIES = 1
        with pytest.raises(ConnectionError):
            ray.init(address=address)
    finally:
        ray._common.utils.reset_ray_address()


class Credentials(grpc.ChannelCredentials):
    def __init__(self, name):
        self.name = name


class Stop(Exception):
    def __init__(self, credentials):
        self.credentials = credentials


def test_ray_init_credentials_with_client(monkeypatch):
    def mock_init(
        self,
        conn_str="",
        secure=False,
        metadata=None,
        connection_retries=3,
        _credentials=None,
    ):
        raise (Stop(_credentials))

    monkeypatch.setattr(Worker, "__init__", mock_init)
    with pytest.raises(Stop) as stop:
        with ray_start_client_server(_credentials=Credentials("test")):
            pass

    assert stop.value.credentials.name == "test"


def test_ray_init_credential(monkeypatch):
    def mock_secure_channel(conn_str, credentials, options=None, compression=None):
        raise (Stop(credentials))

    monkeypatch.setattr(grpc, "secure_channel", mock_secure_channel)

    with pytest.raises(Stop) as stop:
        ray.init("ray://127.0.0.1", _credentials=Credentials("test"))

    ray.util.disconnect()
    assert stop.value.credentials.name == "test"


def test_auto_init_non_client(call_ray_start):
    address = call_ray_start
    with unittest.mock.patch.dict(os.environ, {"RAY_ADDRESS": address}):
        res = ray.put(300)
        # Ensure this is not a client.connect()
        assert not isinstance(res, ClientObjectRef)
        ray.shutdown()

    addr = f"localhost:{parse_address(address)[-1]}"
    with unittest.mock.patch.dict(os.environ, {"RAY_ADDRESS": addr}):
        res = ray.put(300)
        # Ensure this is not a client.connect()
        assert not isinstance(res, ClientObjectRef)


@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head --ray-client-server-port 25036 --port 0"],
    indirect=True,
)
@pytest.mark.parametrize(
    "function", [lambda: ray.put(300), lambda: ray.remote(ray.nodes).remote()]
)
def test_auto_init_client(call_ray_start, function):
    address = parse_address(call_ray_start)[0]

    with unittest.mock.patch.dict(
        os.environ, {"RAY_ADDRESS": f"ray://{build_address(address, 25036)}"}
    ):
        res = function()
        # Ensure this is a client connection.
        assert isinstance(res, ClientObjectRef)
        ray.shutdown()

    with unittest.mock.patch.dict(os.environ, {"RAY_ADDRESS": "ray://localhost:25036"}):
        res = function()
        # Ensure this is a client connection.
        assert isinstance(res, ClientObjectRef)


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="This test is only run on linux CI machines.",
)
def test_ray_init_using_hostname(ray_start_cluster):
    import socket

    hostname = socket.gethostname()
    cluster = Cluster(
        initialize_head=True,
        head_node_args={
            "node_ip_address": hostname,
        },
    )

    # Use `ray.init` to test the connection.
    ray.init(address=cluster.address, _node_ip_address=hostname)

    node_table = cluster.global_state.node_table()
    assert len(node_table) == 1
    assert node_table[0].get("NodeManagerHostname", "") == hostname


def test_new_ray_instance_new_session_dir(shutdown_only):
    ray.init()
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    ray.shutdown()
    ray.init()
    if external_redis_test_enabled():
        assert ray._private.worker._global_node.get_session_dir_path() == session_dir
    else:
        assert ray._private.worker._global_node.get_session_dir_path() != session_dir


def test_new_cluster_new_session_dir(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    ray.init(address=cluster.address)
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    ray.shutdown()
    cluster.shutdown()
    cluster.add_node()
    ray.init(address=cluster.address)
    if external_redis_test_enabled():
        assert ray._private.worker._global_node.get_session_dir_path() == session_dir
    else:
        assert ray._private.worker._global_node.get_session_dir_path() != session_dir
    ray.shutdown()
    cluster.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM only on posix")
def test_ray_init_sigterm_handler():
    TEST_FILENAME = "sigterm.txt"

    def sigterm_handler_cmd(ray_init=False):
        return f"""
import os
import sys
import signal
def sigterm_handler(signum, frame):
    f = open("{TEST_FILENAME}", "w")
    sys.exit(0)
signal.signal(signal.SIGTERM, sigterm_handler)

import ray
{"ray.init()" if ray_init else ""}
os.kill(os.getpid(), signal.SIGTERM)
"""

    # test if sigterm handler is not overwritten by import ray
    test_child = subprocess.run(["python", "-c", sigterm_handler_cmd()])
    assert test_child.returncode == 0 and os.path.exists(TEST_FILENAME)
    os.remove(TEST_FILENAME)

    # test if sigterm handler is overwritten by ray.init
    test_child = subprocess.run(["python", "-c", sigterm_handler_cmd(ray_init=True)])
    assert test_child.returncode == signal.SIGTERM and not os.path.exists(TEST_FILENAME)


@pytest.fixture
def runtime_env_working_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        working_dir = path / "working_dir"
        working_dir.mkdir(parents=True)
        yield working_dir


@pytest.fixture
def py_module_whl():
    f = tempfile.NamedTemporaryFile(suffix=".whl", delete=False)
    f.close()
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def ray_shutdown():
    yield
    ray.shutdown()


def test_ray_init_with_runtime_env_as_dict(
    runtime_env_working_dir, py_module_whl, ray_shutdown
):
    working_dir_path = runtime_env_working_dir
    working_dir_str = str(working_dir_path)
    ray.init(
        runtime_env={"working_dir": working_dir_str, "py_modules": [py_module_whl]}
    )
    worker = ray._private.worker.global_worker.core_worker
    parsed_runtime_env = json.loads(worker.get_current_runtime_env())
    assert "gcs://" in parsed_runtime_env["working_dir"]
    assert len(parsed_runtime_env["py_modules"]) == 1
    assert "gcs://" in parsed_runtime_env["py_modules"][0]


def test_ray_init_with_runtime_env_as_object(
    runtime_env_working_dir, py_module_whl, ray_shutdown
):
    working_dir_path = runtime_env_working_dir
    working_dir_str = str(working_dir_path)
    ray.init(
        runtime_env=RuntimeEnv(working_dir=working_dir_str, py_modules=[py_module_whl])
    )
    worker = ray._private.worker.global_worker.core_worker
    parsed_runtime_env = json.loads(worker.get_current_runtime_env())
    assert "gcs://" in parsed_runtime_env["working_dir"]
    assert len(parsed_runtime_env["py_modules"]) == 1
    assert "gcs://" in parsed_runtime_env["py_modules"][0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
