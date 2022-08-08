import os
import sys
import unittest.mock
import subprocess

import grpc
import pytest

import ray
import ray._private.services
from ray.client_builder import ClientContext
from ray.cluster_utils import Cluster
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

    addr = "localhost:{}".format(address.split(":")[-1])
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
@pytest.mark.parametrize("address", [None, "auto"])
def test_ray_init_existing_instance_crashed(address):
    ray._private.utils.write_ray_address("localhost:6379")
    try:
        # If no address is specified, we will default to an existing cluster.
        ray._private.node.NUM_REDIS_GET_RETRIES = 1
        with pytest.raises(ConnectionError):
            ray.init(address=address)
    finally:
        ray._private.utils.reset_ray_address()


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

    addr = "localhost:{}".format(address.split(":")[-1])
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
    address = call_ray_start.split(":")[0]
    with unittest.mock.patch.dict(
        os.environ, {"RAY_ADDRESS": f"ray://{address}:25036"}
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


if __name__ == "__main__":
    import sys

    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
