"""Client tests that run their own init (as with init_and_serve) live here"""
import pytest

import time
import random
import sys

import ray.util.client.server.server as ray_client_server
import ray.core.generated.ray_client_pb2 as ray_client_pb2

from ray.util.client import RayAPIStub, CURRENT_PROTOCOL_VERSION

import ray


@ray.remote
def hello_world():
    c1 = complex_task.remote(random.randint(1, 10))
    c2 = complex_task.remote(random.randint(1, 10))
    return sum(ray.get([c1, c2]))


@ray.remote
def complex_task(value):
    time.sleep(1)
    return value * 10


@ray.remote
class C:
    def __init__(self, x):
        self.val = x

    def double(self):
        self.val += self.val

    def get(self):
        return self.val


def test_basic_preregister():
    from ray.util.client import ray
    server, _ = ray_client_server.init_and_serve("localhost:50051")
    try:
        ray.connect("localhost:50051")
        val = ray.get(hello_world.remote())
        print(val)
        assert val >= 20
        assert val <= 200
        c = C.remote(3)
        x = c.double.remote()
        y = c.double.remote()
        ray.wait([x, y])
        val = ray.get(c.get.remote())
        assert val == 12
    finally:
        ray.disconnect()
        ray_client_server.shutdown_with_server(server)
        time.sleep(2)


def test_num_clients():
    # Tests num clients reporting; useful if you want to build an app that
    # load balances clients between Ray client servers.
    server_handle, _ = ray_client_server.init_and_serve("localhost:50051")
    server = server_handle.grpc_server
    try:
        api1 = RayAPIStub()
        info1 = api1.connect("localhost:50051")
        assert info1["num_clients"] == 1, info1
        api2 = RayAPIStub()
        info2 = api2.connect("localhost:50051")
        assert info2["num_clients"] == 2, info2

        # Disconnect the first two clients.
        api1.disconnect()
        api2.disconnect()
        time.sleep(1)

        api3 = RayAPIStub()
        info3 = api3.connect("localhost:50051")
        assert info3["num_clients"] == 1, info3

        # Check info contains ray and python version.
        assert isinstance(info3["ray_version"], str), info3
        assert isinstance(info3["ray_commit"], str), info3
        assert isinstance(info3["python_version"], str), info3
        assert isinstance(info3["protocol_version"], str), info3
        api3.disconnect()
    finally:
        ray_client_server.shutdown_with_server(server)
        time.sleep(2)


def test_python_version():

    server_handle, _ = ray_client_server.init_and_serve("localhost:50051")
    try:
        ray = RayAPIStub()
        info1 = ray.connect("localhost:50051")
        assert info1["python_version"] == ".".join(
            [str(x) for x in list(sys.version_info)[:3]])
        ray.disconnect()
        time.sleep(1)

        def mock_connection_response():
            return ray_client_pb2.ConnectionInfoResponse(
                num_clients=1,
                python_version="2.7.12",
                ray_version="",
                ray_commit="",
                protocol_version=CURRENT_PROTOCOL_VERSION,
            )

        # inject mock connection function
        server_handle.data_servicer._build_connection_response = \
            mock_connection_response

        ray = RayAPIStub()
        with pytest.raises(RuntimeError):
            _ = ray.connect("localhost:50051")

        ray = RayAPIStub()
        info3 = ray.connect("localhost:50051", ignore_version=True)
        assert info3["num_clients"] == 1, info3
        ray.disconnect()
    finally:
        ray_client_server.shutdown_with_server(server_handle.grpc_server)
        time.sleep(2)


def test_protocol_version():

    server_handle, _ = ray_client_server.init_and_serve("localhost:50051")
    try:
        ray = RayAPIStub()
        info1 = ray.connect("localhost:50051")
        local_py_version = ".".join(
            [str(x) for x in list(sys.version_info)[:3]])
        assert info1["protocol_version"] == CURRENT_PROTOCOL_VERSION, info1
        ray.disconnect()
        time.sleep(1)

        def mock_connection_response():
            return ray_client_pb2.ConnectionInfoResponse(
                num_clients=1,
                python_version=local_py_version,
                ray_version="",
                ray_commit="",
                protocol_version="2050-01-01",  # from the future
            )

        # inject mock connection function
        server_handle.data_servicer._build_connection_response = \
            mock_connection_response

        ray = RayAPIStub()
        with pytest.raises(RuntimeError):
            _ = ray.connect("localhost:50051")

        ray = RayAPIStub()
        info3 = ray.connect("localhost:50051", ignore_version=True)
        assert info3["num_clients"] == 1, info3
        ray.disconnect()
    finally:
        ray_client_server.shutdown_with_server(server_handle.grpc_server)
        time.sleep(2)
