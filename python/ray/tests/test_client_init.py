"""Client tests that run their own init (as with init_and_serve) live here"""
import pytest

import time
import sys

import ray.util.client.server.server as ray_client_server
import ray.core.generated.ray_client_pb2 as ray_client_pb2

from ray.util.client import RayAPIStub


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
