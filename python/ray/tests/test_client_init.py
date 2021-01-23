"""Client tests that run their own init (as with init_and_serve) live here"""
import time

import ray.util.client.server.server as ray_client_server

from ray.util.client import RayAPIStub


def test_num_clients():
    # Tests num clients reporting; useful if you want to build an app that
    # load balances clients between Ray client servers.
    server, _ = ray_client_server.init_and_serve("localhost:50051")
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
