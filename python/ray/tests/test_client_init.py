"""Client tests that run their own init (as with init_and_serve) live here"""
import time
import random

import ray.util.client.server.server as ray_client_server

from ray.util.client import RayAPIStub

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


def test_basic_preregister():
    from ray.util.client import ray
    server, _ = ray_client_server.init_and_serve("localhost:50051")
    try:
        ray.connect("localhost:50051")
        val = ray.get(hello_world.remote())
        print(val)
        assert val >= 20
        assert val <= 200
    finally:
        ray.disconnect()
        ray_client_server.shutdown_with_server(server)
        time.sleep(2)


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
