from contextlib import contextmanager

import ray.util.client.server.server as ray_client_server
from ray.util.client import ray


@contextmanager
def ray_start_client_server():
    with ray_start_client_server_pair() as pair:
        client, server = pair
        yield client


@contextmanager
def ray_start_client_server_pair():
    ray._inside_client_test = True
    server = ray_client_server.serve("localhost:50051")
    ray.connect("localhost:50051")
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)
