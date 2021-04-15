from contextlib import contextmanager

import ray as real_ray
import ray.util.client.server.server as ray_client_server
from ray.util.client import ray


@contextmanager
def ray_start_client_server(metadata=None, ray_connect_handler=None):
    with ray_start_client_server_pair(
            metadata=metadata,
            ray_connect_handler=ray_connect_handler) as pair:
        client, server = pair
        yield client


@contextmanager
def ray_start_client_server_pair(metadata=None, ray_connect_handler=None):
    ray._inside_client_test = True
    server = ray_client_server.serve(
        "localhost:50051", ray_connect_handler=ray_connect_handler)
    ray.connect("localhost:50051", metadata=metadata)
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)


@contextmanager
def ray_start_cluster_client_server_pair(address):
    ray._inside_client_test = True

    def ray_connect_handler():
        real_ray.init(address=address)

    server = ray_client_server.serve(
        "localhost:50051", ray_connect_handler=ray_connect_handler)
    ray.connect("localhost:50051")
    try:
        yield ray, server
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)
