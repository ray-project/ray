from contextlib import contextmanager

import ray.util.client.server.server as ray_client_server
from ray.util.client import ray


@contextmanager
def ray_start_client_server():
    ray._inside_client_test = True
    server = ray_client_server.serve("localhost:50051")
    ray.connect("localhost:50051")
    try:
        yield ray
    finally:
        ray._inside_client_test = False
        ray.disconnect()
        server.stop(0)
