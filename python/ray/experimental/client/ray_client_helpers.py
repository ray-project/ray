from contextlib import contextmanager

import ray.experimental.client.server.server as ray_client_server
from ray.experimental.client import ray


@contextmanager
def ray_start_client_server():
    server = ray_client_server.serve("localhost:50051", test_mode=True)
    ray.connect("localhost:50051")
    try:
        yield ray
    finally:
        ray.disconnect()
        server.stop(0)
