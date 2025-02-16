import ray
from ray import serve
import pytest


@pytest.fixture
def shutdown_ray_and_serve():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
