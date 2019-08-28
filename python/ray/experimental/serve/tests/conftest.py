import pytest

import ray
import ray.experimental.serve as srv


@pytest.fixture(scope="session")
def serve_instance():
    srv.init()
    srv.global_state.wait_until_http_ready()
    yield
    srv.global_state.shutdown()


@pytest.fixture(scope="session")
def ray_instance():
    ray_already_initialized = ray.is_initialized()
    if not ray_already_initialized:
        ray.init(object_store_memory=int(1e8))
    yield
    if not ray_already_initialized:
        ray.shutdown()
