import pytest

import ray
from ray.serve.tests.conftest import _shared_serve_instance, serve_instance  # noqa


@pytest.fixture(scope="session")
def shared_ray_instance():
    yield ray.init(num_cpus=36, _system_config={"task_retry_delay_ms": 50})
