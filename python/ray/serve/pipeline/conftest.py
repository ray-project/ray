import pytest

import ray
from ray.serve.pipeline.test_utils import LOCAL_EXECUTION_ONLY


@pytest.fixture(scope="session")
def shared_ray_instance():
    if LOCAL_EXECUTION_ONLY:
        # Don't ray.init() if only testing local execution.
        yield
    else:
        # Overriding task_retry_delay_ms to relaunch actors more quickly.
        yield ray.init(num_cpus=36, _system_config={"task_retry_delay_ms": 50})
