import os
import pytest

import ray

TEST_NAMESPACE = "ray_dag_test_namespace"


@pytest.fixture(scope="session")
def shared_ray_instance():
    # Remove ray address for test ray cluster in case we have
    # lingering RAY_ADDRESS="http://127.0.0.1:8265" from previous local job
    # submissions.
    if "RAY_ADDRESS" in os.environ:
        del os.environ["RAY_ADDRESS"]
    yield ray.init(num_cpus=16, namespace=TEST_NAMESPACE, log_to_driver=True)
