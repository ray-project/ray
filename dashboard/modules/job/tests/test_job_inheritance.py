import pytest

import ray
from ray.job_submission import JobSubmissionClient
from ray.dashboard.modules.job.tests.conftest import (
    _driver_script_path,
)


def test_job_driver_inheritance(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=1)

    # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
    client = JobSubmissionClient("http://127.0.0.1:8265")
    driver_script_path = _driver_script_path(
        "driver_runtime_env_inheritance.py")
    job_id = client.submit_job(
        # Entrypoint shell command to execute
        entrypoint=f"python {driver_script_path}",
        # runtime_env={"working_dir": "./"}
    )
    print(job_id)
    # Test key is merged
    # Test env var is merged
    # Test raise an exception upon key conflict
    # Test raise an exception upon env var conflict
    pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
