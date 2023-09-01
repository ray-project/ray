import time
import json

import pytest

import ray
from ray.job_submission import JobSubmissionClient, JobStatus
from ray.cluster_utils import Cluster
from ray.dashboard.modules.job.tests.conftest import (
    _driver_script_path,
)


def wait_until_status(client, job_id, status_to_wait_for, timeout_seconds=20):
    start = time.time()
    while time.time() - start <= timeout_seconds:
        status = client.get_job_status(job_id)
        print(f"status: {status}")
        if status in status_to_wait_for:
            break
        time.sleep(1)


def test_job_driver_inheritance():
    try:
        c = Cluster()
        c.add_node(num_cpus=1)
        # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
        client = JobSubmissionClient("http://127.0.0.1:8265")
        driver_script_path = _driver_script_path(
            "driver_runtime_env_inheritance.py")
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path}",
            runtime_env={
                "env_vars": {"A": "1", "B": "2"},
                "pip": ["requests"],
            }
        )

        def wait(job_id):
            wait_until_status(
                client,
                job_id,
                {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}
            )

        def get_runtime_env_from_logs(client, job_id):
            wait(job_id)
            assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
            logs = client.get_job_logs(job_id)
            return json.loads(logs.strip().split("\n")[-1])

        # Test key is merged
        runtime_env = get_runtime_env_from_logs(client, job_id)
        assert runtime_env == {"A": "1", "B": "2", "C": "1"}
        assert runtime_env["pip"] == ["requests"]
        assert "worker_process_setup_hook" in runtime_env

        # Test raise an exception upon key conflict
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=conda",
            runtime_env={
                "conda": ["numpy"],
            }
        )
        wait(job_id)
        status = client.get_job_status(job_id)
        print(status)

        # Test raise an exception upon env var conflict
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=conda",
            runtime_env={
                "env_vars": {"A": "1"},
            }
        )
        wait(job_id)
        status = client.get_job_status(job_id)
        print(status)
    finally:
        c.shutdown()


def test_runtime_env_hook_triggered_job_submission(create_ray_cluster):
    pass


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
