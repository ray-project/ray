import time
import json
import sys

import pytest

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
            return
        time.sleep(1)
    raise Exception


def test_job_driver_inheritance():
    try:
        c = Cluster()
        c.add_node(num_cpus=1)
        # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
        client = JobSubmissionClient("http://127.0.0.1:8265")
        driver_script_path = _driver_script_path("driver_runtime_env_inheritance.py")
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path}",
            runtime_env={
                "env_vars": {"A": "1", "B": "2"},
                "pip": ["requests"],
            },
        )

        def wait(job_id):
            wait_until_status(
                client,
                job_id,
                {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED},
                timeout_seconds=60,
            )

        def get_runtime_env_from_logs(client, job_id):
            wait(job_id)
            logs = client.get_job_logs(job_id)
            print(logs)
            assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
            return json.loads(logs.strip().split("\n")[-1])

        # Test key is merged
        print("Test key merged")
        runtime_env = get_runtime_env_from_logs(client, job_id)
        assert runtime_env["env_vars"] == {"A": "1", "B": "2", "C": "1"}
        assert runtime_env["pip"] == {"packages": ["requests"], "pip_check": False}

        # Test worker process setuphook works.
        print("Test key setup hook")
        expected_str = "HELLOWORLD"
        job_id = client.submit_job(
            entrypoint=(
                f"python {driver_script_path} "
                f"--worker-process-setup-hook {expected_str}"
            ),
            runtime_env={
                "env_vars": {"A": "1", "B": "2"},
            },
        )
        wait(job_id)
        logs = client.get_job_logs(job_id)
        assert expected_str in logs

        # Test raise an exception upon key conflict
        print("Test conflicting pip")
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=pip",
            runtime_env={"pip": ["numpy"]},
        )
        wait(job_id)
        status = client.get_job_status(job_id)
        logs = client.get_job_logs(job_id)
        assert status == JobStatus.FAILED
        assert "Failed to merge the Job's runtime env" in logs

        # Test raise an exception upon env var conflict
        print("Test conflicting env vars")
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=env_vars",
            runtime_env={
                "env_vars": {"A": "1"},
            },
        )
        wait(job_id)
        status = client.get_job_status(job_id)
        logs = client.get_job_logs(job_id)
        assert status == JobStatus.FAILED
        assert "Failed to merge the Job's runtime env" in logs
    finally:
        c.shutdown()


def test_job_driver_inheritance_override(monkeypatch):
    monkeypatch.setenv("RAY_OVERRIDE_JOB_RUNTIME_ENV", "1")

    try:
        c = Cluster()
        c.add_node(num_cpus=1)
        # If using a remote cluster, replace 127.0.0.1 with the head node's IP address.
        client = JobSubmissionClient("http://127.0.0.1:8265")
        driver_script_path = _driver_script_path("driver_runtime_env_inheritance.py")
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path}",
            runtime_env={
                "env_vars": {"A": "1", "B": "2"},
                "pip": ["requests"],
            },
        )

        def wait(job_id):
            wait_until_status(
                client,
                job_id,
                {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED},
                timeout_seconds=60,
            )

        def get_runtime_env_from_logs(client, job_id):
            wait(job_id)
            logs = client.get_job_logs(job_id)
            print(logs)
            assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
            return json.loads(logs.strip().split("\n")[-1])

        # Test conflict resolution regular field
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=pip",
            runtime_env={"pip": ["pip-install-test==0.5"]},
        )
        runtime_env = get_runtime_env_from_logs(client, job_id)
        print(runtime_env)
        assert runtime_env["pip"] == {"packages": ["numpy"], "pip_check": False}

        # Test raise an exception upon env var conflict
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=env_vars",
            runtime_env={
                "env_vars": {"A": "2"},
            },
        )
        runtime_env = get_runtime_env_from_logs(client, job_id)
        print(runtime_env)
        assert runtime_env["env_vars"]["A"] == "1"
    finally:
        c.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
