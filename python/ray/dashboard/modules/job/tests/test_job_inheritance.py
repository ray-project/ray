import json
import sys
import time

import pytest

from ray.cluster_utils import Cluster
from ray.dashboard.consts import RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR
from ray.dashboard.modules.job.tests.conftest import _driver_script_path
from ray.dashboard.modules.job.tests.subprocess_driver_scripts.driver_runtime_env_inheritance import (  # noqa: E501
    RUNTIME_ENV_LOG_LINE_PREFIX,
)
from ray.job_submission import JobStatus, JobSubmissionClient


def wait_until_status(client, job_id, status_to_wait_for, timeout_seconds=20):
    start = time.time()
    while time.time() - start <= timeout_seconds:
        status = client.get_job_status(job_id)
        print(f"status: {status}")
        if status in status_to_wait_for:
            return
        time.sleep(1)
    raise Exception


def wait(client, job_id):
    wait_until_status(
        client,
        job_id,
        {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED},
        timeout_seconds=60,
    )


def get_runtime_env_from_logs(client, job_id):
    wait(client, job_id)
    logs = client.get_job_logs(job_id)
    print(logs)
    assert client.get_job_status(job_id) == JobStatus.SUCCEEDED
    # Split logs by line, find the unique line that starts with
    # RUNTIME_ENV_LOG_LINE_PREFIX, strip it and parse it as JSON.
    lines = logs.strip().split("\n")
    assert len(lines) > 0
    for line in lines:
        if line.startswith(RUNTIME_ENV_LOG_LINE_PREFIX):
            return json.loads(line[len(RUNTIME_ENV_LOG_LINE_PREFIX) :])


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
        wait(client, job_id)
        logs = client.get_job_logs(job_id)
        assert expected_str in logs

        # Test raise an exception upon key conflict
        print("Test conflicting pip")
        job_id = client.submit_job(
            entrypoint=f"python {driver_script_path} --conflict=pip",
            runtime_env={"pip": ["numpy"]},
        )
        wait(client, job_id)
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
        wait(client, job_id)
        status = client.get_job_status(job_id)
        logs = client.get_job_logs(job_id)
        assert status == JobStatus.FAILED
        assert "Failed to merge the Job's runtime env" in logs
    finally:
        c.shutdown()


@pytest.mark.parametrize("stream_runtime_env_log", ["1", "0"])
def test_runtime_env_logs_streamed_to_job_driver_log(
    monkeypatch, stream_runtime_env_log
):
    monkeypatch.setenv(
        RAY_STREAM_RUNTIME_ENV_LOG_TO_JOB_DRIVER_LOG_ENV_VAR, stream_runtime_env_log
    )
    try:
        c = Cluster()
        c.add_node(num_cpus=1)
        client = JobSubmissionClient("http://127.0.0.1:8265")
        job_id = client.submit_job(
            entrypoint="echo hello world",
            runtime_env={"pip": ["requests==2.25.1"]},
        )
        wait(client, job_id)
        logs = client.get_job_logs(job_id)
        if stream_runtime_env_log == "0":
            assert "Creating virtualenv at" not in logs
        else:
            assert "Creating virtualenv at" in logs
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
