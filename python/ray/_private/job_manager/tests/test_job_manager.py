import logging
import time

import pytest

import ray
from ray._private.job_manager import JobManager, JobStatus
from ray._private.test_utils import wait_for_condition

TEST_NAMESPACE = "jobs_test_namespace"

@pytest.fixture(scope="session")
def shared_ray_instance():
    yield ray.init(num_cpus=16, namespace=TEST_NAMESPACE)

@pytest.fixture
def job_manager(shared_ray_instance):
    yield JobManager()

def test_basic_job(job_manager):
    job_id = job_manager.submit_job("echo 'hello'")
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.SUCCEEDED}
        return status == JobStatus.SUCCEEDED

    wait_for_condition(check_job_finished)
    assert job_manager.get_job_logs(job_id) == "'hello'"

def test_submit_job_with_entrypoint_script(job_manager):
    job_id = job_manager.submit_job(
        "python script.py",
        runtime_env={"working_dir": "s3://runtime-env-test/script.zip"}
    )
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.SUCCEEDED}
        return status == JobStatus.SUCCEEDED

    wait_for_condition(check_job_finished)
    assert job_manager.get_job_logs(job_id) == b"Executing main() from script.py !!"

def test_process_cleanup(job_manager):
    job_manager.submit_job("echo $$ && sleep infinity")

class TestRuntimeEnv:
    def test_inheritance(self, job_manager):
        # Test that the driver and actors/tasks inherit the right runtime_env.
        pass

    def test_multiple_runtime_envs(self, job_manager):
        # Test that you can run two jobs in different envs without conflict.
        pass