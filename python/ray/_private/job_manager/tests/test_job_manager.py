from uuid import uuid4

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


def test_submit_basic_echo(job_manager):
    job_id: str = str(uuid4())
    job_id = job_manager.submit_job(job_id, "echo hello")
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.SUCCEEDED}
        return status == JobStatus.SUCCEEDED

    wait_for_condition(check_job_finished)
    assert job_manager.get_job_stdout(job_id) == b"hello"


def test_submit_stderr(job_manager):
    job_id: str = str(uuid4())
    job_id = job_manager.submit_job(job_id, "echo error 1>&2")
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.SUCCEEDED}
        return status == JobStatus.SUCCEEDED

    wait_for_condition(check_job_finished)
    assert job_manager.get_job_stderr(job_id) == b"error"


def test_submit_ls_grep(job_manager):
    job_id: str = str(uuid4())
    job_id = job_manager.submit_job(job_id, "ls | grep test_job_manager.py")
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.SUCCEEDED}
        return status == JobStatus.SUCCEEDED

    wait_for_condition(check_job_finished)
    assert job_manager.get_job_stdout(job_id) == b"test_job_manager.py"


def test_subprocess_exception(job_manager):
    """
    Run a python script with exception, ensure:
    1) Job status is marked as failed
    2) Job manager can surface exception message back to stderr api
    3) Job no hanging job supervisor actor
    4) Empty stdout
    """
    job_id: str = str(uuid4())
    job_id = job_manager.submit_job(
        job_id, "python command_scripts/script_with_exception.py")
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.FAILED}
        return status == JobStatus.FAILED

    wait_for_condition(check_job_finished)
    stderr = job_manager.get_job_stderr(job_id).decode("utf-8")
    last_line = stderr.strip().splitlines()[-1]
    assert last_line == "Exception: Script failed with exception !"
    assert job_manager._get_actor_for_job(job_id) is None
    assert job_manager.get_job_stdout(job_id) == b""


def test_submit_with_s3_runtime_env(job_manager):
    job_id: str = str(uuid4())
    job_id = job_manager.submit_job(
        job_id,
        "python script.py",
        runtime_env={"working_dir": "s3://runtime-env-test/script.zip"})
    assert isinstance(job_id, str)

    def check_job_finished():
        status = job_manager.get_job_status(job_id)
        assert status in {JobStatus.RUNNING, JobStatus.SUCCEEDED}
        return status == JobStatus.SUCCEEDED

    wait_for_condition(check_job_finished)
    assert job_manager.get_job_stdout(
        job_id) == b"Executing main() from script.py !!"


# def test_process_cleanup(job_manager):
#     job_manager.submit_job("echo $$ && sleep infinity")


class TestRuntimeEnv:
    def test_inheritance(self, job_manager):
        # Test that the driver and actors/tasks inherit the right runtime_env.
        pass

    def test_multiple_runtime_envs(self, job_manager):
        # Test that you can run two jobs in different envs without conflict.
        pass
