from uuid import uuid4
import tempfile
import os
import time
import psutil

import pytest

import ray
from ray._private.job_manager import (JobManager, JobStatus,
                                      JOB_ID_METADATA_KEY)
from ray._private.test_utils import SignalActor, wait_for_condition

TEST_NAMESPACE = "jobs_test_namespace"


@pytest.fixture(scope="session")
def shared_ray_instance():
    yield ray.init(num_cpus=16, namespace=TEST_NAMESPACE, log_to_driver=True)


@pytest.fixture
def job_manager(shared_ray_instance):
    yield JobManager()


def _driver_script_path(file_name: str) -> str:
    return os.path.join(
        os.path.dirname(__file__), "subprocess_driver_scripts", file_name)


def check_job_succeeded(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    if status == JobStatus.FAILED:
        stdout = job_manager.get_job_stdout(job_id)
        stderr = job_manager.get_job_stderr(job_id)
        raise RuntimeError(f"Job failed! stdout:\n{stdout}\nstderr:\n{stderr}")
    assert status in {
        JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCEEDED
    }
    return status == JobStatus.SUCCEEDED


def check_job_failed(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.FAILED}
    return status == JobStatus.FAILED


def check_job_stopped(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.STOPPED}
    return status == JobStatus.STOPPED


def check_subprocess_cleaned(pid):
    return psutil.pid_exists(pid) is False


class TestShellScriptExecution:
    def test_submit_basic_echo(self, job_manager):
        job_id = job_manager.submit_job("echo hello")

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(job_id) == b"hello"

    def test_submit_stderr(self, job_manager):
        job_id = job_manager.submit_job("echo error 1>&2")

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stderr(job_id) == b"error"

    def test_submit_ls_grep(self, job_manager):
        job_id = job_manager.submit_job(
            f"ls {os.path.dirname(__file__)} | grep test_job_manager.py")

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(job_id) == b"test_job_manager.py"

    def test_subprocess_exception(self, job_manager):
        """
        Run a python script with exception, ensure:
        1) Job status is marked as failed
        2) Job manager can surface exception message back to stderr api
        3) Job no hanging job supervisor actor
        4) Empty stdout
        """
        job_id = job_manager.submit_job(
            f"python {_driver_script_path('script_with_exception.py')}")

        wait_for_condition(
            check_job_failed, job_manager=job_manager, job_id=job_id)
        stderr = job_manager.get_job_stderr(job_id).decode("utf-8")
        last_line = stderr.strip().splitlines()[-1]
        assert last_line == "Exception: Script failed with exception !"
        assert job_manager._get_actor_for_job(job_id) is None
        assert job_manager.get_job_stdout(job_id) == b""

    def test_submit_with_s3_runtime_env(self, job_manager):
        job_id = job_manager.submit_job(
            "python script.py",
            runtime_env={"working_dir": "s3://runtime-env-test/script.zip"})

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(
            job_id) == b"Executing main() from script.py !!"


class TestRuntimeEnv:
    def test_inheritance(self, job_manager):
        # Test that the driver and actors/tasks inherit the right runtime_env.
        pass

    def test_pass_env_var(self, job_manager):
        """Test we can pass env vars in the subprocess that executes job's
        driver script.
        """
        job_id = job_manager.submit_job(
            "echo $TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "233"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(job_id) == b"233"

    def test_multiple_runtime_envs(self, job_manager):
        # Test that you can run two jobs in different envs without conflict.
        job_id_1 = job_manager.submit_job(
            f"python {_driver_script_path('print_runtime_env.py')}",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_1_VAR"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id_1)
        assert job_manager.get_job_stdout(
            job_id_1
        ) == b"{'env_vars': {'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_1_VAR'}}"  # noqa: E501

        job_id_2 = job_manager.submit_job(
            f"python {_driver_script_path('print_runtime_env.py')}",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_2_VAR"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id_2)
        assert job_manager.get_job_stdout(
            job_id_2
        ) == b"{'env_vars': {'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_2_VAR'}}"  # noqa: E501

    def test_env_var_and_driver_job_config_warning(self, job_manager):
        """Ensure we got error message from worker.py and job stderr
        if user provided runtime_env in both driver script and submit()
        """
        job_id = job_manager.submit_job(
            f"python {_driver_script_path('override_env_var.py')}",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_1_VAR"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(job_id) == b"JOB_1_VAR"
        stderr = job_manager.get_job_stderr(job_id).decode("utf-8")
        assert stderr.startswith(
            "Both RAY_JOB_CONFIG_JSON_ENV_VAR and ray.init(runtime_env) "
            "are provided")

    def test_failed_runtime_env_configuration(self, job_manager):
        """Ensure job status is correctly set as failed if job supervisor
        actor failed to setup runtime_env.
        """
        with pytest.raises(RuntimeError):
            job_id = job_manager.submit_job(
                f"python {_driver_script_path('override_env_var.py')}",
                runtime_env={"working_dir": "path_not_exist"})

            assert job_manager.get_job_status(job_id) == JobStatus.FAILED

    def test_pass_metadata(self, job_manager):
        def dict_to_binary(d):
            return str(dict(sorted(d.items()))).encode("utf-8")

        print_metadata_cmd = (
            "python -c\""
            "import ray;"
            "ray.init();"
            "job_config=ray.worker.global_worker.core_worker.get_job_config();"
            "print(dict(sorted(job_config.metadata.items())))"
            "\"")

        # Check that we default to only the job ID.
        job_id = job_manager.submit_job(print_metadata_cmd)

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(job_id) == dict_to_binary({
            JOB_ID_METADATA_KEY: job_id
        })

        # Check that we can pass custom metadata.
        job_id = job_manager.submit_job(
            print_metadata_cmd, metadata={
                "key1": "val1",
                "key2": "val2"
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_stdout(job_id) == dict_to_binary({
            JOB_ID_METADATA_KEY: job_id,
            "key1": "val1",
            "key2": "val2"
        })


class TestAsyncAPI:
    def _run_hanging_command(self,
                             job_manager,
                             tmp_dir,
                             _start_signal_actor=None):
        tmp_file = os.path.join(tmp_dir, "hello")
        pid_file = os.path.join(tmp_dir, "pid")

        # Write subprocess pid to pid_file and block until tmp_file is present.
        wait_for_file_cmd = (f"echo $$ > {pid_file} && "
                             f"until [ -f {tmp_file} ]; "
                             "do echo 'Waiting...' && sleep 1; "
                             "done")
        job_id = job_manager.submit_job(
            wait_for_file_cmd, _start_signal_actor=_start_signal_actor)

        for _ in range(10):
            time.sleep(0.1)
            status = job_manager.get_job_status(job_id)
            if _start_signal_actor:
                assert status == JobStatus.PENDING
                stdout = job_manager.get_job_stdout(job_id)
                assert b"No stdout log available yet." in stdout
            else:
                assert status == JobStatus.RUNNING
                stdout = job_manager.get_job_stdout(job_id)
                assert b"Waiting..." in stdout

        return pid_file, tmp_file, job_id

    def test_status_and_logs_while_blocking(self, job_manager):
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, tmp_file, job_id = self._run_hanging_command(
                job_manager, tmp_dir)
            with open(pid_file, "r") as file:
                pid = int(file.read())
                assert psutil.pid_exists(pid), (
                    "driver subprocess should be running")

            # Signal the job to exit by writing to the file.
            with open(tmp_file, "w") as f:
                print("hello", file=f)

            wait_for_condition(
                check_job_succeeded, job_manager=job_manager, job_id=job_id)
            # Ensure driver subprocess gets cleaned up after job reached
            # termination state
            wait_for_condition(check_subprocess_cleaned, pid=pid)

    def test_stop_job(self, job_manager):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, _, job_id = self._run_hanging_command(job_manager, tmp_dir)

            assert job_manager.stop_job(job_id) is True
            wait_for_condition(
                check_job_stopped, job_manager=job_manager, job_id=job_id)

            # Assert re-stopping a stopped job also returns False
            assert job_manager.stop_job(job_id) is False
            # Assert stopping non-existent job returns False
            assert job_manager.stop_job(str(uuid4())) is False

    def test_kill_job_actor_in_before_driver_finish(self, job_manager):
        """
        Test submitting a long running / blocker driver script, and kill
        the job supervisor actor before script returns and ensure

        1) Job status is correctly marked as failed
        2) No hanging subprocess from failed job
        """

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = self._run_hanging_command(
                job_manager, tmp_dir)
            with open(pid_file, "r") as file:
                pid = int(file.read())
                assert psutil.pid_exists(pid), (
                    "driver subprocess should be running")

            actor = job_manager._get_actor_for_job(job_id)
            ray.kill(actor, no_restart=True)
            wait_for_condition(
                check_job_failed, job_manager=job_manager, job_id=job_id)

            # Ensure driver subprocess gets cleaned up after job reached
            # termination state
            wait_for_condition(check_subprocess_cleaned, pid=pid)

    def test_stop_job_in_pending(self, job_manager):
        """
        Kick off a job that is in PENDING state, stop the job and ensure

        1) Job can correctly be stop immediately with correct JobStatus
        2) No dangling subprocess left.
        """
        _start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = self._run_hanging_command(
                job_manager, tmp_dir, _start_signal_actor=_start_signal_actor)
            assert not os.path.exists(pid_file), (
                "driver subprocess should NOT be running while job is "
                "still PENDING.")

            assert job_manager.stop_job(job_id) is True
            # Send run signal to unblock run function
            ray.get(_start_signal_actor.send.remote())
            wait_for_condition(
                check_job_stopped, job_manager=job_manager, job_id=job_id)

    def test_kill_job_actor_in_pending(self, job_manager):
        """
        Kick off a job that is in PENDING state, kill the job actor and ensure

        1) Job can correctly be stop immediately with correct JobStatus
        2) No dangling subprocess left.
        """
        _start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = self._run_hanging_command(
                job_manager, tmp_dir, _start_signal_actor=_start_signal_actor)

            assert not os.path.exists(pid_file), (
                "driver subprocess should NOT be running while job is "
                "still PENDING.")

            actor = job_manager._get_actor_for_job(job_id)
            ray.kill(actor, no_restart=True)
            wait_for_condition(
                check_job_failed, job_manager=job_manager, job_id=job_id)

    def test_stop_job_subprocess_cleanup_upon_stop(self, job_manager):
        """
        Ensure driver scripts' subprocess is cleaned up properly when we
        stopped a running job.

        SIGTERM first, SIGKILL after 3 seconds.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = self._run_hanging_command(
                job_manager, tmp_dir)
            with open(pid_file, "r") as file:
                pid = int(file.read())
                assert psutil.pid_exists(pid), (
                    "driver subprocess should be running")

            assert job_manager.stop_job(job_id) is True
            wait_for_condition(
                check_job_stopped, job_manager=job_manager, job_id=job_id)

            # Ensure driver subprocess gets cleaned up after job reached
            # termination state
            wait_for_condition(check_subprocess_cleaned, pid=pid)
