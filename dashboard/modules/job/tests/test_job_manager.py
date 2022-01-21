import os
import psutil
import tempfile
import sys
from uuid import uuid4
import signal

import pytest

import ray
from ray.dashboard.modules.job.common import (JobStatus, JOB_ID_METADATA_KEY,
                                              JOB_NAME_METADATA_KEY)
from ray.dashboard.modules.job.job_manager import generate_job_id, JobManager
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


def _run_hanging_command(job_manager, tmp_dir, start_signal_actor=None):
    tmp_file = os.path.join(tmp_dir, "hello")
    pid_file = os.path.join(tmp_dir, "pid")

    # Write subprocess pid to pid_file and block until tmp_file is present.
    wait_for_file_cmd = (f"echo $$ > {pid_file} && "
                         f"until [ -f {tmp_file} ]; "
                         "do echo 'Waiting...' && sleep 1; "
                         "done")
    job_id = job_manager.submit_job(
        entrypoint=wait_for_file_cmd, _start_signal_actor=start_signal_actor)

    status = job_manager.get_job_status(job_id)
    if start_signal_actor:
        for _ in range(10):
            assert status.status == JobStatus.PENDING
            logs = job_manager.get_job_logs(job_id)
            assert logs == ""
    else:
        wait_for_condition(
            check_job_running, job_manager=job_manager, job_id=job_id)

        wait_for_condition(
            lambda: "Waiting..." in job_manager.get_job_logs(job_id))

    return pid_file, tmp_file, job_id


def check_job_succeeded(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    if status.status == JobStatus.FAILED:
        raise RuntimeError(f"Job failed! {status.message}")
    assert status.status in {
        JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCEEDED
    }
    return status.status == JobStatus.SUCCEEDED


def check_job_failed(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    assert status.status in {
        JobStatus.PENDING, JobStatus.RUNNING, JobStatus.FAILED
    }
    return status.status == JobStatus.FAILED


def check_job_stopped(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    assert status.status in {
        JobStatus.PENDING, JobStatus.RUNNING, JobStatus.STOPPED
    }
    return status.status == JobStatus.STOPPED


def check_job_running(job_manager, job_id):
    status = job_manager.get_job_status(job_id)
    assert status.status in {JobStatus.PENDING, JobStatus.RUNNING}
    return status.status == JobStatus.RUNNING


def check_subprocess_cleaned(pid):
    return psutil.pid_exists(pid) is False


def test_generate_job_id():
    ids = set()
    for _ in range(10000):
        new_id = generate_job_id()
        assert new_id.startswith("raysubmit_")
        assert new_id.count("_") == 1
        assert "-" not in new_id
        assert "/" not in new_id
        ids.add(new_id)

    assert len(ids) == 10000


def test_pass_job_id(job_manager):
    job_id = "my_custom_id"

    returned_id = job_manager.submit_job(
        entrypoint="echo hello", job_id=job_id)
    assert returned_id == job_id

    wait_for_condition(
        check_job_succeeded, job_manager=job_manager, job_id=job_id)

    # Check that the same job_id is rejected.
    with pytest.raises(RuntimeError):
        job_manager.submit_job(entrypoint="echo hello", job_id=job_id)


class TestShellScriptExecution:
    def test_submit_basic_echo(self, job_manager):
        job_id = job_manager.submit_job(entrypoint="echo hello")

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_logs(job_id) == "hello\n"

    def test_submit_stderr(self, job_manager):
        job_id = job_manager.submit_job(entrypoint="echo error 1>&2")

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_logs(job_id) == "error\n"

    def test_submit_ls_grep(self, job_manager):
        grep_cmd = f"ls {os.path.dirname(__file__)} | grep test_job_manager.py"
        job_id = job_manager.submit_job(entrypoint=grep_cmd)

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_logs(job_id) == "test_job_manager.py\n"

    def test_subprocess_exception(self, job_manager):
        """
        Run a python script with exception, ensure:
        1) Job status is marked as failed
        2) Job manager can surface exception message back to logs api
        3) Job no hanging job supervisor actor
        4) Empty logs
        """
        run_cmd = f"python {_driver_script_path('script_with_exception.py')}"
        job_id = job_manager.submit_job(entrypoint=run_cmd)

        def cleaned_up():
            status = job_manager.get_job_status(job_id)
            if status.status != JobStatus.FAILED:
                return False
            if ("Exception: Script failed with exception !" not in
                    status.message):
                return False

            return job_manager._get_actor_for_job(job_id) is None

        wait_for_condition(cleaned_up)

    def test_submit_with_s3_runtime_env(self, job_manager):
        job_id = job_manager.submit_job(
            entrypoint="python script.py",
            runtime_env={
                "working_dir": "s3://runtime-env-test/script_runtime_env.zip"
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_logs(
            job_id) == "Executing main() from script.py !!\n"


class TestRuntimeEnv:
    def test_inheritance(self, job_manager):
        # Test that the driver and actors/tasks inherit the right runtime_env.
        pass

    def test_pass_env_var(self, job_manager):
        """Test we can pass env vars in the subprocess that executes job's
        driver script.
        """
        job_id = job_manager.submit_job(
            entrypoint="echo $TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "233"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert job_manager.get_job_logs(job_id) == "233\n"

    def test_multiple_runtime_envs(self, job_manager):
        # Test that you can run two jobs in different envs without conflict.
        job_id_1 = job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('print_runtime_env.py')}",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_1_VAR"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id_1)
        logs = job_manager.get_job_logs(job_id_1)
        assert "{'env_vars': {'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_1_VAR'}}" in logs  # noqa: E501

        job_id_2 = job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('print_runtime_env.py')}",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_2_VAR"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id_2)
        logs = job_manager.get_job_logs(job_id_2)
        assert "{'env_vars': {'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_2_VAR'}}" in logs  # noqa: E501

    def test_env_var_and_driver_job_config_warning(self, job_manager):
        """Ensure we got error message from worker.py and job logs
        if user provided runtime_env in both driver script and submit()
        """
        job_id = job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('override_env_var.py')}",
            runtime_env={
                "env_vars": {
                    "TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_1_VAR"
                }
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        logs = job_manager.get_job_logs(job_id)
        assert logs.startswith(
            "Both RAY_JOB_CONFIG_JSON_ENV_VAR and ray.init(runtime_env) "
            "are provided")
        assert "JOB_1_VAR" in logs

    def test_failed_runtime_env_validation(self, job_manager):
        """Ensure job status is correctly set as failed if job has an invalid
        runtime_env.
        """
        run_cmd = f"python {_driver_script_path('override_env_var.py')}"
        job_id = job_manager.submit_job(
            entrypoint=run_cmd, runtime_env={"working_dir": "path_not_exist"})

        status = job_manager.get_job_status(job_id)
        assert status.status == JobStatus.FAILED
        assert "path_not_exist is not a valid URI" in status.message

    def test_failed_runtime_env_setup(self, job_manager):
        """Ensure job status is correctly set as failed if job has a valid
        runtime_env that fails to be set up.
        """
        run_cmd = f"python {_driver_script_path('override_env_var.py')}"
        job_id = job_manager.submit_job(
            entrypoint=run_cmd,
            runtime_env={"working_dir": "s3://does_not_exist.zip"})

        wait_for_condition(
            check_job_failed, job_manager=job_manager, job_id=job_id)

        status = job_manager.get_job_status(job_id)
        assert "runtime_env setup failed" in status.message

    def test_pass_metadata(self, job_manager):
        def dict_to_str(d):
            return str(dict(sorted(d.items())))

        print_metadata_cmd = (
            "python -c\""
            "import ray;"
            "ray.init();"
            "job_config=ray.worker.global_worker.core_worker.get_job_config();"
            "print(dict(sorted(job_config.metadata.items())))"
            "\"")

        # Check that we default to only the job ID and job name.
        job_id = job_manager.submit_job(entrypoint=print_metadata_cmd)

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert dict_to_str({
            JOB_NAME_METADATA_KEY: job_id,
            JOB_ID_METADATA_KEY: job_id
        }) in job_manager.get_job_logs(job_id)

        # Check that we can pass custom metadata.
        job_id = job_manager.submit_job(
            entrypoint=print_metadata_cmd,
            metadata={
                "key1": "val1",
                "key2": "val2"
            })

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert dict_to_str({
            JOB_NAME_METADATA_KEY: job_id,
            JOB_ID_METADATA_KEY: job_id,
            "key1": "val1",
            "key2": "val2"
        }) in job_manager.get_job_logs(job_id)

        # Check that we can override job name.
        job_id = job_manager.submit_job(
            entrypoint=print_metadata_cmd,
            metadata={JOB_NAME_METADATA_KEY: "custom_name"})

        wait_for_condition(
            check_job_succeeded, job_manager=job_manager, job_id=job_id)
        assert dict_to_str({
            JOB_NAME_METADATA_KEY: "custom_name",
            JOB_ID_METADATA_KEY: job_id
        }) in job_manager.get_job_logs(job_id)


class TestAsyncAPI:
    def test_status_and_logs_while_blocking(self, job_manager):
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, tmp_file, job_id = _run_hanging_command(
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
            _, _, job_id = _run_hanging_command(job_manager, tmp_dir)

            assert job_manager.stop_job(job_id) is True
            wait_for_condition(
                check_job_stopped, job_manager=job_manager, job_id=job_id)
            # Assert re-stopping a stopped job also returns False
            wait_for_condition(lambda: job_manager.stop_job(job_id) is False)
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
            pid_file, _, job_id = _run_hanging_command(job_manager, tmp_dir)
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
        start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = _run_hanging_command(
                job_manager, tmp_dir, start_signal_actor=start_signal_actor)
            assert not os.path.exists(pid_file), (
                "driver subprocess should NOT be running while job is "
                "still PENDING.")

            assert job_manager.stop_job(job_id) is True
            # Send run signal to unblock run function
            ray.get(start_signal_actor.send.remote())
            wait_for_condition(
                check_job_stopped, job_manager=job_manager, job_id=job_id)

    def test_kill_job_actor_in_pending(self, job_manager):
        """
        Kick off a job that is in PENDING state, kill the job actor and ensure

        1) Job can correctly be stop immediately with correct JobStatus
        2) No dangling subprocess left.
        """
        start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = _run_hanging_command(
                job_manager, tmp_dir, start_signal_actor=start_signal_actor)

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
            pid_file, _, job_id = _run_hanging_command(job_manager, tmp_dir)
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


class TestTailLogs:
    async def _tail_and_assert_logs(self,
                                    job_id,
                                    job_manager,
                                    expected_log="",
                                    num_iteration=5):
        i = 0
        async for lines in job_manager.tail_job_logs(job_id):
            assert all(s == expected_log for s in lines.strip().split("\n"))
            print(lines, end="")
            if i == num_iteration:
                break
            i += 1

    @pytest.mark.asyncio
    async def test_unknown_job(self, job_manager):
        with pytest.raises(
                RuntimeError, match="Job 'unknown' does not exist."):
            async for _ in job_manager.tail_job_logs("unknown"):
                pass

    @pytest.mark.asyncio
    async def test_successful_job(self, job_manager):
        """Test tailing logs for a PENDING -> RUNNING -> SUCCESSFUL job."""
        start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            _, tmp_file, job_id = _run_hanging_command(
                job_manager, tmp_dir, start_signal_actor=start_signal_actor)

            # TODO(edoakes): check we get no logs before actor starts (not sure
            # how to timeout the iterator call).
            assert job_manager.get_job_status(
                job_id).status == JobStatus.PENDING

            # Signal job to start.
            ray.get(start_signal_actor.send.remote())

            await self._tail_and_assert_logs(
                job_id,
                job_manager,
                expected_log="Waiting...",
                num_iteration=5)

            # Signal the job to exit by writing to the file.
            with open(tmp_file, "w") as f:
                print("hello", file=f)

            async for lines in job_manager.tail_job_logs(job_id):
                assert all(
                    s == "Waiting..." for s in lines.strip().split("\n"))
                print(lines, end="")

            wait_for_condition(
                check_job_succeeded, job_manager=job_manager, job_id=job_id)

    @pytest.mark.asyncio
    async def test_failed_job(self, job_manager):
        """Test tailing logs for a job that unexpectedly exits."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = _run_hanging_command(job_manager, tmp_dir)

            await self._tail_and_assert_logs(
                job_id,
                job_manager,
                expected_log="Waiting...",
                num_iteration=5)

            # Kill the job unexpectedly.
            with open(pid_file, "r") as f:
                os.kill(int(f.read()), signal.SIGKILL)

            async for lines in job_manager.tail_job_logs(job_id):
                assert all(
                    s == "Waiting..." for s in lines.strip().split("\n"))
                print(lines, end="")

            wait_for_condition(
                check_job_failed, job_manager=job_manager, job_id=job_id)

    @pytest.mark.asyncio
    async def test_stopped_job(self, job_manager):
        """Test tailing logs for a job that unexpectedly exits."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, _, job_id = _run_hanging_command(job_manager, tmp_dir)

            await self._tail_and_assert_logs(
                job_id,
                job_manager,
                expected_log="Waiting...",
                num_iteration=5)

            # Stop the job via the API.
            job_manager.stop_job(job_id)

            async for lines in job_manager.tail_job_logs(job_id):
                assert all(
                    s == "Waiting..." for s in lines.strip().split("\n"))
                print(lines, end="")

            wait_for_condition(
                check_job_stopped, job_manager=job_manager, job_id=job_id)


def test_logs_streaming(job_manager):
    """Test that logs are streamed during the job, not just at the end."""

    stream_logs_script = """
import time
print('STREAMED')
while True:
    time.sleep(1)
"""

    stream_logs_cmd = f"python -c \"{stream_logs_script}\""

    job_id = job_manager.submit_job(entrypoint=stream_logs_cmd)
    wait_for_condition(lambda: "STREAMED" in job_manager.get_job_logs(job_id))

    job_manager.stop_job(job_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
