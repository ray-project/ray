import asyncio
import os
import signal
import sys
import tempfile
import urllib.request
from uuid import uuid4

import psutil
import pytest

import ray
from ray._private.gcs_utils import GcsAioClient
from ray._private.ray_constants import (
    RAY_ADDRESS_ENVIRONMENT_VARIABLE,
    KV_NAMESPACE_JOB,
)
from ray._private.test_utils import (
    SignalActor,
    async_wait_for_condition,
    async_wait_for_condition_async_predicate,
)
from ray.dashboard.modules.job.common import JOB_ID_METADATA_KEY, JOB_NAME_METADATA_KEY
from ray.dashboard.modules.job.job_manager import JobManager, generate_job_id
from ray.dashboard.consts import RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR
from ray.job_submission import JobStatus
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: F401
from ray.tests.conftest import call_ray_start  # noqa: F401

TEST_NAMESPACE = "jobs_test_namespace"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
async def test_get_scheduling_strategy(call_ray_start, monkeypatch):  # noqa: F811
    monkeypatch.setenv(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "0")
    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )

    job_manager = JobManager(gcs_aio_client)

    # If no head node id is found, we should use "DEFAULT".
    await gcs_aio_client.internal_kv_del(
        "head_node_id".encode(), del_by_prefix=False, namespace=KV_NAMESPACE_JOB
    )
    strategy = await job_manager._get_scheduling_strategy()
    assert strategy == "DEFAULT"

    # Add a head node id to the internal KV to simulate what is done in node_head.py.
    await gcs_aio_client.internal_kv_put(
        "head_node_id".encode(), "123456".encode(), True, namespace=KV_NAMESPACE_JOB
    )
    strategy = await job_manager._get_scheduling_strategy()
    expected_strategy = NodeAffinitySchedulingStrategy("123456", soft=False)
    assert expected_strategy.node_id == strategy.node_id
    assert expected_strategy.soft == strategy.soft

    # When the env var is set to 1, we should use DEFAULT.
    monkeypatch.setenv(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "1")
    strategy = await job_manager._get_scheduling_strategy()
    assert strategy == "DEFAULT"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head --resources={"TestResourceKey":123}"""],
    indirect=True,
)
async def test_submit_no_ray_address(call_ray_start):  # noqa: F811
    """Test that a job script with an unspecified Ray address works."""

    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client)

    init_ray_no_address_script = """
import ray

ray.init()

# Check that we connected to the running test Ray cluster and didn't create a new one.
print(ray.cluster_resources())
assert ray.cluster_resources().get('TestResourceKey') == 123

"""

    # The job script should work even if RAY_ADDRESS is not set on the cluster.
    os.environ.pop(RAY_ADDRESS_ENVIRONMENT_VARIABLE, None)

    job_id = await job_manager.submit_job(
        entrypoint=f"""python -c "{init_ray_no_address_script}" """
    )

    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )


@pytest.fixture(scope="module")
def shared_ray_instance():
    # Remove ray address for test ray cluster in case we have
    # lingering RAY_ADDRESS="http://127.0.0.1:8265" from previous local job
    # submissions.
    old_ray_address = os.environ.pop(RAY_ADDRESS_ENVIRONMENT_VARIABLE, None)

    yield ray.init(num_cpus=16, namespace=TEST_NAMESPACE, log_to_driver=True)

    if old_ray_address is not None:
        os.environ[RAY_ADDRESS_ENVIRONMENT_VARIABLE] = old_ray_address


@pytest.mark.asyncio
@pytest.fixture
async def job_manager(shared_ray_instance):
    address_info = shared_ray_instance
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    yield JobManager(gcs_aio_client)


def _driver_script_path(file_name: str) -> str:
    return os.path.join(
        os.path.dirname(__file__), "subprocess_driver_scripts", file_name
    )


async def _run_hanging_command(job_manager, tmp_dir, start_signal_actor=None):
    tmp_file = os.path.join(tmp_dir, "hello")
    pid_file = os.path.join(tmp_dir, "pid")

    # Write subprocess pid to pid_file and block until tmp_file is present.
    wait_for_file_cmd = (
        f"echo $$ > {pid_file} && "
        f"until [ -f {tmp_file} ]; "
        "do echo 'Waiting...' && sleep 1; "
        "done"
    )
    job_id = await job_manager.submit_job(
        entrypoint=wait_for_file_cmd, _start_signal_actor=start_signal_actor
    )

    status = await job_manager.get_job_status(job_id)
    if start_signal_actor:
        for _ in range(10):
            assert status == JobStatus.PENDING
            logs = job_manager.get_job_logs(job_id)
            assert logs == ""
            await asyncio.sleep(0.01)
    else:
        await async_wait_for_condition_async_predicate(
            check_job_running, job_manager=job_manager, job_id=job_id
        )
        await async_wait_for_condition(
            lambda: "Waiting..." in job_manager.get_job_logs(job_id)
        )

    return pid_file, tmp_file, job_id


async def check_job_succeeded(job_manager, job_id):
    data = await job_manager.get_job_info(job_id)
    status = data.status
    if status == JobStatus.FAILED:
        raise RuntimeError(f"Job failed! {data.message}")
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCEEDED}
    return status == JobStatus.SUCCEEDED


async def check_job_failed(job_manager, job_id):
    status = await job_manager.get_job_status(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.FAILED}
    return status == JobStatus.FAILED


async def check_job_stopped(job_manager, job_id):
    status = await job_manager.get_job_status(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.STOPPED}
    return status == JobStatus.STOPPED


async def check_job_running(job_manager, job_id):
    status = await job_manager.get_job_status(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING}
    return status == JobStatus.RUNNING


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


# NOTE(architkulkarni): This test must be run first in order for the job
# submission history of the shared Ray runtime to be empty.
@pytest.mark.asyncio
async def test_list_jobs_empty(job_manager: JobManager):
    assert await job_manager.list_jobs() == dict()


@pytest.mark.asyncio
async def test_list_jobs(job_manager: JobManager):
    await job_manager.submit_job(entrypoint="echo hi", submission_id="1")

    runtime_env = {"env_vars": {"TEST": "123"}}
    metadata = {"foo": "bar"}
    await job_manager.submit_job(
        entrypoint="echo hello",
        submission_id="2",
        runtime_env=runtime_env,
        metadata=metadata,
    )
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id="1"
    )
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id="2"
    )
    jobs_info = await job_manager.list_jobs()
    assert "1" in jobs_info
    assert jobs_info["1"].status == JobStatus.SUCCEEDED

    assert "2" in jobs_info
    assert jobs_info["2"].status == JobStatus.SUCCEEDED
    assert jobs_info["2"].message is not None
    assert jobs_info["2"].end_time >= jobs_info["2"].start_time
    assert jobs_info["2"].runtime_env == runtime_env
    assert jobs_info["2"].metadata == metadata


@pytest.mark.asyncio
async def test_pass_job_id(job_manager):
    submission_id = "my_custom_id"

    returned_id = await job_manager.submit_job(
        entrypoint="echo hello", submission_id=submission_id
    )
    assert returned_id == submission_id

    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=submission_id
    )

    # Check that the same job_id is rejected.
    with pytest.raises(RuntimeError):
        await job_manager.submit_job(
            entrypoint="echo hello", submission_id=submission_id
        )


@pytest.mark.asyncio
class TestShellScriptExecution:
    async def test_submit_basic_echo(self, job_manager):
        job_id = await job_manager.submit_job(entrypoint="echo hello")

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert job_manager.get_job_logs(job_id) == "hello\n"

    async def test_submit_stderr(self, job_manager):
        job_id = await job_manager.submit_job(entrypoint="echo error 1>&2")

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert job_manager.get_job_logs(job_id) == "error\n"

    async def test_submit_ls_grep(self, job_manager):
        grep_cmd = f"ls {os.path.dirname(__file__)} | grep test_job_manager.py"
        job_id = await job_manager.submit_job(entrypoint=grep_cmd)

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert job_manager.get_job_logs(job_id) == "test_job_manager.py\n"

    async def test_subprocess_exception(self, job_manager):
        """
        Run a python script with exception, ensure:
        1) Job status is marked as failed
        2) Job manager can surface exception message back to logs api
        3) Job no hanging job supervisor actor
        4) Empty logs
        """
        run_cmd = f"python {_driver_script_path('script_with_exception.py')}"
        job_id = await job_manager.submit_job(entrypoint=run_cmd)

        async def cleaned_up():
            data = await job_manager.get_job_info(job_id)
            if data.status != JobStatus.FAILED:
                return False
            if "Exception: Script failed with exception !" not in data.message:
                return False

            return job_manager._get_actor_for_job(job_id) is None

        await async_wait_for_condition_async_predicate(cleaned_up)

    async def test_submit_with_s3_runtime_env(self, job_manager):
        job_id = await job_manager.submit_job(
            entrypoint="python script.py",
            runtime_env={"working_dir": "s3://runtime-env-test/script_runtime_env.zip"},
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert (
            job_manager.get_job_logs(job_id) == "Executing main() from script.py !!\n"
        )

    async def test_submit_with_file_runtime_env(self, job_manager):
        with tempfile.NamedTemporaryFile(suffix=".zip") as f:
            filename, _ = urllib.request.urlretrieve(
                "https://runtime-env-test.s3.amazonaws.com/script_runtime_env.zip",
                filename=f.name,
            )
            job_id = await job_manager.submit_job(
                entrypoint="python script.py",
                runtime_env={"working_dir": "file://" + filename},
            )
            await async_wait_for_condition_async_predicate(
                check_job_succeeded, job_manager=job_manager, job_id=job_id
            )
            assert (
                job_manager.get_job_logs(job_id)
                == "Executing main() from script.py !!\n"
            )


@pytest.mark.asyncio
class TestRuntimeEnv:
    async def test_pass_env_var(self, job_manager):
        """Test we can pass env vars in the subprocess that executes job's
        driver script.
        """
        job_id = await job_manager.submit_job(
            entrypoint="echo $TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR",
            runtime_env={"env_vars": {"TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "233"}},
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert job_manager.get_job_logs(job_id) == "233\n"

    async def test_multiple_runtime_envs(self, job_manager):
        # Test that you can run two jobs in different envs without conflict.
        job_id_1 = await job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('print_runtime_env.py')}",
            runtime_env={
                "env_vars": {"TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_1_VAR"}
            },
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id_1
        )
        logs = job_manager.get_job_logs(job_id_1)
        assert (
            "{'env_vars': {'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_1_VAR'}}" in logs
        )  # noqa: E501

        job_id_2 = await job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('print_runtime_env.py')}",
            runtime_env={
                "env_vars": {"TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_2_VAR"}
            },
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id_2
        )
        logs = job_manager.get_job_logs(job_id_2)
        assert (
            "{'env_vars': {'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_2_VAR'}}" in logs
        )  # noqa: E501

    async def test_env_var_and_driver_job_config_warning(self, job_manager):
        """Ensure we got error message from worker.py and job logs
        if user provided runtime_env in both driver script and submit()
        """
        job_id = await job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('override_env_var.py')}",
            runtime_env={
                "env_vars": {"TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR": "JOB_1_VAR"}
            },
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        logs = job_manager.get_job_logs(job_id)
        token = (
            "Both RAY_JOB_CONFIG_JSON_ENV_VAR and ray.init(runtime_env) are provided"
        )
        assert token in logs, logs
        assert "JOB_1_VAR" in logs

    async def test_failed_runtime_env_validation(self, job_manager):
        """Ensure job status is correctly set as failed if job has an invalid
        runtime_env.
        """
        run_cmd = f"python {_driver_script_path('override_env_var.py')}"
        job_id = await job_manager.submit_job(
            entrypoint=run_cmd, runtime_env={"working_dir": "path_not_exist"}
        )

        data = await job_manager.get_job_info(job_id)
        assert data.status == JobStatus.FAILED
        assert "path_not_exist is not a valid URI" in data.message

    async def test_failed_runtime_env_setup(self, job_manager):
        """Ensure job status is correctly set as failed if job has a valid
        runtime_env that fails to be set up.
        """
        run_cmd = f"python {_driver_script_path('override_env_var.py')}"
        job_id = await job_manager.submit_job(
            entrypoint=run_cmd, runtime_env={"working_dir": "s3://does_not_exist.zip"}
        )

        await async_wait_for_condition_async_predicate(
            check_job_failed, job_manager=job_manager, job_id=job_id
        )

        data = await job_manager.get_job_info(job_id)
        assert "runtime_env setup failed" in data.message

    async def test_pass_metadata(self, job_manager):
        def dict_to_str(d):
            return str(dict(sorted(d.items())))

        print_metadata_cmd = (
            'python -c"'
            "import ray;"
            "ray.init();"
            "job_config=ray._private.worker.global_worker.core_worker.get_job_config();"
            "print(dict(sorted(job_config.metadata.items())))"
            '"'
        )

        # Check that we default to only the job ID and job name.
        job_id = await job_manager.submit_job(entrypoint=print_metadata_cmd)

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert dict_to_str(
            {JOB_NAME_METADATA_KEY: job_id, JOB_ID_METADATA_KEY: job_id}
        ) in job_manager.get_job_logs(job_id)

        # Check that we can pass custom metadata.
        job_id = await job_manager.submit_job(
            entrypoint=print_metadata_cmd, metadata={"key1": "val1", "key2": "val2"}
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert dict_to_str(
            {
                JOB_NAME_METADATA_KEY: job_id,
                JOB_ID_METADATA_KEY: job_id,
                "key1": "val1",
                "key2": "val2",
            }
        ) in job_manager.get_job_logs(job_id)

        # Check that we can override job name.
        job_id = await job_manager.submit_job(
            entrypoint=print_metadata_cmd,
            metadata={JOB_NAME_METADATA_KEY: "custom_name"},
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert dict_to_str(
            {JOB_NAME_METADATA_KEY: "custom_name", JOB_ID_METADATA_KEY: job_id}
        ) in job_manager.get_job_logs(job_id)

    @pytest.mark.parametrize(
        "env_vars",
        [None, {}, {"hello": "world"}],
    )
    async def test_cuda_visible_devices(self, job_manager, env_vars):
        """Check CUDA_VISIBLE_DEVICES behavior.

        Should not be set in the driver, but should be set in tasks.

        We test a variety of `env_vars` parameters due to custom parsing logic
        that caused https://github.com/ray-project/ray/issues/25086.
        """
        run_cmd = f"python {_driver_script_path('check_cuda_devices.py')}"
        runtime_env = {"env_vars": env_vars}
        job_id = await job_manager.submit_job(
            entrypoint=run_cmd, runtime_env=runtime_env
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )


@pytest.mark.asyncio
class TestAsyncAPI:
    async def test_status_and_logs_while_blocking(self, job_manager):
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, tmp_file, job_id = await _run_hanging_command(
                job_manager, tmp_dir
            )
            with open(pid_file, "r") as file:
                pid = int(file.read())
                assert psutil.pid_exists(pid), "driver subprocess should be running"

            # Signal the job to exit by writing to the file.
            with open(tmp_file, "w") as f:
                print("hello", file=f)

            await async_wait_for_condition_async_predicate(
                check_job_succeeded, job_manager=job_manager, job_id=job_id
            )
            # Ensure driver subprocess gets cleaned up after job reached
            # termination state
            await async_wait_for_condition(check_subprocess_cleaned, pid=pid)

    async def test_stop_job(self, job_manager):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, _, job_id = await _run_hanging_command(job_manager, tmp_dir)

            assert job_manager.stop_job(job_id) is True
            await async_wait_for_condition_async_predicate(
                check_job_stopped, job_manager=job_manager, job_id=job_id
            )
            # Assert re-stopping a stopped job also returns False
            await async_wait_for_condition(
                lambda: job_manager.stop_job(job_id) is False
            )
            # Assert stopping non-existent job returns False
            assert job_manager.stop_job(str(uuid4())) is False

    async def test_kill_job_actor_in_before_driver_finish(self, job_manager):
        """
        Test submitting a long running / blocker driver script, and kill
        the job supervisor actor before script returns and ensure

        1) Job status is correctly marked as failed
        2) No hanging subprocess from failed job
        """

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = await _run_hanging_command(job_manager, tmp_dir)
            with open(pid_file, "r") as file:
                pid = int(file.read())
                assert psutil.pid_exists(pid), "driver subprocess should be running"

            actor = job_manager._get_actor_for_job(job_id)
            ray.kill(actor, no_restart=True)
            await async_wait_for_condition_async_predicate(
                check_job_failed, job_manager=job_manager, job_id=job_id
            )

            # Ensure driver subprocess gets cleaned up after job reached
            # termination state
            await async_wait_for_condition(check_subprocess_cleaned, pid=pid)

    async def test_stop_job_in_pending(self, job_manager):
        """
        Kick off a job that is in PENDING state, stop the job and ensure

        1) Job can correctly be stop immediately with correct JobStatus
        2) No dangling subprocess left.
        """
        start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = await _run_hanging_command(
                job_manager, tmp_dir, start_signal_actor=start_signal_actor
            )
            assert not os.path.exists(pid_file), (
                "driver subprocess should NOT be running while job is " "still PENDING."
            )

            assert job_manager.stop_job(job_id) is True
            # Send run signal to unblock run function
            ray.get(start_signal_actor.send.remote())
            await async_wait_for_condition_async_predicate(
                check_job_stopped, job_manager=job_manager, job_id=job_id
            )

    async def test_kill_job_actor_in_pending(self, job_manager):
        """
        Kick off a job that is in PENDING state, kill the job actor and ensure

        1) Job can correctly be stop immediately with correct JobStatus
        2) No dangling subprocess left.
        """
        start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = await _run_hanging_command(
                job_manager, tmp_dir, start_signal_actor=start_signal_actor
            )

            assert not os.path.exists(pid_file), (
                "driver subprocess should NOT be running while job is " "still PENDING."
            )

            actor = job_manager._get_actor_for_job(job_id)
            ray.kill(actor, no_restart=True)
            await async_wait_for_condition_async_predicate(
                check_job_failed, job_manager=job_manager, job_id=job_id
            )

    async def test_stop_job_subprocess_cleanup_upon_stop(self, job_manager):
        """
        Ensure driver scripts' subprocess is cleaned up properly when we
        stopped a running job.

        SIGTERM first, SIGKILL after 3 seconds.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = await _run_hanging_command(job_manager, tmp_dir)
            with open(pid_file, "r") as file:
                pid = int(file.read())
                assert psutil.pid_exists(pid), "driver subprocess should be running"

            assert job_manager.stop_job(job_id) is True
            await async_wait_for_condition_async_predicate(
                check_job_stopped, job_manager=job_manager, job_id=job_id
            )

            # Ensure driver subprocess gets cleaned up after job reached
            # termination state
            await async_wait_for_condition(check_subprocess_cleaned, pid=pid)


@pytest.mark.asyncio
class TestTailLogs:
    async def _tail_and_assert_logs(
        self, job_id, job_manager, expected_log="", num_iteration=5
    ):
        i = 0
        async for lines in job_manager.tail_job_logs(job_id):
            assert all(s == expected_log for s in lines.strip().split("\n"))
            print(lines, end="")
            if i == num_iteration:
                break
            i += 1

    async def test_unknown_job(self, job_manager):
        with pytest.raises(RuntimeError, match="Job 'unknown' does not exist."):
            async for _ in job_manager.tail_job_logs("unknown"):
                pass

    async def test_successful_job(self, job_manager):
        """Test tailing logs for a PENDING -> RUNNING -> SUCCESSFUL job."""
        start_signal_actor = SignalActor.remote()

        with tempfile.TemporaryDirectory() as tmp_dir:
            _, tmp_file, job_id = await _run_hanging_command(
                job_manager, tmp_dir, start_signal_actor=start_signal_actor
            )

            # TODO(edoakes): check we get no logs before actor starts (not sure
            # how to timeout the iterator call).
            job_status = await job_manager.get_job_status(job_id)
            assert job_status == JobStatus.PENDING

            # Signal job to start.
            ray.get(start_signal_actor.send.remote())

            await self._tail_and_assert_logs(
                job_id, job_manager, expected_log="Waiting...", num_iteration=5
            )

            # Signal the job to exit by writing to the file.
            with open(tmp_file, "w") as f:
                print("hello", file=f)

            async for lines in job_manager.tail_job_logs(job_id):
                assert all(s == "Waiting..." for s in lines.strip().split("\n"))
                print(lines, end="")

            await async_wait_for_condition_async_predicate(
                check_job_succeeded, job_manager=job_manager, job_id=job_id
            )

    async def test_failed_job(self, job_manager):
        """Test tailing logs for a job that unexpectedly exits."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            pid_file, _, job_id = await _run_hanging_command(job_manager, tmp_dir)

            await self._tail_and_assert_logs(
                job_id, job_manager, expected_log="Waiting...", num_iteration=5
            )

            # Kill the job unexpectedly.
            with open(pid_file, "r") as f:
                os.kill(int(f.read()), signal.SIGKILL)

            async for lines in job_manager.tail_job_logs(job_id):
                assert all(s == "Waiting..." for s in lines.strip().split("\n"))
                print(lines, end="")

            await async_wait_for_condition_async_predicate(
                check_job_failed, job_manager=job_manager, job_id=job_id
            )

    async def test_stopped_job(self, job_manager):
        """Test tailing logs for a job that unexpectedly exits."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            _, _, job_id = await _run_hanging_command(job_manager, tmp_dir)

            await self._tail_and_assert_logs(
                job_id, job_manager, expected_log="Waiting...", num_iteration=5
            )

            # Stop the job via the API.
            job_manager.stop_job(job_id)

            async for lines in job_manager.tail_job_logs(job_id):
                assert all(s == "Waiting..." for s in lines.strip().split("\n"))
                print(lines, end="")

            await async_wait_for_condition_async_predicate(
                check_job_stopped, job_manager=job_manager, job_id=job_id
            )


@pytest.mark.asyncio
async def test_logs_streaming(job_manager):
    """Test that logs are streamed during the job, not just at the end."""

    stream_logs_script = """
import time
print('STREAMED')
while True:
    time.sleep(1)
"""

    stream_logs_cmd = f'python -c "{stream_logs_script}"'

    job_id = await job_manager.submit_job(entrypoint=stream_logs_cmd)
    await async_wait_for_condition(
        lambda: "STREAMED" in job_manager.get_job_logs(job_id)
    )

    job_manager.stop_job(job_id)


@pytest.mark.asyncio
async def test_bootstrap_address(job_manager, monkeypatch):
    """Ensure we always use bootstrap address in job manager even though ray
    cluster might be started with http://ip:{dashboard_port} from previous
    runs.
    """
    ip = ray._private.ray_constants.DEFAULT_DASHBOARD_IP
    port = ray._private.ray_constants.DEFAULT_DASHBOARD_PORT

    monkeypatch.setenv("RAY_ADDRESS", f"http://{ip}:{port}")
    print_ray_address_cmd = (
        'python -c"' "import os;" "import ray;" "ray.init();" "print('SUCCESS!');" '"'
    )

    job_id = await job_manager.submit_job(entrypoint=print_ray_address_cmd)

    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )
    assert "SUCCESS!" in job_manager.get_job_logs(job_id)


@pytest.mark.asyncio
async def test_job_runs_with_no_resources_available(job_manager):
    script_path = _driver_script_path("consume_one_cpu.py")

    hang_signal_actor = SignalActor.remote()

    @ray.remote(num_cpus=ray.available_resources()["CPU"])
    def consume_all_cpus():
        ray.get(hang_signal_actor.wait.remote())

    # Start a hanging task that consumes all CPUs.
    hanging_ref = consume_all_cpus.remote()

    try:
        # Check that the job starts up properly even with no CPUs available.
        # The job won't exit until it has a CPU available because it waits for
        # a task.
        job_id = await job_manager.submit_job(entrypoint=f"python {script_path}")
        await async_wait_for_condition_async_predicate(
            check_job_running, job_manager=job_manager, job_id=job_id
        )
        await async_wait_for_condition(
            lambda: "Hanging..." in job_manager.get_job_logs(job_id)
        )

        # Signal the hanging task to exit and release its CPUs.
        ray.get(hang_signal_actor.send.remote())

        # Check the job succeeds now that resources are available.
        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        await async_wait_for_condition(
            lambda: "Success!" in job_manager.get_job_logs(job_id)
        )
    finally:
        # Just in case the test fails.
        ray.cancel(hanging_ref)


async def test_failed_job_logs_max_char(job_manager):
    """Test failed jobs does not print out too many logs"""

    # Prints 21000 characters
    print_large_logs_cmd = (
        "python -c \"print('1234567890'* 2100); raise RuntimeError()\""
    )

    job_id = await job_manager.submit_job(
        entrypoint=print_large_logs_cmd,
    )

    await async_wait_for_condition(
        check_job_failed, job_manager=job_manager, job_id=job_id
    )

    # Verify the status message length
    job_info = await job_manager.get_job_info(job_id)
    assert job_info
    assert len(job_info.message) == 20000 + len(
        "Job failed due to an application error, " "last available logs:\n"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
