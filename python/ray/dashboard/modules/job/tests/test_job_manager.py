import asyncio
import os
import signal
import sys
import tempfile
import time
import urllib.request
from uuid import uuid4

import pytest

import ray
from ray._private.gcs_utils import GcsAioClient
from ray._private.ray_constants import (
    DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
    KV_HEAD_NODE_ID_KEY,
    KV_NAMESPACE_JOB,
    RAY_ADDRESS_ENVIRONMENT_VARIABLE,
)
from ray._private.test_utils import (
    SignalActor,
    async_wait_for_condition,
    async_wait_for_condition_async_predicate,
    wait_for_condition,
)
from ray.dashboard.consts import (
    RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR,
    RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR,
)
from ray.dashboard.modules.job.common import JOB_ID_METADATA_KEY, JOB_NAME_METADATA_KEY
from ray.dashboard.modules.job.job_manager import (
    JobLogStorageClient,
    JobManager,
    JobSupervisor,
    generate_job_id,
)
from ray.dashboard.modules.job.tests.conftest import (
    _driver_script_path,
    create_job_manager,
    create_ray_cluster,
)
from ray.job_submission import JobStatus
from ray.tests.conftest import call_ray_start  # noqa: F401
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: F401

import psutil


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head"""],
    indirect=True,
)
@pytest.mark.parametrize("resources_specified", [True, False])
async def test_get_scheduling_strategy(
    call_ray_start, monkeypatch, resources_specified, tmp_path  # noqa: F811
):
    monkeypatch.setenv(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "0")
    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )

    job_manager = JobManager(gcs_aio_client, tmp_path)

    # If no head node id is found, we should use "DEFAULT".
    await gcs_aio_client.internal_kv_del(
        KV_HEAD_NODE_ID_KEY,
        del_by_prefix=False,
        namespace=KV_NAMESPACE_JOB,
    )
    strategy = await job_manager._get_scheduling_strategy(resources_specified)
    assert strategy == "DEFAULT"

    # Add a head node id to the internal KV to simulate what is done in node_head.py.
    await gcs_aio_client.internal_kv_put(
        KV_HEAD_NODE_ID_KEY,
        "123456".encode(),
        True,
        namespace=KV_NAMESPACE_JOB,
    )
    strategy = await job_manager._get_scheduling_strategy(resources_specified)
    if resources_specified:
        assert strategy == "DEFAULT"
    else:
        expected_strategy = NodeAffinitySchedulingStrategy("123456", soft=False)
        assert expected_strategy.node_id == strategy.node_id
        assert expected_strategy.soft == strategy.soft

    # When the env var is set to 1, we should use DEFAULT.
    monkeypatch.setenv(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR, "1")
    strategy = await job_manager._get_scheduling_strategy(resources_specified)
    assert strategy == "DEFAULT"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["""ray start --head --resources={"TestResourceKey":123}"""],
    indirect=True,
)
async def test_submit_no_ray_address(call_ray_start, tmp_path):  # noqa: F811
    """Test that a job script with an unspecified Ray address works."""

    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client, tmp_path)

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head"],
    indirect=True,
)
async def test_get_all_job_info(call_ray_start, tmp_path):  # noqa: F811
    """Test that JobInfo is correctly populated in the GCS get_all_job_info API."""
    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client, tmp_path)

    # Submit a job.
    submission_id = await job_manager.submit_job(
        entrypoint="python -c 'import ray; ray.init()'",
    )

    # Wait for the job to be finished.
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=submission_id
    )

    found = False
    for job_table_entry in (await gcs_aio_client.get_all_job_info()).values():
        if job_table_entry.config.metadata.get(JOB_ID_METADATA_KEY) == submission_id:
            found = True
            # Check that the job info is populated correctly.
            job_info = job_table_entry.job_info
            assert job_info.status == "SUCCEEDED"
            assert job_info.entrypoint == "python -c 'import ray; ray.init()'"
            assert job_info.message == "Job finished successfully."
            assert job_info.start_time > 0
            assert job_info.end_time > job_info.start_time
            assert job_info.entrypoint_num_cpus == 0
            assert job_info.entrypoint_num_gpus == 0
            assert job_info.entrypoint_memory == 0
            assert job_info.driver_agent_http_address.startswith(
                "http://"
            ) and job_info.driver_agent_http_address.endswith(
                str(DEFAULT_DASHBOARD_AGENT_LISTEN_PORT)
            )
            assert job_info.driver_node_id != ""

    assert found


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head"],
    indirect=True,
)
async def test_get_all_job_info_with_is_running_tasks(call_ray_start):  # noqa: F811
    """Test the is_running_tasks bit in the GCS get_all_job_info API."""

    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )

    @ray.remote
    def sleep_forever():
        while True:
            time.sleep(1)

    object_ref = sleep_forever.remote()

    async def check_is_running_tasks(job_id, expected_is_running_tasks):
        """Return True if the driver indicated by job_id is currently running tasks."""
        found = False
        for job_table_entry in (await gcs_aio_client.get_all_job_info()).values():
            if job_table_entry.job_id.hex() == job_id:
                found = True
                return job_table_entry.is_running_tasks == expected_is_running_tasks
        assert found

    # Get the job id for this driver.
    job_id = ray.get_runtime_context().get_job_id()

    # Task should be running.
    assert await check_is_running_tasks(job_id, True)

    # Kill the task.
    ray.cancel(object_ref)

    # Task should not be running.
    await async_wait_for_condition_async_predicate(
        lambda: check_is_running_tasks(job_id, False), timeout=30
    )

    # Shutdown and start a new driver.
    ray.shutdown()
    ray.init(address=call_ray_start)

    old_job_id = job_id
    job_id = ray.get_runtime_context().get_job_id()
    assert old_job_id != job_id

    new_object_ref = sleep_forever.remote()

    # Tasks should still not be running for the old driver.
    assert await check_is_running_tasks(old_job_id, False)

    # Task should be running for the new driver.
    assert await check_is_running_tasks(job_id, True)

    # Start an actor that will run forever.
    @ray.remote
    class Actor:
        pass

    actor = Actor.remote()

    # Cancel the task.
    ray.cancel(new_object_ref)

    # The actor is still running, so is_running_tasks should be true.
    assert await check_is_running_tasks(job_id, True)

    # Kill the actor.
    ray.kill(actor)

    # The actor is no longer running, so is_running_tasks should be false.
    await async_wait_for_condition_async_predicate(
        lambda: check_is_running_tasks(job_id, False), timeout=30
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    [{"env": {"RAY_BACKEND_LOG_JSON": "1"}, "cmd": "ray start --head"}],
    indirect=True,
)
async def test_job_supervisor_log_json(call_ray_start, tmp_path):  # noqa: F811
    """Test JobSupervisor logs are structured JSON logs"""
    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client, tmp_path)
    job_id = await job_manager.submit_job(
        entrypoint="echo hello 1", submission_id="job_1"
    )
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )

    # Verify logs saved to file
    supervisor_log_path = os.path.join(
        ray._private.worker._global_node.get_logs_dir_path(),
        f"jobs/supervisor-{job_id}.log",
    )
    log_message = f'"message": "Job {job_id} entrypoint command exited with code 0"'
    with open(supervisor_log_path, "r") as f:
        logs = f.read()
        assert log_message in logs


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head"],
    indirect=True,
)
async def test_job_supervisor_logs_saved(
    call_ray_start, tmp_path, capsys  # noqa: F811
):
    """Test JobSupervisor logs are saved to jobs/supervisor-{submission_id}.log"""
    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client, tmp_path)
    job_id = await job_manager.submit_job(
        entrypoint="echo hello 1", submission_id="job_1"
    )
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )

    # Verify logs saved to file
    supervisor_log_path = os.path.join(
        ray._private.worker._global_node.get_logs_dir_path(),
        f"jobs/supervisor-{job_id}.log",
    )
    # By default, log is TEXT format
    log_message = f"-- Job {job_id} entrypoint command exited with code 0"
    with open(supervisor_log_path, "r") as f:
        logs = f.read()
        assert log_message in logs

    # Verify logs in stderr. Run in wait_for_condition to ensure
    # logs are flushed
    wait_for_condition(lambda: log_message in capsys.readouterr().err)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    ["ray start --head"],
    indirect=True,
)
async def test_runtime_env_setup_logged_to_job_driver_logs(
    call_ray_start, tmp_path  # noqa: F811
):
    """Test runtime env setup messages are logged to jobs driver log"""
    address_info = ray.init(address=call_ray_start)
    gcs_aio_client = GcsAioClient(
        address=address_info["gcs_address"], nums_reconnect_retry=0
    )
    job_manager = JobManager(gcs_aio_client, tmp_path)

    job_id = await job_manager.submit_job(
        entrypoint="echo hello 1", submission_id="test_runtime_env_setup_logs"
    )
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )

    # Verify logs saved to file
    job_driver_log_path = os.path.join(
        ray._private.worker._global_node.get_logs_dir_path(),
        f"job-driver-{job_id}.log",
    )
    start_message = "Runtime env is setting up."
    with open(job_driver_log_path, "r") as f:
        logs = f.read()
        assert start_message in logs


@pytest.fixture(scope="module")
def shared_ray_instance():
    # Remove ray address for test ray cluster in case we have
    # lingering RAY_ADDRESS="http://127.0.0.1:8265" from previous local job
    # submissions.
    old_ray_address = os.environ.pop(RAY_ADDRESS_ENVIRONMENT_VARIABLE, None)

    yield create_ray_cluster()

    if old_ray_address is not None:
        os.environ[RAY_ADDRESS_ENVIRONMENT_VARIABLE] = old_ray_address


@pytest.fixture
def job_manager(shared_ray_instance, tmp_path):
    yield create_job_manager(shared_ray_instance, tmp_path)


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
    if status == JobStatus.SUCCEEDED:
        assert data.driver_exit_code == 0
    else:
        assert data.driver_exit_code is None
    return status == JobStatus.SUCCEEDED


async def check_job_failed(job_manager, job_id):
    status = await job_manager.get_job_status(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.FAILED}
    return status == JobStatus.FAILED


async def check_job_stopped(job_manager, job_id):
    status = await job_manager.get_job_status(job_id)
    data = await job_manager.get_job_info(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING, JobStatus.STOPPED}
    assert data.driver_exit_code is None
    return status == JobStatus.STOPPED


async def check_job_running(job_manager, job_id):
    status = await job_manager.get_job_status(job_id)
    data = await job_manager.get_job_info(job_id)
    assert status in {JobStatus.PENDING, JobStatus.RUNNING}
    assert data.driver_exit_code is None
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
async def test_list_jobs_same_submission_id(job_manager: JobManager):
    # Multiple drivers started from the same job submission
    cmd = (
        "python -c 'import ray; ray.init(); ray.shutdown(); "
        "ray.init(); ray.shutdown();'"
    )
    submission_id = await job_manager.submit_job(entrypoint=cmd)

    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=submission_id
    )
    jobs_info = await job_manager.list_jobs()
    # We expect only the last driver to be returned in the case of
    # multiple jobs per job submission
    assert len(jobs_info) == 1


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
    with pytest.raises(ValueError):
        await job_manager.submit_job(
            entrypoint="echo hello", submission_id=submission_id
        )


@pytest.mark.asyncio
async def test_simultaneous_submit_job(job_manager):
    """Test that we can submit multiple jobs at once."""
    job_ids = await asyncio.gather(
        job_manager.submit_job(entrypoint="echo hello"),
        job_manager.submit_job(entrypoint="echo hello"),
        job_manager.submit_job(entrypoint="echo hello"),
    )

    for job_id in job_ids:
        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )


@pytest.mark.asyncio
async def test_simultaneous_with_same_id(job_manager):
    """Test that we can submit multiple jobs at once with the same id.

    The second job should raise a friendly error.
    """
    with pytest.raises(ValueError) as excinfo:
        await asyncio.gather(
            job_manager.submit_job(entrypoint="echo hello", submission_id="1"),
            job_manager.submit_job(entrypoint="echo hello", submission_id="1"),
        )
    assert "Job with submission_id 1 already exists" in str(excinfo.value)
    # Check that the (first) job can still succeed.
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id="1"
    )


@pytest.mark.asyncio
class TestShellScriptExecution:
    async def test_submit_basic_echo(self, job_manager):
        job_id = await job_manager.submit_job(entrypoint="echo hello")

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert "hello\n" in job_manager.get_job_logs(job_id)

    async def test_submit_stderr(self, job_manager):
        job_id = await job_manager.submit_job(entrypoint="echo error 1>&2")

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert "error\n" in job_manager.get_job_logs(job_id)

    async def test_submit_ls_grep(self, job_manager):
        grep_cmd = f"ls {os.path.dirname(__file__)} | grep test_job_manager.py"
        job_id = await job_manager.submit_job(entrypoint=grep_cmd)

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )
        assert "test_job_manager.py\n" in job_manager.get_job_logs(job_id)

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
        assert "Executing main() from script.py !!\n" in job_manager.get_job_logs(
            job_id
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
            assert "Executing main() from script.py !!\n" in job_manager.get_job_logs(
                job_id
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
        assert "233\n" in job_manager.get_job_logs(job_id)

    async def test_niceness(self, job_manager):
        job_id = await job_manager.submit_job(
            entrypoint=f"python {_driver_script_path('check_niceness.py')}",
        )

        await async_wait_for_condition_async_predicate(
            check_job_succeeded, job_manager=job_manager, job_id=job_id
        )

        logs = job_manager.get_job_logs(job_id)
        assert "driver 0" in logs
        assert "worker 15" in logs

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
        assert "'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_1_VAR'" in logs

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
        assert "'TEST_SUBPROCESS_JOB_CONFIG_ENV_VAR': 'JOB_2_VAR'" in logs

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
        assert data.driver_exit_code is None

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
        assert data.driver_exit_code is None
        log_path = JobLogStorageClient().get_log_file_path(job_id=job_id)
        with open(log_path, "r") as f:
            job_logs = f.read()
        assert "Traceback (most recent call last):" in job_logs

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
    @pytest.mark.parametrize(
        "resource_kwarg",
        [
            {},
            {"entrypoint_num_cpus": 1},
            {"entrypoint_num_gpus": 1},
            {"entrypoint_memory": 4},
            {"entrypoint_resources": {"Custom": 1}},
        ],
    )
    async def test_cuda_visible_devices(self, job_manager, resource_kwarg, env_vars):
        """Check CUDA_VISIBLE_DEVICES behavior introduced in #24546.

        Should not be set in the driver, but should be set in tasks.
        We test a variety of `env_vars` parameters due to custom parsing logic
        that caused https://github.com/ray-project/ray/issues/25086.

        If the user specifies a resource, we should not use the CUDA_VISIBLE_DEVICES
        logic. Instead, the behavior should match that of the user specifying
        resources for any other actor. So CUDA_VISIBLE_DEVICES should be set in the
        driver and tasks.
        """
        run_cmd = f"python {_driver_script_path('check_cuda_devices.py')}"
        runtime_env = {"env_vars": env_vars}
        if resource_kwarg:
            run_cmd = "RAY_TEST_RESOURCES_SPECIFIED=1 " + run_cmd
        job_id = await job_manager.submit_job(
            entrypoint=run_cmd,
            runtime_env=runtime_env,
            **resource_kwarg,
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
            data = await job_manager.get_job_info(job_id)
            assert data.driver_exit_code is None

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
            data = await job_manager.get_job_info(job_id)
            assert data.driver_exit_code is None

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
            assert all(
                s == expected_log or "Runtime env" in s
                for s in lines.strip().split("\n")
            )
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
                assert all(
                    s == "Waiting..." or "Runtime env" in s
                    for s in lines.strip().split("\n")
                )
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
                assert all(
                    s == "Waiting..." or "Runtime env" in s
                    for s in lines.strip().split("\n")
                )
                print(lines, end="")

            await async_wait_for_condition_async_predicate(
                check_job_failed, job_manager=job_manager, job_id=job_id
            )
            # check if the driver is killed
            data = await job_manager.get_job_info(job_id)
            assert data.driver_exit_code == -signal.SIGKILL

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
                assert all(
                    s == "Waiting..." or s == "Terminated" or "Runtime env" in s
                    for s in lines.strip().split("\n")
                )
                print(lines, end="")

            await async_wait_for_condition_async_predicate(
                check_job_stopped, job_manager=job_manager, job_id=job_id
            )


@pytest.mark.asyncio
async def test_stop_job_gracefully(job_manager):
    """
    Stop job should send SIGTERM to child process (before trying to kill).
    """
    entrypoint = """python -c \"
import sys
import signal
import time
def handler(*args):
    print('SIGTERM signal handled!');
    sys.exit()
signal.signal(signal.SIGTERM, handler)

while True:
    print('Waiting...')
    time.sleep(1)\"
"""
    job_id = await job_manager.submit_job(entrypoint=entrypoint)

    await async_wait_for_condition(
        lambda: "Waiting..." in job_manager.get_job_logs(job_id)
    )

    assert job_manager.stop_job(job_id) is True

    await async_wait_for_condition_async_predicate(
        check_job_stopped, job_manager=job_manager, job_id=job_id
    )

    assert "SIGTERM signal handled!" in job_manager.get_job_logs(job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "use_env_var,stop_timeout",
    [(True, 10), (False, JobSupervisor.DEFAULT_RAY_JOB_STOP_WAIT_TIME_S)],
)
async def test_stop_job_timeout(job_manager, use_env_var, stop_timeout):
    """
    Stop job should send SIGTERM first, then if timeout occurs, send SIGKILL.
    """
    entrypoint = """python -c \"
import sys
import signal
import time
def handler(*args):
    print('SIGTERM signal handled!');
signal.signal(signal.SIGTERM, handler)

while True:
    print('Waiting...')
    time.sleep(1)\"
"""
    if use_env_var:
        job_id = await job_manager.submit_job(
            entrypoint=entrypoint,
            runtime_env={"env_vars": {"RAY_JOB_STOP_WAIT_TIME_S": str(stop_timeout)}},
        )
    else:
        job_id = await job_manager.submit_job(entrypoint=entrypoint)

    await async_wait_for_condition(
        lambda: "Waiting..." in job_manager.get_job_logs(job_id)
    )

    assert job_manager.stop_job(job_id) is True

    with pytest.raises(RuntimeError):
        await async_wait_for_condition_async_predicate(
            check_job_stopped,
            job_manager=job_manager,
            job_id=job_id,
            timeout=stop_timeout - 1,
        )

    await async_wait_for_condition(
        lambda: "SIGTERM signal handled!" in job_manager.get_job_logs(job_id)
    )

    await async_wait_for_condition_async_predicate(
        check_job_stopped,
        job_manager=job_manager,
        job_id=job_id,
        timeout=10,
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


@pytest.mark.asyncio
async def test_failed_job_logs_max_char(job_manager):
    """Test failed jobs does not print out too many logs"""

    # Prints 21000 characters
    print_large_logs_cmd = (
        "python -c \"print('1234567890'* 2100); raise RuntimeError()\""
    )

    job_id = await job_manager.submit_job(
        entrypoint=print_large_logs_cmd,
    )

    await async_wait_for_condition_async_predicate(
        check_job_failed, job_manager=job_manager, job_id=job_id
    )

    # Verify the status message length
    job_info = await job_manager.get_job_info(job_id)
    assert job_info
    assert len(job_info.message) == 20000 + len(
        "Job entrypoint command failed with exit code 1,"
        " last available logs (truncated to 20,000 chars):\n"
    )
    assert job_info.driver_exit_code == 1


@pytest.mark.asyncio
async def test_simultaneous_drivers(job_manager):
    """Test that multiple drivers can be used to submit jobs at the same time."""

    cmd = "python -c 'import ray; ray.init(); ray.shutdown();'"
    job_id = await job_manager.submit_job(
        entrypoint=f"{cmd} & {cmd} && wait && echo 'done'"
    )

    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )
    assert "done" in job_manager.get_job_logs(job_id)


@pytest.mark.asyncio
async def test_monitor_job_pending(job_manager):
    """Test that monitor_job does not error when the job is PENDING."""

    # Create a signal actor to keep the job pending.
    start_signal_actor = SignalActor.remote()

    # Submit a job.
    job_id = await job_manager.submit_job(
        entrypoint="echo 'hello world'",
        _start_signal_actor=start_signal_actor,
    )

    # Trigger _recover_running_jobs while the job is still pending. This
    # will pick up the new pending job.
    await job_manager._recover_running_jobs()

    # Trigger the job to start.
    ray.get(start_signal_actor.send.remote())

    # Wait for the job to finish.
    await async_wait_for_condition_async_predicate(
        check_job_succeeded, job_manager=job_manager, job_id=job_id
    )


@pytest.mark.asyncio
async def test_job_pending_timeout(job_manager, monkeypatch):
    """Test the timeout for pending jobs."""

    monkeypatch.setenv(RAY_JOB_START_TIMEOUT_SECONDS_ENV_VAR, "0.1")

    # Create a signal actor to keep the job pending.
    start_signal_actor = SignalActor.remote()

    # Submit a job.
    job_id = await job_manager.submit_job(
        entrypoint="echo 'hello world'",
        _start_signal_actor=start_signal_actor,
    )

    # Trigger _recover_running_jobs while the job is still pending. This
    # will pick up the new pending job.
    await job_manager._recover_running_jobs()

    # Wait for the job to timeout.
    await async_wait_for_condition_async_predicate(
        check_job_failed, job_manager=job_manager, job_id=job_id
    )

    # Check that the job timed out.
    job_info = await job_manager.get_job_info(job_id)
    assert job_info.status == JobStatus.FAILED
    assert "Job supervisor actor failed to start within" in job_info.message
    assert job_info.driver_exit_code is None


@pytest.mark.asyncio
async def test_failed_driver_exit_code(job_manager):
    """Test driver exit code from finished task that failed"""
    EXIT_CODE = 10

    exit_code_script = f"""
import sys
sys.exit({EXIT_CODE})
"""

    exit_code_cmd = f'python -c "{exit_code_script}"'

    job_id = await job_manager.submit_job(entrypoint=exit_code_cmd)
    # Wait for the job to timeout.
    await async_wait_for_condition_async_predicate(
        check_job_failed, job_manager=job_manager, job_id=job_id
    )

    # Check that the job failed
    job_info = await job_manager.get_job_info(job_id)
    assert job_info.status == JobStatus.FAILED
    assert job_info.driver_exit_code == EXIT_CODE


@pytest.mark.asyncio
async def test_actor_creation_error_not_overwritten(shared_ray_instance, tmp_path):
    """Regression test for: https://github.com/ray-project/ray/issues/40062.

    Previously there existed a race condition that could overwrite error messages from
    actor creation (such as an invalid `runtime_env`). This would happen
    non-deterministically after an initial correct error message was set, so this test
    runs many iterations.

    Without the fix in place, this test failed consistently.
    """
    for _ in range(10):
        # Race condition existed when a job was submitted just after constructing the
        # `JobManager`, so make a new one in each test iteration.
        job_manager = create_job_manager(shared_ray_instance, tmp_path)
        job_id = await job_manager.submit_job(
            entrypoint="doesn't matter", runtime_env={"working_dir": "path_not_exist"}
        )

        # `await` many times to yield the `asyncio` loop and verify that the error
        # message does not get overwritten.
        for _ in range(100):
            data = await job_manager.get_job_info(job_id)
            assert data.status == JobStatus.FAILED
            assert "path_not_exist is not a valid URI" in data.message
            assert data.driver_exit_code is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
