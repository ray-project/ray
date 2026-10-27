import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
import yaml

import ray
import ray.dashboard.optional_utils as optional_utils
from ray._common.network_utils import build_address
from ray._common.test_utils import (
    async_wait_for_condition,
    run_string_as_driver,
    wait_for_condition,
)
from ray._common.utils import get_or_create_event_loop
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.runtime_env.py_modules import upload_py_modules_if_needed
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.test_utils import (
    chdir,
    format_web_url,
    get_current_unused_port,
    request_with_auth_token,
    run_string_as_driver_nonblocking,
    wait_until_server_available,
)
from ray.dashboard.modules.job import job_agent as job_agent_module
from ray.dashboard.modules.job.common import (
    JOB_ACTOR_NAME_TEMPLATE,
    SUPERVISOR_ACTOR_RAY_NAMESPACE,
    JobInfo,
    JobInfoStorageClient,
    JobSubmitRequest,
    validate_request_type,
)
from ray.dashboard.modules.job.job_agent import JobAgent
from ray.dashboard.modules.job.job_head import JobAgentSubmissionClient
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.runtime_env.runtime_env import RuntimeEnv, RuntimeEnvConfig
from ray.tests.conftest import _ray_start, call_ray_start  # noqa: F401
from ray.util.state import get_node, list_actors, list_nodes

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

logger = logging.getLogger(__name__)

DRIVER_SCRIPT_DIR = os.path.join(os.path.dirname(__file__), "subprocess_driver_scripts")
EVENT_LOOP = get_or_create_event_loop()


def get_node_id_for_supervisor_actor_for_job(
    address: str, job_submission_id: str
) -> str:
    actors = list_actors(
        address=address,
        filters=[("ray_namespace", "=", SUPERVISOR_ACTOR_RAY_NAMESPACE)],
    )
    for actor in actors:
        if actor.name == JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_submission_id):
            return actor.node_id
    raise ValueError(f"actor not found for job_submission_id {job_submission_id}")


def get_node_ip_by_id(node_id: str) -> str:
    node = get_node(id=node_id)
    return node.node_ip


class JobAgentSubmissionBrowserClient(JobAgentSubmissionClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session.headers[
            "User-Agent"
        ] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"  # noqa: E501


@pytest_asyncio.fixture
async def job_sdk_client(make_sure_dashboard_http_port_unused):
    with _ray_start(include_dashboard=True, num_cpus=1) as ctx:
        node_ip = ctx.address_info["node_ip_address"]
        agent_address = build_address(node_ip, DEFAULT_DASHBOARD_AGENT_LISTEN_PORT)
        assert wait_until_server_available(agent_address)
        head_address = ctx.address_info["webui_url"]
        assert wait_until_server_available(head_address)
        yield (
            JobAgentSubmissionClient(format_web_url(agent_address)),
            JobSubmissionClient(format_web_url(head_address)),
        )


def _check_job(
    client: JobSubmissionClient, job_id: str, status: JobStatus, timeout: int = 10
) -> bool:
    res_status = client.get_job_status(job_id)
    assert res_status == status
    return True


@pytest.fixture(
    scope="module",
    params=[
        "no_working_dir",
        "local_working_dir",
        "s3_working_dir",
        "local_py_modules",
        "working_dir_and_local_py_modules_whl",
        "local_working_dir_zip",
        "pip_txt",
        "conda_yaml",
        "local_py_modules",
    ],
)
def runtime_env_option(request):
    import_in_task_script = """
import ray
ray.init(address="auto")

@ray.remote
def f():
    import pip_install_test

ray.get(f.remote())
"""
    if request.param == "no_working_dir":
        yield {
            "runtime_env": {},
            "entrypoint": "echo hello",
            "expected_logs": "hello\n",
        }
    elif request.param in {
        "local_working_dir",
        "local_working_dir_zip",
        "local_py_modules",
        "working_dir_and_local_py_modules_whl",
    }:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)

            hello_file = path / "test.py"
            with hello_file.open(mode="w") as f:
                f.write("from test_module import run_test\n")
                f.write("print(run_test())")

            module_path = path / "test_module"
            module_path.mkdir(parents=True)

            test_file = module_path / "test.py"
            with test_file.open(mode="w") as f:
                f.write("def run_test():\n")
                f.write("    return 'Hello from test_module!'\n")  # noqa: Q000

            init_file = module_path / "__init__.py"
            with init_file.open(mode="w") as f:
                f.write("from test_module.test import run_test\n")

            if request.param == "local_working_dir":
                yield {
                    "runtime_env": {"working_dir": tmp_dir},
                    "entrypoint": "python test.py",
                    "expected_logs": "Hello from test_module!\n",
                }
            elif request.param == "local_working_dir_zip":
                local_zipped_dir = shutil.make_archive(
                    os.path.join(tmp_dir, "test"), "zip", tmp_dir
                )
                yield {
                    "runtime_env": {"working_dir": local_zipped_dir},
                    "entrypoint": "python test.py",
                    "expected_logs": "Hello from test_module!\n",
                }
            elif request.param == "local_py_modules":
                yield {
                    "runtime_env": {"py_modules": [str(Path(tmp_dir) / "test_module")]},
                    "entrypoint": (
                        "python -c 'import test_module;print(test_module.run_test())'"
                    ),
                    "expected_logs": "Hello from test_module!\n",
                }
            elif request.param == "working_dir_and_local_py_modules_whl":
                yield {
                    "runtime_env": {
                        "working_dir": "s3://runtime-env-test/script_runtime_env.zip",
                        "py_modules": [
                            Path(os.path.dirname(__file__))
                            / "pip_install_test-0.5-py3-none-any.whl"
                        ],
                    },
                    "entrypoint": (
                        "python script.py && python -c 'import pip_install_test'"
                    ),
                    "expected_logs": (
                        "Executing main() from script.py !!\n"
                        "Good job!  You installed a pip module."
                    ),
                }
            else:
                raise ValueError(f"Unexpected pytest fixture option {request.param}")
    elif request.param == "s3_working_dir":
        yield {
            "runtime_env": {
                "working_dir": "s3://runtime-env-test/script_runtime_env.zip",
            },
            "entrypoint": "python script.py",
            "expected_logs": "Executing main() from script.py !!\n",
        }
    elif request.param == "pip_txt":
        with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
            pip_list = ["pip-install-test==0.5"]
            relative_filepath = "requirements.txt"
            pip_file = Path(relative_filepath)
            pip_file.write_text("\n".join(pip_list))
            runtime_env = {"pip": {"packages": relative_filepath, "pip_check": False}}
            yield {
                "runtime_env": runtime_env,
                "entrypoint": (
                    f"python -c 'import pip_install_test' && "
                    f"python -c '{import_in_task_script}'"
                ),
                "expected_logs": "Good job!  You installed a pip module.",
            }
    elif request.param == "conda_yaml":
        with tempfile.TemporaryDirectory() as tmpdir, chdir(tmpdir):
            conda_dict = {"dependencies": ["pip", {"pip": ["pip-install-test==0.5"]}]}
            relative_filepath = "environment.yml"
            conda_file = Path(relative_filepath)
            conda_file.write_text(yaml.dump(conda_dict))
            runtime_env = {"conda": relative_filepath}

            yield {
                "runtime_env": runtime_env,
                "entrypoint": f"python -c '{import_in_task_script}'",
                # TODO(architkulkarni): Uncomment after #22968 is fixed.
                # "entrypoint": "python -c 'import pip_install_test'",
                "expected_logs": "Good job!  You installed a pip module.",
            }
    else:
        assert False, f"Unrecognized option: {request.param}."


@pytest.mark.asyncio
async def test_submit_job(job_sdk_client, runtime_env_option, monkeypatch):
    # This flag allows for local testing of runtime env conda functionality
    # without needing a built Ray wheel.  Rather than insert the link to the
    # wheel into the conda spec, it links to the current Python site.
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", "1")

    agent_client, head_client = job_sdk_client

    runtime_env = runtime_env_option["runtime_env"]
    runtime_env = upload_working_dir_if_needed(
        runtime_env, include_gitignore=True, logger=logger
    )
    runtime_env = upload_py_modules_if_needed(
        runtime_env, include_gitignore=True, logger=logger
    )
    runtime_env = RuntimeEnv(**runtime_env_option["runtime_env"]).to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": runtime_env_option["entrypoint"]},
        JobSubmitRequest,
    )
    submit_result = await agent_client.submit_job_internal(request)
    job_id = submit_result.submission_id

    try:
        job_start_time = time.time()
        wait_for_condition(
            partial(
                _check_job,
                client=head_client,
                job_id=job_id,
                status=JobStatus.SUCCEEDED,
            ),
            timeout=300,
        )
        job_duration = time.time() - job_start_time
        print(f"The job took {job_duration}s to succeed.")
    except RuntimeError as e:
        # If the job is still pending, include job logs and info in error.
        if head_client.get_job_status(job_id) == JobStatus.PENDING:
            logs = head_client.get_job_logs(job_id)
            info = head_client.get_job_info(job_id)
            raise RuntimeError(
                f"Job was stuck in PENDING.\nLogs: {logs}\nInfo: {info}"
            ) from e

    # There is only one node, so there is no need to replace the client of the JobAgent
    resp = await agent_client.get_job_logs_internal(job_id)
    assert runtime_env_option["expected_logs"] in resp.logs


@pytest.mark.asyncio
async def test_submit_job_rejects_browsers(
    job_sdk_client, runtime_env_option, monkeypatch
):
    # This flag allows for local testing of runtime env conda functionality
    # without needing a built Ray wheel.  Rather than insert the link to the
    # wheel into the conda spec, it links to the current Python site.
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", "1")

    agent_client, head_client = job_sdk_client
    agent_address = agent_client._agent_address
    agent_client = JobAgentSubmissionBrowserClient(agent_address)

    runtime_env = runtime_env_option["runtime_env"]
    runtime_env = upload_working_dir_if_needed(
        runtime_env, include_gitignore=True, logger=logger
    )
    runtime_env = upload_py_modules_if_needed(
        runtime_env, include_gitignore=True, logger=logger
    )
    runtime_env = RuntimeEnv(**runtime_env_option["runtime_env"]).to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": runtime_env_option["entrypoint"]},
        JobSubmitRequest,
    )

    with pytest.raises(RuntimeError) as exc:
        _ = await agent_client.submit_job_internal(request)

    assert "status code 403" in str(exc.value)


@pytest.mark.asyncio
async def test_delete_job_rejects_browsers(job_sdk_client, monkeypatch):
    """Test that DELETE job requests from browsers are rejected."""
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", "1")

    agent_client, head_client = job_sdk_client

    # First, submit a job using the normal client
    runtime_env = RuntimeEnv().to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": "echo hello"},
        JobSubmitRequest,
    )
    submit_result = await agent_client.submit_job_internal(request)
    job_id = submit_result.submission_id

    # Now try to delete the job using browser-like headers
    agent_address = agent_client._agent_address
    browser_client = JobAgentSubmissionBrowserClient(agent_address)

    with pytest.raises(RuntimeError) as exc:
        _ = await browser_client.delete_job_internal(job_id)

    assert "status code 403" in str(exc.value)

    await browser_client.close()


@pytest.mark.asyncio
async def test_timeout(job_sdk_client):
    agent_client, head_client = job_sdk_client

    runtime_env = RuntimeEnv(
        pip={
            "packages": ["tensorflow", "requests", "botocore", "torch"],
            "pip_check": False,
            "pip_version": "==23.3.2;python_version=='3.9.16'",
        },
        config=RuntimeEnvConfig(setup_timeout_seconds=1),
    ).to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": "echo hello"},
        JobSubmitRequest,
    )

    submit_result = await agent_client.submit_job_internal(request)
    job_id = submit_result.submission_id

    wait_for_condition(
        partial(_check_job, client=head_client, job_id=job_id, status=JobStatus.FAILED),
        timeout=10,
    )

    data = head_client.get_job_info(job_id)
    assert "Failed to set up runtime environment" in data.message
    assert "Timeout" in data.message
    assert "setup_timeout_seconds" in data.message


@pytest.mark.asyncio
async def test_runtime_env_setup_failure(job_sdk_client):
    agent_client, head_client = job_sdk_client

    runtime_env = RuntimeEnv(working_dir="s3://does_not_exist.zip").to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": "echo hello"},
        JobSubmitRequest,
    )

    submit_result = await agent_client.submit_job_internal(request)
    job_id = submit_result.submission_id

    wait_for_condition(
        partial(_check_job, client=head_client, job_id=job_id, status=JobStatus.FAILED),
        timeout=10,
    )

    data = head_client.get_job_info(job_id)
    assert "Failed to set up runtime environment" in data.message


@pytest.mark.asyncio
async def test_stop_long_running_job(job_sdk_client):
    """
    Submit a job that runs for a while and stop it in the middle.
    """
    agent_client, head_client = job_sdk_client

    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        driver_script = """
print('Hello !')
import time
time.sleep(300) # This should never finish
raise RuntimeError('Intentionally failed.')
        """
        test_script_file = path / "test_script.py"
        with open(test_script_file, "w+") as file:
            file.write(driver_script)

        runtime_env = {"working_dir": tmp_dir}
        runtime_env = upload_working_dir_if_needed(
            runtime_env, include_gitignore=True, scratch_dir=tmp_dir, logger=logger
        )
        runtime_env = RuntimeEnv(**runtime_env).to_dict()

        request = validate_request_type(
            {"runtime_env": runtime_env, "entrypoint": "python test_script.py"},
            JobSubmitRequest,
        )
        submit_result = await agent_client.submit_job_internal(request)
        job_id = submit_result.submission_id

        resp = await agent_client.stop_job_internal(job_id)
        assert resp.stopped is True

        wait_for_condition(
            partial(
                _check_job, client=head_client, job_id=job_id, status=JobStatus.STOPPED
            ),
            timeout=10,
        )


@pytest.mark.asyncio
async def test_tail_job_logs_with_echo(job_sdk_client):
    agent_client, head_client = job_sdk_client

    runtime_env = RuntimeEnv().to_dict()
    entrypoint = "python -c \"import time; [(print('Hello', i), time.sleep(0.1)) for i in range(100)]\""  # noqa: E501
    request = validate_request_type(
        {
            "runtime_env": runtime_env,
            "entrypoint": entrypoint,
        },
        JobSubmitRequest,
    )

    submit_result = await agent_client.submit_job_internal(request)
    job_id = submit_result.submission_id

    i = 0
    async for lines in agent_client.tail_job_logs(job_id):
        print(lines, end="")
        for line in lines.strip().split("\n"):
            if (
                "Runtime env is setting up." in line
                or "Running entrypoint for job" in line
            ):
                continue
            assert line.split(" ") == ["Hello", str(i)]
            i += 1

    wait_for_condition(
        partial(
            _check_job, client=head_client, job_id=job_id, status=JobStatus.SUCCEEDED
        ),
        timeout=10,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
            "dashboard_agent_listen_port": DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
        }
    ],
    indirect=True,
)
async def test_job_log_in_multiple_node(
    make_sure_dashboard_http_port_unused,
    enable_test_module,
    disable_aiohttp_cache,
    ray_start_cluster_head,
):
    cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    cluster.add_node(
        dashboard_agent_listen_port=DEFAULT_DASHBOARD_AGENT_LISTEN_PORT + 1
    )
    cluster.add_node(
        dashboard_agent_listen_port=DEFAULT_DASHBOARD_AGENT_LISTEN_PORT + 2
    )

    node_ip = cluster.head_node.node_ip_address
    agent_address = build_address(node_ip, DEFAULT_DASHBOARD_AGENT_LISTEN_PORT)
    assert wait_until_server_available(agent_address)
    client = JobAgentSubmissionClient(format_web_url(agent_address))

    def _check_nodes():
        try:
            assert len(list_nodes()) == 3
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_check_nodes, timeout=15)

    job_ids = []
    job_check_status = []
    JOB_NUM = 10
    job_agent_ports = [
        DEFAULT_DASHBOARD_AGENT_LISTEN_PORT,
        DEFAULT_DASHBOARD_AGENT_LISTEN_PORT + 1,
        DEFAULT_DASHBOARD_AGENT_LISTEN_PORT + 2,
    ]
    for index in range(JOB_NUM):
        runtime_env = RuntimeEnv().to_dict()
        request = validate_request_type(
            {
                "runtime_env": runtime_env,
                "entrypoint": f"while true; do echo hello index-{index}"
                " && sleep 3600; done",
            },
            JobSubmitRequest,
        )

        submit_result = await client.submit_job_internal(request)
        job_ids.append(submit_result.submission_id)
        job_check_status.append(False)

    async def _check_all_jobs_log():
        response = request_with_auth_token("GET", webui_url + "/nodes?view=summary")
        response.raise_for_status()
        summary = response.json()
        assert summary["result"] is True, summary["msg"]
        summary = summary["data"]["summary"]

        for index, job_id in enumerate(job_ids):
            if job_check_status[index]:
                continue
            result_log = f"hello index-{index}"
            # Try to get the node id which supervisor actor running in.
            node_id = get_node_id_for_supervisor_actor_for_job(cluster.address, job_id)
            for node_info in summary:
                if node_info["raylet"]["nodeId"] == node_id:
                    break
            assert node_info["raylet"]["nodeId"] == node_id, f"node id: {node_id}"

            # Try to get the agent HTTP port by node id.
            for agent_port in job_agent_ports:
                if f"--listen-port={agent_port}" in " ".join(node_info["cmdline"]):
                    break
            assert f"--listen-port={agent_port}" in " ".join(
                node_info["cmdline"]
            ), f"port: {agent_port}"

            # Finally, we got the whole agent address, and try to get the job log.
            ip = get_node_ip_by_id(node_id)
            agent_address = f"{ip}:{agent_port}"
            assert wait_until_server_available(agent_address)
            client = JobAgentSubmissionClient(format_web_url(agent_address))
            resp = await client.get_job_logs_internal(job_id)
            assert result_log in resp.logs, f"logs: {resp.logs}"

            job_check_status[index] = True
        return True

    st = time.time()
    while time.time() - st <= 30:
        try:
            await _check_all_jobs_log()
            break
        except Exception as ex:
            print("error:", ex)
            time.sleep(1)
    assert all(job_check_status), job_check_status


def test_agent_logs_not_streamed_to_drivers():
    """Ensure when the job submission is used,
    (ray.init is called from an agent), the agent logs are
    not streamed to drivers.

    Related: https://github.com/ray-project/ray/issues/29944
    """
    script = """
import ray
from ray.job_submission import JobSubmissionClient, JobStatus
from ray._private.test_utils import format_web_url
from ray._common.test_utils import wait_for_condition

ray.init()
address = ray._private.worker._global_node.webui_url
address = format_web_url(address)
client = JobSubmissionClient(address)
submission_id = client.submit_job(entrypoint="ls")
wait_for_condition(
    lambda: client.get_job_status(submission_id) == JobStatus.SUCCEEDED
)
    """

    proc = run_string_as_driver_nonblocking(script)
    out_str = proc.stdout.read().decode("ascii")
    err_str = proc.stderr.read().decode("ascii")

    print(out_str, err_str)

    assert "(raylet)" not in out_str
    assert "(raylet)" not in err_str


@pytest.mark.asyncio
async def test_non_default_dashboard_agent_http_port(tmp_path):
    """Test that we can connect to the dashboard agent with a non-default
    http port.
    """
    import subprocess

    dashboard_agent_port = get_current_unused_port()
    cmd = f"ray start --head --dashboard-agent-listen-port {dashboard_agent_port}"
    subprocess.check_output(cmd, shell=True)

    try:
        # We will need to wait for the ray to be started in the subprocess.
        address_info = ray.init("auto", ignore_reinit_error=True).address_info

        node_ip = address_info["node_ip_address"]

        dashboard_agent_listen_port = address_info["dashboard_agent_listen_port"]
        agent_address = build_address(node_ip, dashboard_agent_listen_port)
        print("agent address = ", agent_address)

        agent_client = JobAgentSubmissionClient(format_web_url(agent_address))
        head_client = JobSubmissionClient(format_web_url(address_info["webui_url"]))

        assert wait_until_server_available(agent_address)

        # Submit a job through the agent.
        runtime_env = RuntimeEnv().to_dict()
        request = validate_request_type(
            {
                "runtime_env": runtime_env,
                "entrypoint": "echo hello",
            },
            JobSubmitRequest,
        )
        submit_result = await agent_client.submit_job_internal(request)
        job_id = submit_result.submission_id

        async def verify():
            # Wait for job finished.
            wait_for_condition(
                partial(
                    _check_job,
                    client=head_client,
                    job_id=job_id,
                    status=JobStatus.SUCCEEDED,
                ),
                timeout=10,
            )

            resp = await agent_client.get_job_logs_internal(job_id)
            assert "hello" in resp.logs, resp.logs

            return True

        await async_wait_for_condition(verify, retry_interval_ms=2000)
    finally:
        subprocess.check_output("ray stop --force", shell=True)


def _make_job_agent(*, is_head: bool, **kwargs) -> JobAgent:
    dashboard_agent = SimpleNamespace(
        is_head=is_head,
        session_name="session_test",
        gcs_address=kwargs.pop("gcs_address", "127.0.0.1:6379"),
        gcs_client=kwargs.pop("gcs_client", MagicMock()),
        log_dir=kwargs.pop("log_dir", "/tmp/ray/session_latest/logs"),
    )
    return JobAgent(dashboard_agent)


def _job_info(status: JobStatus) -> JobInfo:
    return JobInfo(status=status, entrypoint="echo hi")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "is_head,statuses,expect_job_manager",
    [
        # Unfinished jobs on the head: connect and recover them at startup.
        (True, [JobStatus.SUCCEEDED, JobStatus.RUNNING], True),
        (True, [JobStatus.PENDING], True),
        # Nothing to recover: stay lazy and do not connect to Ray as a driver.
        (True, [], False),
        (True, [JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.STOPPED], False),
        # Other nodes never create the JobManager at startup.
        (False, [JobStatus.RUNNING], False),
    ],
)
async def test_job_agent_run_creates_job_manager_only_for_unfinished_jobs(
    is_head, statuses, expect_job_manager
):
    """The head agent creates its JobManager at startup, connecting to Ray off
    the event loop thread, only when there are unfinished jobs to recover."""
    agent = _make_job_agent(is_head=is_head)
    jobs = {f"job_{i}": _job_info(status) for i, status in enumerate(statuses)}
    init_threads = []

    def init_ray_connection(gcs_address):
        assert gcs_address == "127.0.0.1:6379"
        init_threads.append(threading.current_thread())

    with patch.object(
        JobInfoStorageClient, "get_all_jobs", AsyncMock(return_value=jobs)
    ) as get_all_jobs, patch.object(
        optional_utils, "init_ray_connection", side_effect=init_ray_connection
    ), patch.object(
        job_agent_module, "JobManager"
    ) as job_manager_cls:
        await agent.run(server=None)

    if not is_head:
        get_all_jobs.assert_not_called()
    if expect_job_manager:
        assert len(init_threads) == 1
        assert init_threads[0] is not threading.current_thread()
        job_manager_cls.assert_called_once()
        assert agent._job_manager is job_manager_cls.return_value
    else:
        assert init_threads == []
        job_manager_cls.assert_not_called()
        assert agent._job_manager is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_step", ["list_jobs", "connect"])
async def test_job_agent_run_falls_back_to_lazy_init_on_failure(failing_step):
    """A failure to list jobs or to connect at startup must not fail the agent;
    the JobManager is then created on the next Job API request as before."""
    agent = _make_job_agent(is_head=True)
    error = ConnectionError("GCS is unavailable")
    get_all_jobs = AsyncMock(
        side_effect=error if failing_step == "list_jobs" else None,
        return_value={"job": _job_info(JobStatus.RUNNING)},
    )

    with patch.object(JobInfoStorageClient, "get_all_jobs", get_all_jobs), patch.object(
        optional_utils,
        "init_ray_connection",
        side_effect=error if failing_step == "connect" else None,
    ), patch.object(job_agent_module, "JobManager") as job_manager_cls:
        await agent.run(server=None)
        job_manager_cls.assert_not_called()
        assert agent._job_manager is None

        assert agent.get_job_manager() is job_manager_cls.return_value
        job_manager_cls.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "call_ray_start",
    [
        "ray start --head --num-cpus=1 --min-worker-port=0 --max-worker-port=0 "
        "--port 0 --include-dashboard=False"
    ],
    indirect=True,
)
async def test_restarted_head_job_agent_recovers_running_job(
    call_ray_start, tmp_path  # noqa: F811
):
    """A RUNNING job must be monitored again once the head agent restarts,
    without any Job API request reaching the new agent.

    The previous agent is a separate driver process that submits the job and
    exits, leaving the detached JobSupervisor running.
    """
    job_id = "test_restarted_head_job_agent"
    # `ray start` may not print the address, so connect to the local cluster.
    address = call_ray_start or "auto"
    run_string_as_driver(
        f"""
import asyncio

import ray
from ray.dashboard.modules.job.job_manager import JobManager
from ray.job_submission import JobStatus

ray.init(address="{address}")


async def main():
    job_manager = JobManager(
        ray._private.worker.global_worker.gcs_client, {str(tmp_path)!r}
    )
    await job_manager.submit_job(entrypoint="sleep 60", submission_id="{job_id}")
    while await job_manager.get_job_status("{job_id}") != "RUNNING":
        await asyncio.sleep(0.1)


asyncio.run(main())
"""
    )

    # Earlier tests in this module may leave this process connected to Ray.
    ray.shutdown()
    ray.init(address=address)
    gcs_client = ray._private.worker.global_worker.gcs_client
    job_info_client = JobInfoStorageClient(gcs_client)
    assert await job_info_client.get_status(job_id) == JobStatus.RUNNING

    # Start the new head agent. Nothing calls its Job API handlers.
    agent = _make_job_agent(
        is_head=True,
        gcs_address=ray._private.worker.global_worker.node.gcs_address,
        gcs_client=gcs_client,
        log_dir=str(tmp_path),
    )
    await agent.run(server=None)

    # Only a JobManager monitoring the job notices its supervisor dying.
    supervisor = ray.get_actor(
        JOB_ACTOR_NAME_TEMPLATE.format(job_id=job_id),
        namespace=SUPERVISOR_ACTOR_RAY_NAMESPACE,
    )
    ray.kill(supervisor, no_restart=True)

    async def job_failed():
        return await job_info_client.get_status(job_id) == JobStatus.FAILED

    await async_wait_for_condition(job_failed, timeout=30)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
