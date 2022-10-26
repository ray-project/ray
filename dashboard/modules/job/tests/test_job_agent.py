import asyncio
import logging
import os
import requests
import shutil
import sys
import tempfile
import time
from pathlib import Path
from functools import partial
import pytest
import yaml

from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray._private.runtime_env.py_modules import upload_py_modules_if_needed
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.test_utils import (
    chdir,
    format_web_url,
    wait_until_server_available,
    wait_for_condition,
)
from ray.dashboard.modules.job.common import JobSubmitRequest
from ray.dashboard.modules.job.utils import (
    validate_request_type,
    get_supervisor_actor_into,
)
from ray.dashboard.tests.conftest import *  # noqa
from ray.runtime_env.runtime_env import RuntimeEnv, RuntimeEnvConfig
from ray.experimental.state.api import list_nodes
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.tests.conftest import _ray_start
from ray.dashboard.modules.job.job_head import JobAgentSubmissionClient


# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

logger = logging.getLogger(__name__)

DRIVER_SCRIPT_DIR = os.path.join(os.path.dirname(__file__), "subprocess_driver_scripts")
EVENT_LOOP = asyncio.get_event_loop()


@pytest.fixture
def job_sdk_client():
    with _ray_start(include_dashboard=True, num_cpus=1) as ctx:
        ip, _ = ctx.address_info["webui_url"].split(":")
        agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
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
    return res_status == status


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
                        "python -c 'import test_module;"
                        "print(test_module.run_test())'"
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
    runtime_env = upload_working_dir_if_needed(runtime_env, logger=logger)
    runtime_env = upload_py_modules_if_needed(runtime_env, logger=logger)
    runtime_env = RuntimeEnv(**runtime_env_option["runtime_env"]).to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": runtime_env_option["entrypoint"]},
        JobSubmitRequest,
    )
    submit_result = await agent_client.submit_job_internal(request)
    job_id = submit_result.submission_id

    wait_for_condition(
        partial(
            _check_job, client=head_client, job_id=job_id, status=JobStatus.SUCCEEDED
        ),
        timeout=120,
    )

    # There is only one node, so there is no need to replace the client of the JobAgent
    resp = await agent_client.get_job_logs_internal(job_id)
    assert runtime_env_option["expected_logs"] in resp.logs


@pytest.mark.asyncio
async def test_timeout(job_sdk_client):
    agent_client, head_client = job_sdk_client

    runtime_env = RuntimeEnv(
        pip={
            "packages": ["tensorflow", "requests", "botocore", "torch"],
            "pip_check": False,
            "pip_version": "==22.0.2;python_version=='3.8.11'",
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
    assert "consider increasing `setup_timeout_seconds`" in data.message


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
        runtime_env = upload_working_dir_if_needed(runtime_env, tmp_dir, logger=logger)
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
    enable_test_module, disable_aiohttp_cache, ray_start_cluster_head
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

    ip, port = cluster.webui_url.split(":")
    agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
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
        response = requests.get(webui_url + "/nodes?view=summary")
        response.raise_for_status()
        summary = response.json()
        assert summary["result"] is True, summary["msg"]
        summary = summary["data"]["summary"]

        for index, job_id in enumerate(job_ids):
            if job_check_status[index]:
                continue
            result_log = f"hello index-{index}"
            gcs_aio_client = GcsAioClient(
                address=cluster.address, nums_reconnect_retry=0
            )
            supervisor_actor_info = await get_supervisor_actor_into(
                gcs_aio_client, job_id
            )

            # Try to get the node id which supervisor actor running in.
            node_id = supervisor_actor_info.actor_table_data.address.raylet_id.hex()
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
            ip = supervisor_actor_info.actor_table_data.address.ip_address
            agent_address = f"{ip}:{agent_port}"
            assert wait_until_server_available(agent_address)
            client = JobAgentSubmissionClient(format_web_url(agent_address))
            resp = await client.get_job_logs_internal(job_id)
            assert result_log in resp.logs, resp.logs

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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
