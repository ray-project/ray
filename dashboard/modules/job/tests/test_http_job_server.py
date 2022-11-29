import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
import subprocess
import time
from typing import Optional
from unittest.mock import patch

import pytest
from ray.runtime_env.runtime_env import RuntimeEnv, RuntimeEnvConfig
import yaml

import ray
from ray._private.test_utils import (
    chdir,
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.dashboard.modules.dashboard_sdk import ClusterInfo, parse_cluster_info
from ray.dashboard.modules.job.pydantic_models import JobDetails
from ray.dashboard.modules.job.job_head import JobHead
from ray.dashboard.modules.version import CURRENT_VERSION
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobStatus, JobSubmissionClient
from ray.tests.conftest import _ray_start
from ray.dashboard.modules.job.tests.test_cli_integration import set_env_var

# This test requires you have AWS credentials set up (any AWS credentials will
# do, this test only accesses a public bucket).

logger = logging.getLogger(__name__)

DRIVER_SCRIPT_DIR = os.path.join(os.path.dirname(__file__), "subprocess_driver_scripts")


@pytest.fixture(scope="module")
def headers():
    return {"Connection": "keep-alive", "Authorization": "TOK:<MY_TOKEN>"}


@pytest.fixture(scope="module")
def job_sdk_client(headers) -> JobSubmissionClient:
    with _ray_start(include_dashboard=True, num_cpus=1) as ctx:
        address = ctx.address_info["webui_url"]
        assert wait_until_server_available(address)
        yield JobSubmissionClient(format_web_url(address), headers=headers)


@pytest.fixture
def shutdown_only():
    yield None
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_submit_job_with_resources(shutdown_only):
    ctx = ray.init(
        include_dashboard=True,
        num_cpus=1,
        num_gpus=1,
        resources={"Custom": 1},
        dashboard_port=8269,
    )
    address = ctx.address_info["webui_url"]
    client = JobSubmissionClient(format_web_url(address))
    # Check the case of too many resources.
    for kwargs in [
        {"entrypoint_num_cpus": 2},
        {"entrypoint_num_gpus": 2},
        {"entrypoint_resources": {"Custom": 2}},
    ]:
        job_id = client.submit_job(entrypoint="echo hello", **kwargs)
        data = client.get_job_info(job_id)
        assert "waiting for resources" in data.message

    # Check the case of sufficient resources.
    job_id = client.submit_job(
        entrypoint="echo hello",
        entrypoint_num_cpus=1,
        entrypoint_num_gpus=1,
        entrypoint_resources={"Custom": 1},
    )
    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id, timeout=10)


@pytest.mark.parametrize("use_sdk", [True, False])
def test_list_jobs_empty(headers, use_sdk: bool):
    # Create a cluster using `ray start` instead of `ray.init` to avoid creating a job
    subprocess.check_output(["ray", "start", "--head"])
    address = "http://127.0.0.1:8265"
    try:
        with set_env_var("RAY_ADDRESS", address):
            client = JobSubmissionClient(format_web_url(address), headers=headers)

            if use_sdk:
                assert client.list_jobs() == []
            else:
                r = client._do_request(
                    "GET",
                    "/api/jobs/",
                )

                assert r.status_code == 200
                assert json.loads(r.text) == []

    finally:
        subprocess.check_output(["ray", "stop", "--force"])


@pytest.mark.parametrize("use_sdk", [True, False])
def test_list_jobs(job_sdk_client: JobSubmissionClient, use_sdk: bool):
    client = job_sdk_client

    runtime_env = {"env_vars": {"TEST": "123"}}
    metadata = {"foo": "bar"}
    entrypoint = "echo hello"
    submission_id = client.submit_job(
        entrypoint=entrypoint, runtime_env=runtime_env, metadata=metadata
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id=submission_id)
    if use_sdk:
        info: JobDetails = next(
            job_info
            for job_info in client.list_jobs()
            if job_info.submission_id == submission_id
        )
    else:
        r = client._do_request(
            "GET",
            "/api/jobs/",
        )

        assert r.status_code == 200
        jobs_info_json = json.loads(r.text)
        info_json = next(
            job_info
            for job_info in jobs_info_json
            if job_info["submission_id"] == submission_id
        )
        info = JobDetails(**info_json)

    assert info.entrypoint == entrypoint
    assert info.status == JobStatus.SUCCEEDED
    assert info.message is not None
    assert info.end_time >= info.start_time
    assert info.runtime_env == runtime_env
    assert info.metadata == metadata

    # Test get job status by job / driver id
    status = client.get_job_status(info.submission_id)
    assert status == JobStatus.SUCCEEDED


def _check_job_succeeded(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    if status == JobStatus.FAILED:
        logs = client.get_job_logs(job_id)
        raise RuntimeError(
            f"Job failed\nlogs:\n{logs}, info: {client.get_job_info(job_id)}"
        )
    return status == JobStatus.SUCCEEDED


def _check_job_failed(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status == JobStatus.FAILED


def _check_job_stopped(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status == JobStatus.STOPPED


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


def test_submit_job(job_sdk_client, runtime_env_option, monkeypatch):
    # This flag allows for local testing of runtime env conda functionality
    # without needing a built Ray wheel.  Rather than insert the link to the
    # wheel into the conda spec, it links to the current Python site.
    monkeypatch.setenv("RAY_RUNTIME_ENV_LOCAL_DEV_MODE", "1")

    client = job_sdk_client

    job_id = client.submit_job(
        entrypoint=runtime_env_option["entrypoint"],
        runtime_env=runtime_env_option["runtime_env"],
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id, timeout=60)

    logs = client.get_job_logs(job_id)
    assert runtime_env_option["expected_logs"] in logs


def test_timeout(job_sdk_client):
    client = job_sdk_client

    job_id = client.submit_job(
        entrypoint="echo hello",
        # Assume pip packages take > 1s to download, or this test will spuriously fail.
        runtime_env=RuntimeEnv(
            pip={
                "packages": ["tensorflow", "requests", "botocore", "torch"],
                "pip_check": False,
                "pip_version": "==22.0.2;python_version=='3.8.11'",
            },
            config=RuntimeEnvConfig(setup_timeout_seconds=1),
        ),
    )

    wait_for_condition(_check_job_failed, client=client, job_id=job_id, timeout=10)
    data = client.get_job_info(job_id)
    assert "Failed to set up runtime environment" in data.message
    assert "Timeout" in data.message
    assert "consider increasing `setup_timeout_seconds`" in data.message


def test_per_task_runtime_env(job_sdk_client: JobSubmissionClient):
    run_cmd = "python per_task_runtime_env.py"
    job_id = job_sdk_client.submit_job(
        entrypoint=run_cmd,
        runtime_env={"working_dir": DRIVER_SCRIPT_DIR},
    )

    wait_for_condition(_check_job_succeeded, client=job_sdk_client, job_id=job_id)


def test_ray_tune_basic(job_sdk_client: JobSubmissionClient):
    run_cmd = "python ray_tune_basic.py"
    job_id = job_sdk_client.submit_job(
        entrypoint=run_cmd,
        runtime_env={"working_dir": DRIVER_SCRIPT_DIR},
    )
    wait_for_condition(
        _check_job_succeeded, timeout=30, client=job_sdk_client, job_id=job_id
    )


def test_http_bad_request(job_sdk_client):
    """
    Send bad requests to job http server and ensure right return code and
    error message is returned via http.
    """
    client = job_sdk_client

    # 400 - HTTPBadRequest
    r = client._do_request(
        "POST",
        "/api/jobs/",
        json_data={"key": "baaaad request"},
    )

    assert r.status_code == 400
    assert "TypeError: __init__() got an unexpected keyword argument" in r.text


def test_invalid_runtime_env(job_sdk_client):
    client = job_sdk_client
    with pytest.raises(ValueError, match="Only .zip files supported"):
        client.submit_job(
            entrypoint="echo hello", runtime_env={"working_dir": "s3://not_a_zip"}
        )


def test_runtime_env_setup_failure(job_sdk_client):
    client = job_sdk_client
    job_id = client.submit_job(
        entrypoint="echo hello", runtime_env={"working_dir": "s3://does_not_exist.zip"}
    )

    wait_for_condition(_check_job_failed, client=client, job_id=job_id)
    data = client.get_job_info(job_id)
    assert "Failed to set up runtime environment" in data.message


def test_submit_job_with_exception_in_driver(job_sdk_client):
    """
    Submit a job that's expected to throw exception while executing.
    """
    client = job_sdk_client

    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        driver_script = """
print('Hello !')
raise RuntimeError('Intentionally failed.')
        """
        test_script_file = path / "test_script.py"
        with open(test_script_file, "w+") as file:
            file.write(driver_script)

        job_id = client.submit_job(
            entrypoint="python test_script.py", runtime_env={"working_dir": tmp_dir}
        )

        wait_for_condition(_check_job_failed, client=client, job_id=job_id)
        logs = client.get_job_logs(job_id)
        assert "Hello !" in logs
        assert "RuntimeError: Intentionally failed." in logs


def test_stop_long_running_job(job_sdk_client):
    """
    Submit a job that runs for a while and stop it in the middle.
    """
    client = job_sdk_client

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

        job_id = client.submit_job(
            entrypoint="python test_script.py", runtime_env={"working_dir": tmp_dir}
        )
        assert client.stop_job(job_id) is True
        wait_for_condition(_check_job_stopped, client=client, job_id=job_id)


def test_delete_job(job_sdk_client, capsys):
    """
    Submit a job and delete it.
    """
    client: JobSubmissionClient = job_sdk_client

    job_id = client.submit_job(entrypoint="sleep 300 && echo hello")
    with pytest.raises(Exception, match="but it is in a non-terminal state"):
        # This should fail because the job is not in a terminal state.
        client.delete_job(job_id)

    # Check that the job appears in list_jobs
    jobs = client.list_jobs()
    assert job_id in [job.submission_id for job in jobs]

    finished_job_id = client.submit_job(entrypoint="echo hello")
    wait_for_condition(_check_job_succeeded, client=client, job_id=finished_job_id)
    deleted = client.delete_job(finished_job_id)
    assert deleted is True

    # Check that the job no longer appears in list_jobs
    jobs = client.list_jobs()
    assert finished_job_id not in [job.submission_id for job in jobs]


def test_job_metadata(job_sdk_client):
    client = job_sdk_client

    print_metadata_cmd = (
        'python -c"'
        "import ray;"
        "ray.init();"
        "job_config=ray._private.worker.global_worker.core_worker.get_job_config();"
        "print(dict(sorted(job_config.metadata.items())))"
        '"'
    )

    job_id = client.submit_job(
        entrypoint=print_metadata_cmd, metadata={"key1": "val1", "key2": "val2"}
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    assert str(
        {
            "job_name": job_id,
            "job_submission_id": job_id,
            "key1": "val1",
            "key2": "val2",
        }
    ) in client.get_job_logs(job_id)


def test_pass_job_id(job_sdk_client):
    client = job_sdk_client

    job_id = "my_custom_id"
    returned_id = client.submit_job(entrypoint="echo hello", job_id=job_id)

    assert returned_id == job_id
    wait_for_condition(_check_job_succeeded, client=client, job_id=returned_id)

    # Test that a duplicate job_id is rejected.
    with pytest.raises(Exception, match=f"{job_id} already exists"):
        returned_id = client.submit_job(entrypoint="echo hello", job_id=job_id)


def test_nonexistent_job(job_sdk_client):
    client = job_sdk_client

    with pytest.raises(RuntimeError, match="nonexistent_job does not exist"):
        client.get_job_status("nonexistent_job")


def test_submit_optional_args(job_sdk_client):
    """Check that job_id, runtime_env, and metadata are optional."""
    client = job_sdk_client

    r = client._do_request(
        "POST",
        "/api/jobs/",
        json_data={"entrypoint": "ls"},
    )

    wait_for_condition(
        _check_job_succeeded, client=client, job_id=r.json()["submission_id"]
    )


def test_submit_still_accepts_job_id_or_submission_id(job_sdk_client):
    """Check that job_id, runtime_env, and metadata are optional."""
    client = job_sdk_client

    client._do_request(
        "POST",
        "/api/jobs/",
        json_data={"entrypoint": "ls", "job_id": "raysubmit_12345"},
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id="raysubmit_12345")

    client._do_request(
        "POST",
        "/api/jobs/",
        json_data={"entrypoint": "ls", "submission_id": "raysubmit_23456"},
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id="raysubmit_23456")


def test_missing_resources(job_sdk_client):
    """Check that 404s are raised for resources that don't exist."""
    client = job_sdk_client

    conditions = [
        ("GET", "/api/jobs/fake_job_id"),
        ("GET", "/api/jobs/fake_job_id/logs"),
        ("POST", "/api/jobs/fake_job_id/stop"),
        ("GET", "/api/packages/fake_package_uri"),
    ]

    for method, route in conditions:
        assert client._do_request(method, route).status_code == 404


def test_version_endpoint(job_sdk_client):
    client = job_sdk_client

    r = client._do_request("GET", "/api/version")
    assert r.status_code == 200
    assert r.json() == {
        "version": CURRENT_VERSION,
        "ray_version": ray.__version__,
        "ray_commit": ray.__commit__,
    }


def test_request_headers(job_sdk_client):
    client = job_sdk_client

    with patch("requests.request") as mock_request:
        _ = client._do_request(
            "POST",
            "/api/jobs/",
            json_data={"entrypoint": "ls"},
        )
        mock_request.assert_called_with(
            "POST",
            "http://127.0.0.1:8265/api/jobs/",
            cookies=None,
            data=None,
            json={"entrypoint": "ls"},
            headers={"Connection": "keep-alive", "Authorization": "TOK:<MY_TOKEN>"},
        )


@pytest.mark.parametrize("scheme", ["http", "https", "fake_module"])
@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "fake.dns.name"])
@pytest.mark.parametrize("port", [None, 8265, 10000])
def test_parse_cluster_info(scheme: str, host: str, port: Optional[int]):
    address = f"{scheme}://{host}"
    if port is not None:
        address += f":{port}"

    if scheme in {"http", "https"}:
        assert parse_cluster_info(address, False) == ClusterInfo(
            address=address,
            cookies=None,
            metadata=None,
            headers=None,
        )
    else:
        with pytest.raises(RuntimeError):
            parse_cluster_info(address, False)


@pytest.mark.asyncio
async def test_tail_job_logs(job_sdk_client):
    client = job_sdk_client
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        driver_script = """
import time
for i in range(100):
    print("Hello", i)
    time.sleep(0.1)
"""
        test_script_file = path / "test_script.py"
        with open(test_script_file, "w+") as f:
            f.write(driver_script)

        job_id = client.submit_job(
            entrypoint="python test_script.py", runtime_env={"working_dir": tmp_dir}
        )

        st = time.time()
        while time.time() - st <= 10:
            try:
                i = 0
                async for lines in client.tail_job_logs(job_id):
                    print(lines, end="")
                    for line in lines.strip().split("\n"):
                        assert line.split(" ") == ["Hello", str(i)]
                        i += 1
            except Exception as ex:
                print("Exception:", ex)

        wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)


def _hook(env):
    with open(env["env_vars"]["TEMPPATH"], "w+") as f:
        f.write(env["env_vars"]["TOKEN"])
    return env


def test_jobs_env_hook(job_sdk_client: JobSubmissionClient):
    client = job_sdk_client

    _, path = tempfile.mkstemp()
    runtime_env = {"env_vars": {"TEMPPATH": path, "TOKEN": "Ray rocks!"}}
    run_job_script = """
import os
import ray
os.environ["RAY_RUNTIME_ENV_HOOK"] =\
    "ray.dashboard.modules.job.tests.test_http_job_server._hook"
ray.init(address="auto")
"""
    entrypoint = f"python -c '{run_job_script}'"
    job_id = client.submit_job(entrypoint=entrypoint, runtime_env=runtime_env)

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    with open(path) as f:
        assert f.read().strip() == "Ray rocks!"


@pytest.mark.asyncio
async def test_job_head_choose_job_agent():
    with set_env_var("CANDIDATE_AGENT_NUMBER", "2"):
        import importlib

        importlib.reload(ray.dashboard.consts)

        from ray.dashboard.datacenter import DataSource

        class MockJobHead(JobHead):
            def __init__(self):
                self._agents = dict()

        DataSource.agents = {}
        DataSource.node_id_to_ip = {}
        job_head = MockJobHead()

        def add_agent(agent):
            node_id = agent[0]
            node_ip = agent[1]["ipAddress"]
            http_port = agent[1]["httpPort"]
            grpc_port = agent[1]["grpcPort"]
            DataSource.node_id_to_ip[node_id] = node_ip
            DataSource.agents[node_id] = (http_port, grpc_port)

        def del_agent(agent):
            node_id = agent[0]
            DataSource.node_id_to_ip.pop(node_id)
            DataSource.agents.pop(node_id)

        agent_1 = (
            "node1",
            dict(
                ipAddress="1.1.1.1",
                httpPort=1,
                grpcPort=1,
                httpAddress="1.1.1.1:1",
            ),
        )
        agent_2 = (
            "node2",
            dict(
                ipAddress="2.2.2.2",
                httpPort=2,
                grpcPort=2,
                httpAddress="2.2.2.2:2",
            ),
        )
        agent_3 = (
            "node3",
            dict(
                ipAddress="3.3.3.3",
                httpPort=3,
                grpcPort=3,
                httpAddress="3.3.3.3:3",
            ),
        )

        add_agent(agent_1)
        job_agent_client = await job_head.choose_agent()
        assert job_agent_client._agent_address == "http://1.1.1.1:1"

        del_agent(agent_1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(job_head.choose_agent(), timeout=3)

        add_agent(agent_1)
        add_agent(agent_2)
        add_agent(agent_3)

        # Theoretically, the probability of failure is 1/3^100
        addresses_1 = set()
        for address in range(100):
            job_agent_client = await job_head.choose_agent()
            addresses_1.add(job_agent_client._agent_address)
        assert len(addresses_1) == 2
        addresses_2 = set()
        for address in range(100):
            job_agent_client = await job_head.choose_agent()
            addresses_2.add(job_agent_client._agent_address)
        assert len(addresses_2) == 2 and addresses_1 == addresses_2

        for agent in [agent_1, agent_2, agent_3]:
            if f"http://{agent[1]['httpAddress']}" in addresses_2:
                break
        del_agent(agent)

        # Theoretically, the probability of failure is 1/2^100
        addresses_3 = set()
        for address in range(100):
            job_agent_client = await job_head.choose_agent()
            addresses_3.add(job_agent_client._agent_address)
        assert len(addresses_3) == 2
        assert addresses_2 - addresses_3 == {f"http://{agent[1]['httpAddress']}"}
        addresses_4 = set()
        for address in range(100):
            job_agent_client = await job_head.choose_agent()
            addresses_4.add(job_agent_client._agent_address)
        assert addresses_4 == addresses_3

        for agent in [agent_1, agent_2, agent_3]:
            if f"http://{agent[1]['httpAddress']}" in addresses_4:
                break
        del_agent(agent)
        address = None
        for _ in range(3):
            job_agent_client = await job_head.choose_agent()
            assert address is None or address == job_agent_client._agent_address
            address = job_agent_client._agent_address


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
