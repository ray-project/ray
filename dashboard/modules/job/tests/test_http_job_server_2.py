import asyncio
import logging
import os
import sys
import tempfile
from pathlib import Path
import time
from typing import Optional
from unittest.mock import patch

import pytest

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.dashboard.modules.dashboard_sdk import ClusterInfo, parse_cluster_info
from ray.dashboard.modules.job.job_head import JobHead
from ray.dashboard.modules.version import CURRENT_VERSION
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobSubmissionClient
from ray.tests.conftest import _ray_start
from ray.dashboard.modules.job.tests.test_cli_integration import set_env_var
from ray.dashboard.modules.job.tests.test_http_job_server import (
    _check_job_stopped,
    _check_job_succeeded,
)

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
            verify=True,
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
    "ray.dashboard.modules.job.tests.test_http_job_server_2._hook"
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
