import logging
from pathlib import Path
import sys
import tempfile
from typing import Optional

import pytest
from unittest.mock import patch

import ray
from ray.dashboard.modules.job.common import CURRENT_VERSION, JobStatus
from ray.dashboard.modules.job.sdk import (
    ClusterInfo,
    JobSubmissionClient,
    parse_cluster_info,
)
from ray.dashboard.tests.conftest import *  # noqa
from ray.ray_constants import DEFAULT_DASHBOARD_PORT
from ray.tests.conftest import _ray_start
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def headers():
    return {"Connection": "keep-alive", "Authorization": "TOK:<MY_TOKEN>"}


@pytest.fixture(scope="module")
def job_sdk_client(headers):
    with _ray_start(include_dashboard=True, num_cpus=1) as address_info:
        address = address_info["webui_url"]
        assert wait_until_server_available(address)
        yield JobSubmissionClient(format_web_url(address), headers=headers)


def _check_job_succeeded(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    if status.status == JobStatus.FAILED:
        logs = client.get_job_logs(job_id)
        raise RuntimeError(f"Job failed\nlogs:\n{logs}")
    return status.status == JobStatus.SUCCEEDED


def _check_job_failed(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status.status == JobStatus.FAILED


def _check_job_stopped(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status.status == JobStatus.STOPPED


@pytest.fixture(
    scope="module", params=["no_working_dir", "local_working_dir", "s3_working_dir"]
)
def working_dir_option(request):
    if request.param == "no_working_dir":
        yield {
            "runtime_env": {},
            "entrypoint": "echo hello",
            "expected_logs": "hello\n",
        }
    elif request.param == "local_working_dir":
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

            yield {
                "runtime_env": {"working_dir": tmp_dir},
                "entrypoint": "python test.py",
                "expected_logs": "Hello from test_module!\n",
            }
    elif request.param == "s3_working_dir":
        yield {
            "runtime_env": {
                "working_dir": "s3://runtime-env-test/script_runtime_env.zip",
            },
            "entrypoint": "python script.py",
            "expected_logs": "Executing main() from script.py !!\n",
        }
    else:
        assert False, f"Unrecognized option: {request.param}."


def test_submit_job(job_sdk_client, working_dir_option):
    client = job_sdk_client

    job_id = client.submit_job(
        entrypoint=working_dir_option["entrypoint"],
        runtime_env=working_dir_option["runtime_env"],
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    logs = client.get_job_logs(job_id)
    assert logs == working_dir_option["expected_logs"]


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
    job_id = client.submit_job(
        entrypoint="echo hello", runtime_env={"working_dir": "s3://not_a_zip"}
    )

    wait_for_condition(_check_job_failed, client=client, job_id=job_id)
    status = client.get_job_status(job_id)
    assert "Only .zip files supported for remote URIs" in status.message


def test_runtime_env_setup_failure(job_sdk_client):
    client = job_sdk_client
    job_id = client.submit_job(
        entrypoint="echo hello", runtime_env={"working_dir": "s3://does_not_exist.zip"}
    )

    wait_for_condition(_check_job_failed, client=client, job_id=job_id)
    status = client.get_job_status(job_id)
    assert "Failed to setup runtime environment" in status.message


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


def test_job_metadata(job_sdk_client):
    client = job_sdk_client

    print_metadata_cmd = (
        'python -c"'
        "import ray;"
        "ray.init();"
        "job_config=ray.worker.global_worker.core_worker.get_job_config();"
        "print(dict(sorted(job_config.metadata.items())))"
        '"'
    )

    job_id = client.submit_job(
        entrypoint=print_metadata_cmd, metadata={"key1": "val1", "key2": "val2"}
    )

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    assert (
        str(
            {
                "job_name": job_id,
                "job_submission_id": job_id,
                "key1": "val1",
                "key2": "val2",
            }
        )
        in client.get_job_logs(job_id)
    )


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

    wait_for_condition(_check_job_succeeded, client=client, job_id=r.json()["job_id"])


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


@pytest.mark.parametrize("scheme", ["http", "https", "ray", "fake_module"])
@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "fake.dns.name"])
@pytest.mark.parametrize("port", [None, 8265, 10000])
def test_parse_cluster_info(scheme: str, host: str, port: Optional[int]):
    address = f"{scheme}://{host}"
    if port is not None:
        address += f":{port}"

    final_port = port if port is not None else DEFAULT_DASHBOARD_PORT
    if scheme in {"http", "ray"}:
        assert parse_cluster_info(address, False) == ClusterInfo(
            address=f"http://{host}:{final_port}",
            cookies=None,
            metadata=None,
            headers=None,
        )
    elif scheme == "https":
        assert parse_cluster_info(address, False) == ClusterInfo(
            address=f"https://{host}:{final_port}",
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

        i = 0
        async for lines in client.tail_job_logs(job_id):
            print(lines, end="")
            for line in lines.strip().split("\n"):
                assert line.split(" ") == ["Hello", str(i)]
                i += 1

        wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
