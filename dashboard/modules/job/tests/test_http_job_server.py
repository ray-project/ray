import logging
from pathlib import Path
import sys
import tempfile
import requests

import pytest

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (format_web_url, wait_for_condition,
                                     wait_until_server_available)
from ray._private.job_manager import JobStatus
from ray.dashboard.modules.job.sdk import JobSubmissionClient
from ray.dashboard.modules.job.job_head import JOBS_API_ROUTE_SUBMIT

logger = logging.getLogger(__name__)


@pytest.fixture
def job_sdk_client(ray_start_with_dashboard, disable_aiohttp_cache,
                   enable_test_module):
    address = ray_start_with_dashboard["webui_url"]
    assert wait_until_server_available(address)
    yield JobSubmissionClient(format_web_url(address))


def _check_job_succeeded(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    if status == JobStatus.FAILED:
        logs = client.get_job_logs(job_id)
        raise RuntimeError(f"Job failed\nlogs:\n{logs}")
    return status == JobStatus.SUCCEEDED


def _check_job_failed(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status == JobStatus.FAILED


def _check_job_stopped(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status == JobStatus.STOPPED


def _check_job_does_not_exist(client: JobSubmissionClient,
                              job_id: str) -> bool:
    status = client.get_job_status(job_id)
    return status == JobStatus.DOES_NOT_EXIST


@pytest.fixture(
    scope="function",
    params=["no_working_dir", "local_working_dir", "s3_working_dir"])
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
                f.write("    return 'Hello from test_module!'\n")

            init_file = module_path / "__init__.py"
            with init_file.open(mode="w") as f:
                f.write("from test_module.test import run_test\n")

            yield {
                "runtime_env": {
                    "working_dir": tmp_dir
                },
                "entrypoint": "python test.py",
                "expected_logs": "Hello from test_module!\n",
            }
    elif request.param == "s3_working_dir":
        yield {
            "runtime_env": {
                "working_dir": "s3://runtime-env-test/script.zip",
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
        runtime_env=working_dir_option["runtime_env"])

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
    with pytest.raises(requests.exceptions.HTTPError) as e:
        _ = client._do_request(
            "POST",
            JOBS_API_ROUTE_SUBMIT,
            json_data={"key": "baaaad request"},
        )

    ex_message = str(e.value)
    assert "400 Client Error" in ex_message
    assert "TypeError: __init__() got an unexpected keyword argument" in ex_message  # noqa: E501

    # 405 - HTTPMethodNotAllowed
    with pytest.raises(requests.exceptions.HTTPError) as e:
        _ = client._do_request(
            "GET",
            JOBS_API_ROUTE_SUBMIT,
            json_data={"key": "baaaad request"},
        )
    ex_message = str(e.value)
    assert "405 Client Error: Method Not Allowed" in ex_message

    # 500 - HTTPInternalServerError
    with pytest.raises(requests.exceptions.HTTPError) as e:
        _ = client.submit_job(
            entrypoint="echo hello",
            runtime_env={"working_dir": "s3://does_not_exist"})
    ex_message = str(e.value)
    assert "500 Server Error" in ex_message
    assert "Only .zip files supported for S3 URIs" in ex_message


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
            entrypoint="python test_script.py",
            runtime_env={"working_dir": tmp_dir})

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
            entrypoint="python test_script.py",
            runtime_env={"working_dir": tmp_dir})
        assert client.stop_job(job_id) is True
        wait_for_condition(_check_job_stopped, client=client, job_id=job_id)


def test_job_metadata(job_sdk_client):
    client = job_sdk_client

    print_metadata_cmd = (
        "python -c\""
        "import ray;"
        "ray.init();"
        "job_config=ray.worker.global_worker.core_worker.get_job_config();"
        "print(dict(sorted(job_config.metadata.items())))"
        "\"")

    job_id = client.submit_job(
        entrypoint=print_metadata_cmd,
        metadata={
            "key1": "val1",
            "key2": "val2"
        })

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    assert str({
        "job_submission_id": job_id,
        "key1": "val1",
        "key2": "val2"
    }) in client.get_job_logs(job_id)


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

    _check_job_does_not_exist(client, "nonexistent_job")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
