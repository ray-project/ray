import logging
from pathlib import Path
import sys
import tempfile

import pytest

from ray.dashboard.tests.conftest import *  # noqa
from ray._private.test_utils import (format_web_url, wait_for_condition,
                                     wait_until_server_available)
from ray._private.job_manager import JobStatus
from ray.dashboard.modules.job.sdk import JobSubmissionClient

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
        stdout, stderr = client.get_job_logs(job_id)
        raise RuntimeError(f"Job failed\nstdout:\n{stdout}\nstderr:\n{stderr}")
    return status == JobStatus.SUCCEEDED


@pytest.fixture(
    scope="function",
    params=["no_working_dir", "local_working_dir", "s3_working_dir"])
def working_dir_option(request):
    if request.param == "no_working_dir":
        yield {
            "runtime_env": {},
            "entrypoint": "echo hello",
            "expected_stdout": "hello",
            "expected_stderr": ""
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
                "expected_stdout": "Hello from test_module!",
                "expected_stderr": ""
            }
    elif request.param == "s3_working_dir":
        yield {
            "runtime_env": {
                "working_dir": "s3://runtime-env-test/script.zip",
            },
            "entrypoint": "python script.py",
            "expected_stdout": "Executing main() from script.py !!",
            "expected_stderr": ""
        }
    else:
        assert False, f"Unrecognized option: {request.param}."


def test_submit_job(job_sdk_client, working_dir_option):
    client = job_sdk_client

    job_id = client.submit_job(
        entrypoint=working_dir_option["entrypoint"],
        runtime_env=working_dir_option["runtime_env"])

    wait_for_condition(_check_job_succeeded, client=client, job_id=job_id)

    stdout, stderr = client.get_job_logs(job_id)
    assert stdout == working_dir_option["expected_stdout"]
    assert stderr == working_dir_option["expected_stderr"]


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

    stdout, stderr = client.get_job_logs(job_id)
    assert stdout == str({
        "job_submission_id": job_id,
        "key1": "val1",
        "key2": "val2"
    })


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
