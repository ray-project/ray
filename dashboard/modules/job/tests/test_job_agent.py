import asyncio
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import pytest
from ray.runtime_env.runtime_env import RuntimeEnv
import yaml

from ray._private.runtime_env.packaging import Protocol, parse_uri
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.test_utils import (
    chdir,
    format_web_url,
    wait_until_server_available,
)
from ray.dashboard.modules.job.common import JobSubmitRequest
from ray.dashboard.modules.job.utils import validate_request_type
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobStatus
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
        ip, port = ctx.address_info["webui_url"].split(":")
        agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
        assert wait_until_server_available(agent_address)
        yield JobAgentSubmissionClient(format_web_url(agent_address))


async def _check_job_succeeded(
    client: JobAgentSubmissionClient, job_id: str, timeout: int = 10
) -> bool:
    async def _check():
        result = await client.get_job_info(job_id)
        status = result.status
        return status == JobStatus.SUCCEEDED

    st = time.time()
    while time.time() <= timeout + st:
        res = await _check()
        if res:
            return True
        await asyncio.sleep(0.1)
    return False


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

    client = job_sdk_client

    need_upload = False
    working_dir = runtime_env_option["runtime_env"].get("working_dir", None)
    py_modules = runtime_env_option["runtime_env"].get("py_modules", [])

    def _need_upload(path):
        try:
            protocol, _ = parse_uri(path)
            if protocol == Protocol.GCS:
                return True
        except ValueError:
            # local file, need upload
            return True
        return False

    if working_dir:
        need_upload = need_upload or _need_upload(working_dir)
    if py_modules:
        need_upload = need_upload or any(
            [_need_upload(str(py_module)) for py_module in py_modules]
        )

    # TODO(Catch-Bull): delete this after we implemented
    # `upload package` and `get package`
    if need_upload:
        # not implemented `upload package` yet.
        print("Skip test, because of need upload")
        return

    runtime_env = RuntimeEnv(**runtime_env_option["runtime_env"]).to_dict()
    request = validate_request_type(
        {"runtime_env": runtime_env, "entrypoint": runtime_env_option["entrypoint"]},
        JobSubmitRequest,
    )

    submit_result = await client.submit_job_internal(request)
    job_id = submit_result.submission_id

    check_result = await _check_job_succeeded(client=client, job_id=job_id, timeout=120)
    assert check_result

    # TODO(Catch-Bull): delete this after we implemented
    # `get_job_logs`
    # not implemented `get_job_logs` yet.
    print("Skip test, because of need get job logs")
    return
    logs = client.get_job_logs(job_id)
    assert runtime_env_option["expected_logs"] in logs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
