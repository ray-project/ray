import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from unittest.mock import Mock, patch

import pytest

import ray.experimental.internal_kv as kv
from ray._common.test_utils import wait_for_condition
from ray._private.ray_constants import (
    KV_NAMESPACE_DASHBOARD,
)
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)
from ray._raylet import GcsClient
from ray.dashboard.consts import (
    DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
    GCS_RPC_TIMEOUT_SECONDS,
    RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR,
)
from ray.dashboard.modules.dashboard_sdk import (
    DEFAULT_DASHBOARD_ADDRESS,
    ClusterInfo,
    parse_cluster_info,
)
from ray.dashboard.modules.job.pydantic_models import JobType
from ray.dashboard.modules.job.sdk import JobStatus, JobSubmissionClient
from ray.dashboard.tests.conftest import *  # noqa
from ray.runtime_env.runtime_env import RuntimeEnv
from ray.tests.conftest import _ray_start
from ray.util.state import list_nodes


def _check_job_succeeded(client: JobSubmissionClient, job_id: str) -> bool:
    status = client.get_job_status(job_id)
    if status == JobStatus.FAILED:
        logs = client.get_job_logs(job_id)
        raise RuntimeError(f"Job failed\nlogs:\n{logs}")
    return status == JobStatus.SUCCEEDED


def check_internal_kv_gced():
    return len(kv._internal_kv_list("gcs://")) == 0


@pytest.mark.parametrize(
    "address_param",
    [
        ("ray://1.2.3.4:10001", "ray", "1.2.3.4:10001"),
        ("other_module://", "other_module", ""),
        ("other_module://address", "other_module", "address"),
    ],
)
@pytest.mark.parametrize("create_cluster_if_needed", [True, False])
@pytest.mark.parametrize("cookies", [None, {"test_cookie_key": "test_cookie_val"}])
@pytest.mark.parametrize("metadata", [None, {"test_metadata_key": "test_metadata_val"}])
@pytest.mark.parametrize("headers", [None, {"test_headers_key": "test_headers_val"}])
def test_parse_cluster_info(
    address_param: Tuple[str, str, str],
    create_cluster_if_needed: bool,
    cookies: Optional[Dict[str, str]],
    metadata: Optional[Dict[str, str]],
    headers: Optional[Dict[str, str]],
):
    """
    Test ray.dashboard.modules.dashboard_sdk.parse_cluster_info for different
    format of addresses.
    """
    mock_get_job_submission_client_cluster = Mock(return_value="Ray ClusterInfo")
    mock_module = Mock()
    mock_module.get_job_submission_client_cluster_info = Mock(
        return_value="Other module ClusterInfo"
    )
    mock_import_module = Mock(return_value=mock_module)

    address, module_string, inner_address = address_param

    with patch.multiple(
        "ray.dashboard.modules.dashboard_sdk",
        get_job_submission_client_cluster_info=mock_get_job_submission_client_cluster,
    ), patch.multiple("importlib", import_module=mock_import_module):
        if module_string == "ray":
            with pytest.raises(ValueError, match="ray://"):
                parse_cluster_info(
                    address,
                    create_cluster_if_needed=create_cluster_if_needed,
                    cookies=cookies,
                    metadata=metadata,
                    headers=headers,
                )
        elif module_string == "other_module":
            assert (
                parse_cluster_info(
                    address,
                    create_cluster_if_needed=create_cluster_if_needed,
                    cookies=cookies,
                    metadata=metadata,
                    headers=headers,
                )
                == "Other module ClusterInfo"
            )
            mock_import_module.assert_called_once_with(module_string)
            mock_module.get_job_submission_client_cluster_info.assert_called_once_with(
                inner_address,
                create_cluster_if_needed=create_cluster_if_needed,
                cookies=cookies,
                metadata=metadata,
                headers=headers,
            )


def test_parse_cluster_info_default_address():
    assert parse_cluster_info(
        address=None,
    ) == ClusterInfo(address=DEFAULT_DASHBOARD_ADDRESS)


@pytest.mark.parametrize("expiration_s", [0, 10])
def test_temporary_uri_reference(monkeypatch, expiration_s):
    """Test that temporary GCS URI references are deleted after expiration_s."""
    monkeypatch.setenv(
        "RAY_RUNTIME_ENV_TEMPORARY_REFERENCE_EXPIRATION_S", str(expiration_s)
    )
    # We can't use a fixture with a shared Ray runtime because we need to set the
    # expiration_s env var before Ray starts.
    with _ray_start(include_dashboard=True, num_cpus=1) as ctx:
        headers = {"Connection": "keep-alive", "Authorization": "TOK:<MY_TOKEN>"}
        address = ctx.address_info["webui_url"]
        assert wait_until_server_available(address)
        client = JobSubmissionClient(format_web_url(address), headers=headers)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir)

            hello_file = path / "hi.txt"
            with hello_file.open(mode="w") as f:
                f.write("hi\n")

            start = time.time()

            client.submit_job(
                entrypoint="echo hi", runtime_env={"working_dir": tmp_dir}
            )

            # Give time for deletion to occur if expiration_s is 0.
            time.sleep(2)
            # Need to connect to Ray to check internal_kv.
            # ray.init(address="auto")

            print("Starting Internal KV checks at time ", time.time() - start)
            if expiration_s > 0:
                assert not check_internal_kv_gced()
                wait_for_condition(check_internal_kv_gced, timeout=2 * expiration_s)
                assert expiration_s < time.time() - start < 2 * expiration_s
                print("Internal KV was GC'ed at time ", time.time() - start)
            else:
                wait_for_condition(check_internal_kv_gced)
                print("Internal KV was GC'ed at time ", time.time() - start)


def get_register_agents_number(gcs_client):
    keys = gcs_client.internal_kv_keys(
        prefix=DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX,
        namespace=KV_NAMESPACE_DASHBOARD,
        timeout=GCS_RPC_TIMEOUT_SECONDS,
    )
    return len(keys)


@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "include_dashboard": True,
            "env_vars": {RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR: "1"},
        },
        {
            "include_dashboard": True,
            "env_vars": {RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR: "0"},
        },
    ],
    indirect=True,
)
def test_jobs_run_on_head_by_default_E2E(ray_start_cluster_head_with_env_vars):
    allow_driver_on_worker_nodes = (
        os.environ.get(RAY_JOB_ALLOW_DRIVER_ON_WORKER_NODES_ENV_VAR) == "1"
    )
    # Cluster setup
    cluster = ray_start_cluster_head_with_env_vars
    cluster.add_node(dashboard_agent_listen_port=52366)
    cluster.add_node(dashboard_agent_listen_port=52367)
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    client = JobSubmissionClient(webui_url)
    gcs_client = GcsClient(address=cluster.gcs_address)

    def _check_nodes(num_nodes):
        try:
            assert len(list_nodes()) == num_nodes
            return True
        except Exception as ex:
            print(ex)
            return False

    wait_for_condition(lambda: _check_nodes(num_nodes=3), timeout=15)
    wait_for_condition(lambda: get_register_agents_number(gcs_client) == 3, timeout=20)

    # Submit 20 simple jobs.
    for i in range(20):
        client.submit_job(entrypoint="echo hi", submission_id=f"job_{i}")
    import pprint

    def check_all_jobs_succeeded():
        submission_jobs = [
            job for job in client.list_jobs() if job.type == JobType.SUBMISSION
        ]
        for job in submission_jobs:
            pprint.pprint(job)
            if job.status != JobStatus.SUCCEEDED:
                return False
        return True

    # Wait until all jobs have finished.
    wait_for_condition(check_all_jobs_succeeded, timeout=60, retry_interval_ms=1000)

    # Check driver_node_id of all jobs.
    submission_jobs = [
        job for job in client.list_jobs() if job.type == JobType.SUBMISSION
    ]
    driver_node_ids = [job.driver_node_id for job in submission_jobs]

    # Spuriously fails with probability (1/3)^20.
    pprint.pprint(driver_node_ids)
    num_ids = len(set(driver_node_ids))
    assert (num_ids > 1) if allow_driver_on_worker_nodes else (num_ids == 1), [
        id[:5] for id in driver_node_ids
    ]


@pytest.fixture
def runtime_env_working_dir():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        working_dir = path / "working_dir"
        working_dir.mkdir(parents=True)
        yield working_dir


@pytest.fixture
def py_module_whl():
    with tempfile.NamedTemporaryFile(suffix=".whl") as tmp_file:
        yield tmp_file.name


def test_job_submission_with_runtime_env_as_dict(
    runtime_env_working_dir, py_module_whl
):
    working_dir_str = str(runtime_env_working_dir)
    with _ray_start(num_cpus=1):
        client = JobSubmissionClient()
        runtime_env = {"working_dir": working_dir_str, "py_modules": [py_module_whl]}
        job_id = client.submit_job(entrypoint="echo hi", runtime_env=runtime_env)
        job_details = client.get_job_info(job_id)
        parsed_runtime_env = job_details.runtime_env
        assert "gcs://" in parsed_runtime_env["working_dir"]
        assert len(parsed_runtime_env["py_modules"]) == 1
        assert "gcs://" in parsed_runtime_env["py_modules"][0]


def test_job_submission_with_runtime_env_as_object(
    runtime_env_working_dir, py_module_whl
):
    working_dir_str = str(runtime_env_working_dir)
    with _ray_start(num_cpus=1):
        client = JobSubmissionClient()
        runtime_env = RuntimeEnv(
            working_dir=working_dir_str, py_modules=[py_module_whl]
        )
        job_id = client.submit_job(entrypoint="echo hi", runtime_env=runtime_env)
        job_details = client.get_job_info(job_id)
        parsed_runtime_env = job_details.runtime_env
        assert "gcs://" in parsed_runtime_env["working_dir"]
        assert len(parsed_runtime_env["py_modules"]) == 1
        assert "gcs://" in parsed_runtime_env["py_modules"][0]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
