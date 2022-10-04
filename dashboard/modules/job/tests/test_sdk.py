from pathlib import Path
import tempfile
import time
import os
import requests
import pytest
import sys
from typing import Dict, Optional, Tuple
from unittest.mock import Mock, patch
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)

from ray.dashboard.modules.dashboard_sdk import (
    ClusterInfo,
    DEFAULT_DASHBOARD_ADDRESS,
    parse_cluster_info,
)
from ray.dashboard.modules.job.sdk import JobSubmissionClient, JobStatus
from ray.dashboard.tests.conftest import *  # noqa
from ray.tests.conftest import _ray_start
import ray
import ray.experimental.internal_kv as kv


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
    assert (
        parse_cluster_info(
            address=None,
        )
        == ClusterInfo(address=DEFAULT_DASHBOARD_ADDRESS)
    )


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


@pytest.fixture
def mock_candidate_number():
    os.environ["CANDIDATE_AGENT_NUMBER"] = "2"
    yield
    os.environ.pop("CANDIDATE_AGENT_NUMBER", None)


@pytest.mark.parametrize(
    "ray_start_cluster_head", [{"include_dashboard": True}], indirect=True
)
def test_job_head_choose_job_agent_E2E(mock_candidate_number, ray_start_cluster_head):
    cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    client = JobSubmissionClient(webui_url)

    def get_register_agents_number():
        response = requests.get(webui_url + "/internal/node_module")
        response.raise_for_status()
        result = response.json()
        data = result["data"]
        return data["registeredAgents"]

    def submit_job_and_wait_finish():
        submission_id = client.submit_job(entrypoint="echo hello")

        wait_for_condition(_check_job_succeeded, client=client, job_id=submission_id)

    # make sure list(cluster.worker_nodes)[0] will be the owner of a supervisor actor.
    cluster.add_node(dashboard_agent_listen_port=52366)
    wait_for_condition(lambda: get_register_agents_number() == 2, timeout=20)
    assert len(cluster.worker_nodes) == 1
    node_try_to_kill = list(cluster.worker_nodes)[0]
    submit_job_and_wait_finish()

    cluster.add_node(dashboard_agent_listen_port=52367)
    wait_for_condition(lambda: get_register_agents_number() == 3, timeout=20)

    submit_job_and_wait_finish()
    submit_job_and_wait_finish()
    submit_job_and_wait_finish()

    def get_all_new_supervisor_actor_info(old_supervisor_actor):
        all_actors = ray.state.state.actor_table(None)
        res = dict()
        for actor_id, actor_info in all_actors.items():
            if actor_id in old_supervisor_actor:
                continue
            if not actor_info["Name"].startswith("_ray_internal_job_actor"):
                continue
            res[actor_id] = actor_info
        return res

    old_supervisor_actor = set()
    new_supervisor_actor = get_all_new_supervisor_actor_info(old_supervisor_actor)
    new_owner_port = set()
    for actor_id, actor_info in new_supervisor_actor.items():
        old_supervisor_actor.add(actor_id)
        new_owner_port.add(actor_info["OwnerAddress"]["Port"])

    assert len(new_owner_port) == 2
    old_owner_port = new_owner_port

    node_try_to_kill.kill_raylet()

    # make sure the head updates the info of the dead node.
    wait_for_condition(lambda: get_register_agents_number() == 2, timeout=20)

    submit_job_and_wait_finish()
    submit_job_and_wait_finish()
    submit_job_and_wait_finish()

    new_supervisor_actor = get_all_new_supervisor_actor_info(old_supervisor_actor)
    new_owner_port = set()
    for actor_id, actor_info in new_supervisor_actor.items():
        old_supervisor_actor.add(actor_id)
        new_owner_port.add(actor_info["OwnerAddress"]["Port"])
    assert len(new_owner_port) == 2
    assert len(old_owner_port - new_owner_port) == 1
    assert len(new_owner_port - old_owner_port) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
