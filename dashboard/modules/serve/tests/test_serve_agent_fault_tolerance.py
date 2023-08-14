import requests
import pytest
import ray
import sys
from ray import serve
from ray.tests.conftest import *  # noqa: F401,F403
from ray._private.test_utils import generate_system_config_map

DEPLOYMENTS_URL = "http://localhost:52365/api/serve/deployments/"
STATUS_URL = "http://localhost:52365/api/serve/deployments/status"


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_failover_worker_reconnect_timeout=20,
                gcs_rpc_server_reconnect_timeout_s=3600,
                gcs_server_request_timeout_seconds=3,
            ),
        }
    ],
    indirect=True,
)
def test_deployments_get_tolerane(monkeypatch, ray_start_regular_with_external_redis):
    # test serve agent's availability when gcs is down
    monkeypatch.setenv("RAY_SERVE_KV_TIMEOUT_S", "3")
    serve.start(detached=True)

    get_response = requests.get(DEPLOYMENTS_URL, timeout=15)
    assert get_response.status_code == 200
    ray._private.worker._global_node.kill_gcs_server()

    get_response = requests.get(DEPLOYMENTS_URL, timeout=30)
    assert get_response.status_code == 503


@pytest.mark.skipif(sys.platform == "darwin", reason="Flaky on OSX.")
@pytest.mark.parametrize(
    "ray_start_regular_with_external_redis",
    [
        {
            **generate_system_config_map(
                gcs_failover_worker_reconnect_timeout=20,
                gcs_rpc_server_reconnect_timeout_s=3600,
                gcs_server_request_timeout_seconds=1,
            ),
        }
    ],
    indirect=True,
)
def test_status_url_get_tolerane(monkeypatch, ray_start_regular_with_external_redis):
    # test serve agent's availability when gcs is down
    monkeypatch.setenv("RAY_SERVE_KV_TIMEOUT_S", "3")
    serve.start(detached=True)
    get_response = requests.get(STATUS_URL, timeout=15)
    assert get_response.status_code == 200

    ray._private.worker._global_node.kill_gcs_server()

    get_response = requests.get(STATUS_URL, timeout=30)
    assert get_response.status_code == 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-vs", "--forked", __file__]))
