import sys
import ray
import requests
import pytest
from ray._raylet import GcsClient


@pytest.mark.asyncio
async def test_get_cluster_id():
    assert ray._private.utils.check_dashboard_dependencies_installed()

    ray.init()

    data = requests.get("http://localhost:8265/cluster_id").json()

    assert data["result"] is True
    assert data["msg"] == "Fetched cluster id"
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)
    assert data["data"]["clusterId"] == gcs_client.cluster_id.hex()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
