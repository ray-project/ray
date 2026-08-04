import sys

import pytest

import ray
from ray._private.test_utils import external_redis_test_enabled


@pytest.fixture
def setup_tls(monkeypatch):
    from pathlib import Path

    tls_path = Path(__file__).parent
    monkeypatch.setenv("RAY_REDIS_CA_CERT", f"{str(tls_path)}/tls/ca.crt")
    monkeypatch.setenv("RAY_REDIS_CLIENT_CERT", f"{str(tls_path)}/tls/redis.crt")
    monkeypatch.setenv("RAY_REDIS_CLIENT_KEY", f"{str(tls_path)}/tls/redis.key")
    ray._raylet.Config.initialize("")
    yield


@pytest.fixture
def setup_replicas(request, monkeypatch):
    monkeypatch.setenv("TEST_EXTERNAL_REDIS_REPLICAS", str(request.param))
    yield


@pytest.mark.skipif(
    not external_redis_test_enabled(), reason="Only work for redis mode"
)
@pytest.mark.skipif(sys.platform != "linux", reason="Only work in linux")
@pytest.mark.parametrize("setup_replicas", [1, 3], indirect=True)
def test_redis_tls(setup_tls, setup_replicas, ray_start_cluster_head):
    @ray.remote
    def hello():
        return "world"

    assert ray.get(hello.remote()) == "world"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
