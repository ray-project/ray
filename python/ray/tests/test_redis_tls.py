import pytest
import subprocess
import sys
import ray
from ray._private.test_utils import enable_external_redis


@pytest.fixture
def setup_tls(tmp_path, monkeypatch):
    shell_scripts = f"""
mkdir -p {str(tmp_path)}/tls
openssl genrsa -out {str(tmp_path)}/tls/ca.key 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key {str(tmp_path)}/tls/ca.key \
    -days 3650 \
    -subj '/O=Redis Test/CN=Certificate Authority' \
    -out {str(tmp_path)}/tls/ca.crt
openssl genrsa -out {str(tmp_path)}/tls/redis.key 2048
openssl req \
    -new -sha256 \
    -key {str(tmp_path)}/tls/redis.key \
    -subj '/O=Redis Test/CN=Server' | \
    openssl x509 \
        -req -sha256 \
        -CA {str(tmp_path)}/tls/ca.crt \
        -CAkey {str(tmp_path)}/tls/ca.key \
        -CAserial {str(tmp_path)}/tls/ca.txt \
        -CAcreateserial \
        -days 365 \
        -out {str(tmp_path)}/tls/redis.crt
openssl dhparam -out {str(tmp_path)}/tls/redis.dh 2048
"""
    (tmp_path / "gen-test-certs.sh").write_text(shell_scripts)

    print(subprocess.check_output(["bash", f"{tmp_path}/gen-test-certs.sh"]))
    """
    ls {tmp_path}/tls/
    ca.crt  ca.key  ca.txt  redis.crt  redis.dh  redis.key
    """

    monkeypatch.setenv("RAY_REDIS_CA_CERT", f"{str(tmp_path)}/tls/ca.crt")
    monkeypatch.setenv("RAY_REDIS_CLIENT_CERT", f"{str(tmp_path)}/tls/redis.crt")
    monkeypatch.setenv("RAY_REDIS_CLIENT_KEY", f"{str(tmp_path)}/tls/redis.key")
    ray._raylet.Config.initialize("")
    yield tmp_path


@pytest.mark.skipif(not enable_external_redis(), reason="Only work for redis mode")
@pytest.mark.skipif(sys.platform != "linux", reason="Only work in linux")
def test_redis_tls(setup_tls, ray_start_cluster_head):
    @ray.remote
    def hello():
        return "world"

    assert ray.get(hello.remote()) == "world"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
