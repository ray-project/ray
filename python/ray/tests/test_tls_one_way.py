import os
import sys
import tempfile

import pytest

from ray._common.test_utils import run_string_as_driver
from ray._common.tls_utils import generate_self_signed_tls_certs


def build_env():
    env = os.environ.copy()
    if sys.platform == "win32" and "SYSTEMROOT" not in env:
        env["SYSTEMROOT"] = r"C:\Windows"
    return env


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason=("Cryptography (TLS dependency) doesn't install in Mac build pipeline"),
)
def test_one_way_tls():
    cert, key = generate_self_signed_tls_certs()
    with tempfile.TemporaryDirectory() as temp_dir:
        cert_filepath = os.path.join(temp_dir, "server.crt")
        key_filepath = os.path.join(temp_dir, "server.key")
        with open(cert_filepath, "w") as fh:
            fh.write(cert)
        with open(key_filepath, "w") as fh:
            fh.write(key)

        # Set up environment for one-way TLS
        # Server will use these certs, but won't verify client.
        env = build_env()
        env["RAY_USE_TLS"] = "1"
        env["RAY_TLS_SERVER_CERT"] = cert_filepath
        env["RAY_TLS_SERVER_KEY"] = key_filepath
        env["RAY_TLS_CA_CERT"] = cert_filepath
        env["RAY_TLS_REQUIRE_CLIENT_AUTH"] = "0"

        # Client doesn't need to specify SERVER_CERT and SERVER_KEY if it doesn't want to send them.
        # But for ray.init() in the same process (or its children), it will pick them up from env if present.
        # So we'll run a driver that explicitly unsets them for the client.

        driver_script = """
import ray
import os

# Unset client certs to simulate a client that doesn't have them
if "RAY_TLS_SERVER_CERT" in os.environ:
    del os.environ["RAY_TLS_SERVER_CERT"]
if "RAY_TLS_SERVER_KEY" in os.environ:
    del os.environ["RAY_TLS_SERVER_KEY"]

try:
    ray.init()
    assert ray.is_initialized()
    @ray.remote
    def f(x):
        return x + 1
    assert ray.get(f.remote(1)) == 2
finally:
    ray.shutdown()
"""
        run_string_as_driver(driver_script, env=env)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
