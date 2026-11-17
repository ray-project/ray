import sys
from unittest import mock

import pytest

from ray._common import tls_utils


@pytest.mark.parametrize(
    "env,expected",
    [
        (None, True),
        ("1", True),
        ("true", True),
        ("0", False),
        ("false", False),
        ("False", False),
    ],
)
def test_require_client_auth_flag(monkeypatch, env, expected):
    monkeypatch.setenv("RAY_USE_TLS", "1")
    if env is None:
        monkeypatch.delenv("RAY_TLS_REQUIRE_CLIENT_AUTH", raising=False)
    else:
        monkeypatch.setenv("RAY_TLS_REQUIRE_CLIENT_AUTH", env)
    fake_grpc = mock.MagicMock()
    server = mock.MagicMock()
    with mock.patch.dict(sys.modules, {"grpc": fake_grpc}), mock.patch.object(
        tls_utils, "load_certs_from_env", return_value=(b"cert", b"key", b"ca")
    ):
        tls_utils.add_port_to_grpc_server(server, "0.0.0.0:1234")
    kwargs = fake_grpc.ssl_server_credentials.call_args.kwargs
    assert kwargs["require_client_auth"] is expected
    assert kwargs["root_certificates"] == b"ca"
    server.add_secure_port.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
