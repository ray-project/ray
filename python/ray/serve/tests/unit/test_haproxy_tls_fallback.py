import sys
from unittest import mock

import pytest

from ray.serve._private.api import _fallback_to_python_proxy_for_tls
from ray.serve.config import ControllerOptions, HTTPOptions


def test_tls_falls_back_to_python_proxy():
    http_options = HTTPOptions(
        host="10.0.0.1", ssl_keyfile="key.pem", ssl_certfile="cert.pem"
    )
    controller_options = ControllerOptions(
        runtime_env={
            "env_vars": {
                "EXISTING": "value",
                "RAY_SERVE_ENABLE_HA_PROXY": "1",
            }
        }
    )

    resolved_http, resolved_controller = _fallback_to_python_proxy_for_tls(
        http_options, controller_options
    )

    assert resolved_http is http_options
    assert resolved_controller.runtime_env == {
        "env_vars": {
            "EXISTING": "value",
            "RAY_SERVE_ENABLE_HA_PROXY": "0",
        }
    }
    # Do not mutate the caller's configuration objects.
    assert (
        controller_options.runtime_env["env_vars"]["RAY_SERVE_ENABLE_HA_PROXY"] == "1"
    )


def test_tls_fallback_restores_python_proxy_default_host():
    http_options = HTTPOptions(ssl_keyfile="key.pem", ssl_certfile="cert.pem")

    with mock.patch(
        "ray.serve._private.api.RAY_SERVE_ENABLE_HA_PROXY", True
    ), mock.patch("ray.serve._private.api.get_localhost_ip", return_value="127.0.0.9"):
        resolved_http, _ = _fallback_to_python_proxy_for_tls(
            http_options, ControllerOptions()
        )

    assert resolved_http.host == "127.0.0.9"


def test_tls_fallback_preserves_explicit_host():
    http_options = HTTPOptions(
        host="10.0.0.1", ssl_keyfile="key.pem", ssl_certfile="cert.pem"
    )

    with mock.patch("ray.serve._private.api.RAY_SERVE_ENABLE_HA_PROXY", True):
        resolved_http, _ = _fallback_to_python_proxy_for_tls(
            http_options, ControllerOptions()
        )

    assert resolved_http.host == "10.0.0.1"


def test_without_tls_is_noop():
    http_options = HTTPOptions(host="10.0.0.1")
    controller_options = ControllerOptions(
        runtime_env={"env_vars": {"RAY_SERVE_ENABLE_HA_PROXY": "1"}}
    )

    resolved_http, resolved_controller = _fallback_to_python_proxy_for_tls(
        http_options, controller_options
    )

    assert resolved_http is http_options
    assert resolved_controller is controller_options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
