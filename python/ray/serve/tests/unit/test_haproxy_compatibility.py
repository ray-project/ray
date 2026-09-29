from unittest import mock

from ray.serve._private.api import _apply_haproxy_compatibility
from ray.serve.config import ControllerOptions, HTTPOptions


def test_tls_disables_haproxy_and_preserves_controller_env():
    http_options = HTTPOptions(ssl_keyfile="key.pem", ssl_certfile="cert.pem")
    controller_options = ControllerOptions(
        runtime_env={
            "env_vars": {
                "EXISTING": "value",
                "RAY_SERVE_ENABLE_HA_PROXY": "1",
            }
        }
    )

    with mock.patch(
        "ray.serve._private.api.RAY_SERVE_ENABLE_HA_PROXY", True
    ), mock.patch("ray.serve._private.api.get_localhost_ip", return_value="127.0.0.9"):
        resolved_http, resolved_controller = _apply_haproxy_compatibility(
            http_options, controller_options
        )

    assert resolved_http.host == "127.0.0.9"
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


def test_tls_preserves_explicit_http_host():
    http_options = HTTPOptions(
        host="10.0.0.1", ssl_keyfile="key.pem", ssl_certfile="cert.pem"
    )

    with mock.patch("ray.serve._private.api.RAY_SERVE_ENABLE_HA_PROXY", True):
        resolved_http, resolved_controller = _apply_haproxy_compatibility(
            http_options, ControllerOptions()
        )

    assert resolved_http.host == "10.0.0.1"
    assert resolved_controller.runtime_env == {
        "env_vars": {"RAY_SERVE_ENABLE_HA_PROXY": "0"}
    }


def test_plain_http_does_not_change_options():
    http_options = HTTPOptions(host="10.0.0.1")
    controller_options = ControllerOptions(
        runtime_env={"env_vars": {"RAY_SERVE_ENABLE_HA_PROXY": "1"}}
    )

    resolved_http, resolved_controller = _apply_haproxy_compatibility(
        http_options, controller_options
    )

    assert resolved_http is http_options
    assert resolved_controller is controller_options
