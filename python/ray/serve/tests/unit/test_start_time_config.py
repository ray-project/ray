import sys
from types import SimpleNamespace

import pytest

from ray.serve._private.api import _check_start_time_config_unchanged
from ray.serve._private.grpc_util import set_proxy_default_grpc_options
from ray.serve._private.http_util import configure_http_options_with_defaults
from ray.serve.config import HTTPOptions, ProxyLocation, gRPCOptions
from ray.serve.exceptions import RayServeConfigException


def fake_client(
    http_options=None,
    grpc_options=None,
    proxy_location=ProxyLocation.EveryNode,
):
    """A client whose config went through the same defaulting as the controller's."""
    return SimpleNamespace(
        http_config=configure_http_options_with_defaults(http_options or HTTPOptions()),
        grpc_config=set_proxy_default_grpc_options(grpc_options or gRPCOptions()),
        proxy_location=proxy_location,
    )


def test_nothing_requested():
    client = fake_client()
    _check_start_time_config_unchanged(client)
    _check_start_time_config_unchanged(client, http_options={}, grpc_options={})
    _check_start_time_config_unchanged(client, http_options=None, proxy_location=None)


def test_matching_options():
    client = fake_client(HTTPOptions(host="0.0.0.0", port=8000))
    _check_start_time_config_unchanged(client, http_options={"host": "0.0.0.0"})
    _check_start_time_config_unchanged(
        client, http_options=HTTPOptions(host="0.0.0.0", port=8000)
    )
    _check_start_time_config_unchanged(client, proxy_location="EveryNode")


def test_changed_field_raises():
    client = fake_client(HTTPOptions(host="0.0.0.0", port=8000))
    with pytest.raises(RayServeConfigException) as exc:
        _check_start_time_config_unchanged(client, http_options={"port": 8001})

    message = str(exc.value)
    assert "http_options.port: 8000 -> 8001" in message
    assert "can't be updated at runtime" in message


def test_every_changed_field_is_reported():
    client = fake_client(HTTPOptions(host="0.0.0.0", port=8000))
    with pytest.raises(RayServeConfigException) as exc:
        _check_start_time_config_unchanged(
            client,
            http_options={"host": "127.0.0.1", "port": 8001},
            grpc_options={"port": 9001},
            proxy_location="HeadOnly",
        )

    message = str(exc.value)
    assert "http_options.host: '0.0.0.0' -> '127.0.0.1'" in message
    assert "http_options.port: 8000 -> 8001" in message
    assert "grpc_options.port: 9000 -> 9001" in message
    assert "proxy_location: 'EveryNode' -> 'HeadOnly'" in message


def test_unset_fields_are_not_a_change():
    """A partially-specified model must not report the fields it left alone."""
    client = fake_client(HTTPOptions(host="0.0.0.0", port=8001))
    _check_start_time_config_unchanged(client, http_options=HTTPOptions(port=8001))


def test_full_schema_dump_is_not_a_change():
    """The declarative path sends every field, including untouched defaults."""
    from ray.serve.schema import HTTPOptionsSchema

    schema = HTTPOptionsSchema()
    client = fake_client(HTTPOptions.model_validate(schema.model_dump()))
    _check_start_time_config_unchanged(client, http_options=schema.model_dump())


def test_env_var_default_is_not_a_change(monkeypatch):
    """The controller's env-var defaulting must not look like a requested change."""
    monkeypatch.setattr(
        "ray.serve._private.http_util.RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S", 42
    )
    client = fake_client(HTTPOptions(keep_alive_timeout_s=5))
    assert client.http_config.keep_alive_timeout_s == 42
    _check_start_time_config_unchanged(client, http_options={"keep_alive_timeout_s": 5})


def test_unknown_keys_ignored():
    """Serve drops unknown keys when starting, so they can't be a change either."""
    _check_start_time_config_unchanged(fake_client(), http_options={"prot": 8001})


def test_grpc_options():
    client = fake_client(grpc_options=gRPCOptions(port=9000))
    _check_start_time_config_unchanged(client, grpc_options={"port": 9000})
    with pytest.raises(
        RayServeConfigException, match=r"grpc_options\.port: 9000 -> 9001"
    ):
        _check_start_time_config_unchanged(client, grpc_options={"port": 9001})


def test_proxy_location():
    client = fake_client(proxy_location=ProxyLocation.HeadOnly)
    _check_start_time_config_unchanged(client, proxy_location=ProxyLocation.HeadOnly)
    with pytest.raises(RayServeConfigException, match="proxy_location"):
        _check_start_time_config_unchanged(client, proxy_location="EveryNode")


def test_deprecated_location_is_compared_against_resolved_placement():
    """`HTTPOptions.location` is an alias for `proxy_location` and wins over it."""
    client = fake_client(proxy_location=ProxyLocation.HeadOnly)
    _check_start_time_config_unchanged(client, http_options={"location": "HeadOnly"})
    _check_start_time_config_unchanged(
        client, http_options={"location": "HeadOnly"}, proxy_location="EveryNode"
    )
    with pytest.raises(RayServeConfigException, match="proxy_location"):
        _check_start_time_config_unchanged(
            client, http_options=HTTPOptions(location="EveryNode")
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
