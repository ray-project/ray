import os
import sys
import time

import pytest

from ray._common.tls_utils import (
    _ReloadableServerCertConfig,
    generate_self_signed_tls_certs,
)


def _write_cert_key_ca(tmp_path, suffix=""):
    cert_contents, key_contents = generate_self_signed_tls_certs()
    cert_path = tmp_path / f"server{suffix}.crt"
    key_path = tmp_path / f"server{suffix}.key"
    # Reuse the self-signed cert as its own CA for test purposes.
    cert_path.write_text(cert_contents)
    key_path.write_text(key_contents)
    return str(cert_path), str(key_path), str(cert_path)


def _bump_mtime(*paths, delta_seconds=10):
    """Sets an mtime clearly in the future so filesystem timestamp
    resolution can't make a real content change look unchanged."""
    new_time = time.time() + delta_seconds
    for path in paths:
        os.utime(path, (new_time, new_time))


def test_generate_self_signed_tls_certs_returns_tuple():
    cert_contents, key_contents = generate_self_signed_tls_certs()
    assert isinstance(cert_contents, str)
    assert isinstance(key_contents, str)


def test_generate_self_signed_tls_certs_pem_format():
    cert_contents, key_contents = generate_self_signed_tls_certs()
    assert cert_contents.strip().startswith("-----BEGIN CERTIFICATE-----")
    assert cert_contents.strip().endswith("-----END CERTIFICATE-----")
    assert key_contents.strip().startswith("-----BEGIN")
    assert "PRIVATE KEY" in key_contents


def test_generate_self_signed_tls_certs_usable_for_ssl():
    import ssl
    import tempfile

    cert_contents, key_contents = generate_self_signed_tls_certs()
    with (
        tempfile.NamedTemporaryFile(mode="w", suffix=".crt") as cf,
        tempfile.NamedTemporaryFile(mode="w", suffix=".key") as kf,
    ):
        cf.write(cert_contents)
        cf.flush()
        kf.write(key_contents)
        kf.flush()

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cf.name, kf.name)


def test_reloadable_server_cert_config_caches_when_unchanged(tmp_path, monkeypatch):
    import grpc

    cert_path, key_path, ca_path = _write_cert_key_ca(tmp_path)

    build_calls = []

    def fake_build(*args, **kwargs):
        build_calls.append((args, kwargs))
        return object()

    monkeypatch.setattr(grpc, "ssl_server_certificate_configuration", fake_build)

    reloader = _ReloadableServerCertConfig(cert_path, key_path, ca_path)
    assert len(build_calls) == 1  # initial load in __init__

    first_config = reloader.fetch()
    second_config = reloader.fetch()

    assert first_config is second_config
    assert len(build_calls) == 1  # no re-read/re-build since nothing changed


def test_reloadable_server_cert_config_reloads_on_change(tmp_path, monkeypatch):
    import grpc

    cert_path, key_path, ca_path = _write_cert_key_ca(tmp_path)

    build_calls = []

    def fake_build(*args, **kwargs):
        config = object()
        build_calls.append(config)
        return config

    monkeypatch.setattr(grpc, "ssl_server_certificate_configuration", fake_build)

    reloader = _ReloadableServerCertConfig(cert_path, key_path, ca_path)
    first_config = reloader.fetch()

    # Simulate cert-manager rotating the cert/key files in place.
    new_cert_contents, new_key_contents = generate_self_signed_tls_certs()
    with open(cert_path, "w") as f:
        f.write(new_cert_contents)
    with open(key_path, "w") as f:
        f.write(new_key_contents)
    _bump_mtime(cert_path, key_path, ca_path)

    second_config = reloader.fetch()

    assert second_config is not first_config
    assert len(build_calls) == 2


def test_reloadable_server_cert_config_falls_back_on_reload_error(
    tmp_path, monkeypatch
):
    import grpc

    cert_path, key_path, ca_path = _write_cert_key_ca(tmp_path)

    reloader = _ReloadableServerCertConfig(cert_path, key_path, ca_path)
    first_config = reloader.fetch()
    assert first_config is not None

    # Bump the mtime to trigger a reload attempt, but make the rebuild fail
    # (e.g. as if cert-manager left the file in a bad/partial state).
    _bump_mtime(cert_path, key_path, ca_path)

    def boom(*args, **kwargs):
        raise RuntimeError("boom: simulated bad certificate content")

    monkeypatch.setattr(grpc, "ssl_server_certificate_configuration", boom)

    second_config = reloader.fetch()

    # Falls back to the last known-good config instead of raising.
    assert second_config is first_config


def test_reloadable_server_cert_config_falls_back_on_missing_files(tmp_path):
    cert_path, key_path, ca_path = _write_cert_key_ca(tmp_path)

    reloader = _ReloadableServerCertConfig(cert_path, key_path, ca_path)
    first_config = reloader.fetch()
    assert first_config is not None

    os.remove(cert_path)

    second_config = reloader.fetch()

    # Falls back to the last known-good config instead of raising.
    assert second_config is first_config


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
