"""Tests for ray._common.tls_utils."""

import sys

import pytest

from ray._common.tls_utils import generate_self_signed_tls_certs


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
    import tempfile

    import ssl

    cert_contents, key_contents = generate_self_signed_tls_certs()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".crt", delete=False) as cf:
        cf.write(cert_contents)
        cert_path = cf.name
    with tempfile.NamedTemporaryFile(mode="w", suffix=".key", delete=False) as kf:
        kf.write(key_contents)
        key_path = kf.name
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, key_path)
    finally:
        import os

        os.unlink(cert_path)
        os.unlink(key_path)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
