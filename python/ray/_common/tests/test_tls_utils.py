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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
