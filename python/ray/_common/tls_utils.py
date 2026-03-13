"""TLS utilities shared across Ray libraries (e.g. Serve)."""

import datetime
import os
import socket
from typing import Tuple

from ray._common.network_utils import (
    get_localhost_ip,
    node_ip_address_from_perspective,
)


def generate_self_signed_tls_certs() -> Tuple[str, str]:
    """Create self-signed key/cert pair for testing.

    Returns:
        Tuple of (cert_pem_contents, key_pem_contents).

    Raises:
        ImportError: If the ``cryptography`` library is not installed.
    """
    try:
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError as e:
        raise ImportError(
            "Using self-signed TLS certs requires `cryptography`. "
            "Install it with: pip install cryptography"
        ) from e

    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    key_contents = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ray-internal")])
    altnames = x509.SubjectAlternativeName(
        [
            x509.DNSName(socket.gethostbyname(socket.gethostname())),
            x509.DNSName(get_localhost_ip()),
            x509.DNSName(node_ip_address_from_perspective()),
            x509.DNSName("localhost"),
        ]
    )
    now = datetime.datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .add_extension(altnames, critical=False)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256(), default_backend())
    )

    cert_contents = cert.public_bytes(serialization.Encoding.PEM).decode()
    return cert_contents, key_contents


def add_port_to_grpc_server(server, address):
    import grpc

    if os.environ.get("RAY_USE_TLS", "0").lower() in ("1", "true"):
        server_cert_chain, private_key, ca_cert = load_certs_from_env(is_server=True)
        if server_cert_chain is None or private_key is None:
            raise RuntimeError(
                "When RAY_USE_TLS is enabled for a server, RAY_TLS_SERVER_CERT and "
                "RAY_TLS_SERVER_KEY must be provided."
            )

        require_client_auth = os.environ.get(
            "RAY_TLS_REQUIRE_CLIENT_AUTH", "1"
        ).lower() in ("1", "true")
        credentials = grpc.ssl_server_credentials(
            [(private_key, server_cert_chain)],
            root_certificates=ca_cert,
            require_client_auth=require_client_auth and ca_cert is not None,
        )
        return server.add_secure_port(address, credentials)
    else:
        return server.add_insecure_port(address)


def load_certs_from_env(is_server: bool = False):
    # RAY_TLS_CA_CERT is always required for TLS
    if "RAY_TLS_CA_CERT" not in os.environ:
        raise RuntimeError(
            "If the environment variable RAY_USE_TLS is set to true "
            "then RAY_TLS_CA_CERT must also be set."
        )

    require_client_auth = os.environ.get(
        "RAY_TLS_REQUIRE_CLIENT_AUTH", "1"
    ).lower() in ("1", "true")

    # RAY_TLS_SERVER_CERT and RAY_TLS_SERVER_KEY are required if we are a server,
    # or if we are a client in mTLS mode.
    # For simplicity, we check them if they are provided, or if require_client_auth
    # is true or is_server is true.

    server_cert_chain = None
    if "RAY_TLS_SERVER_CERT" in os.environ:
        with open(os.environ["RAY_TLS_SERVER_CERT"], "rb") as f:
            server_cert_chain = f.read()
    elif require_client_auth or is_server:
        raise RuntimeError(
            "RAY_TLS_SERVER_CERT must be set if "
            "RAY_TLS_REQUIRE_CLIENT_AUTH is true or if it's a server."
        )

    private_key = None
    if "RAY_TLS_SERVER_KEY" in os.environ:
        with open(os.environ["RAY_TLS_SERVER_KEY"], "rb") as f:
            private_key = f.read()
    elif require_client_auth or is_server:
        raise RuntimeError(
            "RAY_TLS_SERVER_KEY must be set if "
            "RAY_TLS_REQUIRE_CLIENT_AUTH is true or if it's a server."
        )

    with open(os.environ["RAY_TLS_CA_CERT"], "rb") as f:
        ca_cert = f.read()

    return server_cert_chain, private_key, ca_cert
