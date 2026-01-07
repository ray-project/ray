import datetime
import os
import socket

from ray._common.network_utils import (
    get_localhost_ip,
    node_ip_address_from_perspective,
)


def generate_self_signed_tls_certs():
    """Create self-signed key/cert pair for testing.

    This method requires the library ``cryptography`` be installed.
    """
    try:
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        raise ImportError(
            "Using `Security.temporary` requires `cryptography`, please "
            "install it using either pip or conda"
        )
    key = rsa.generate_private_key(
        public_exponent=65537, key_size=2048, backend=default_backend()
    )
    key_contents = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()

    ray_interal = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ray-internal")])
    altnames = x509.SubjectAlternativeName(
        [
            x509.DNSName(
                socket.gethostbyname(socket.gethostname())
            ),  # Probably 127.0.0.1 or ::1
            x509.DNSName(get_localhost_ip()),
            x509.DNSName(node_ip_address_from_perspective()),
            x509.DNSName("localhost"),
        ]
    )
    now = datetime.datetime.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(ray_interal)
        .issuer_name(ray_interal)
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
        server_cert_chain, private_key, ca_cert = load_certs_from_env()
        credentials = grpc.ssl_server_credentials(
            [(private_key, server_cert_chain)],
            root_certificates=ca_cert,
            require_client_auth=ca_cert is not None,
        )
        return server.add_secure_port(address, credentials)
    else:
        return server.add_insecure_port(address)


def load_certs_from_env():
    tls_env_vars = ["RAY_TLS_SERVER_CERT", "RAY_TLS_SERVER_KEY", "RAY_TLS_CA_CERT"]
    if any(v not in os.environ for v in tls_env_vars):
        raise RuntimeError(
            "If the environment variable RAY_USE_TLS is set to true "
            "then RAY_TLS_SERVER_CERT, RAY_TLS_SERVER_KEY and "
            "RAY_TLS_CA_CERT must also be set."
        )

    with open(os.environ["RAY_TLS_SERVER_CERT"], "rb") as f:
        server_cert_chain = f.read()
    with open(os.environ["RAY_TLS_SERVER_KEY"], "rb") as f:
        private_key = f.read()
    with open(os.environ["RAY_TLS_CA_CERT"], "rb") as f:
        ca_cert = f.read()

    return server_cert_chain, private_key, ca_cert
