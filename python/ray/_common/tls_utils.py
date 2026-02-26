"""TLS utilities shared across Ray libraries (e.g. Serve)."""

import datetime
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

    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "ray-internal")]
    )
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
