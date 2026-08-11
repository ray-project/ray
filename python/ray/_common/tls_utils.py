"""TLS utilities shared across Ray libraries (e.g. Serve)."""

import datetime
import logging
import os
import socket
import threading
from typing import Optional, Tuple

from ray._common.network_utils import (
    get_localhost_ip,
    node_ip_address_from_perspective,
)

logger = logging.getLogger(__name__)


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


class _ReloadableServerCertConfig:
    """Watches the TLS cert/key/CA files on disk by mtime and rebuilds the
    gRPC server certificate configuration only when they change.

    This is used as the fetcher callback for
    ``grpc.dynamic_ssl_server_credentials``, which gRPC invokes before
    establishing new TLS handshakes. This lets a long-running server pick
    up certificates rotated on disk (e.g. by cert-manager) without a
    restart. Any read/parse error while reloading falls back to the last
    known-good configuration instead of raising, since raising out of the
    fetcher would break the server's ability to accept new connections.
    """

    def __init__(self, cert_path: str, key_path: str, ca_path: str):
        self._cert_path = cert_path
        self._key_path = key_path
        self._ca_path = ca_path
        self._lock = threading.Lock()
        self._mtimes: Optional[Tuple[float, ...]] = None
        self._cached_config = None
        # Fail loudly if the initial load doesn't work, matching the
        # existing behavior of load_certs_from_env().
        self._reload_locked()

    def _stat_mtimes(self) -> Tuple[float, ...]:
        return tuple(
            os.stat(p).st_mtime_ns
            for p in (self._cert_path, self._key_path, self._ca_path)
        )

    def _reload_locked(self) -> None:
        import grpc

        with open(self._cert_path, "rb") as f:
            cert_chain = f.read()
        with open(self._key_path, "rb") as f:
            private_key = f.read()
        with open(self._ca_path, "rb") as f:
            ca_cert = f.read()

        self._cached_config = grpc.ssl_server_certificate_configuration(
            [(private_key, cert_chain)], root_certificates=ca_cert
        )

    def fetch(self):
        """Callback passed to grpc.dynamic_ssl_server_credentials."""
        with self._lock:
            try:
                mtimes = self._stat_mtimes()
            except OSError as e:
                logger.warning(
                    "Failed to stat TLS cert files, reusing cached "
                    "certificate configuration: %s",
                    e,
                )
                return self._cached_config

            if mtimes != self._mtimes:
                try:
                    self._reload_locked()
                    self._mtimes = mtimes
                except Exception as e:
                    logger.warning(
                        "Failed to reload TLS cert files, reusing cached "
                        "certificate configuration: %s",
                        e,
                    )

            return self._cached_config


def add_port_to_grpc_server(server, address):
    import grpc

    if os.environ.get("RAY_USE_TLS", "0").lower() in ("1", "true"):
        # Validates that RAY_TLS_SERVER_CERT, RAY_TLS_SERVER_KEY and
        # RAY_TLS_CA_CERT are all set, the same way the reloader will need.
        load_certs_from_env()
        reloader = _ReloadableServerCertConfig(
            os.environ["RAY_TLS_SERVER_CERT"],
            os.environ["RAY_TLS_SERVER_KEY"],
            os.environ["RAY_TLS_CA_CERT"],
        )
        credentials = grpc.dynamic_ssl_server_credentials(
            reloader.fetch, require_client_authentication=True
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
