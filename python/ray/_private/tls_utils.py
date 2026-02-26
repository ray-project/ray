import os

from ray._common.tls_utils import (
    generate_self_signed_tls_certs,  # noqa: F401 (re-export)
)


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
