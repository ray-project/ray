import os

# How often Serve health-checks the MX server replica.
MX_SERVER_HEALTH_CHECK_PERIOD_S = float(
    os.environ.get("RAY_MX_SERVER_HEALTH_CHECK_PERIOD_S", "10")
)


def get_mx_server_binary() -> str:
    """Path of the MX server binary.

    The ray-llm image installs it at /usr/local/bin/modelexpress-server
    (docker/ray-llm/Dockerfile); set RAY_MX_SERVER_BINARY_PATH for a custom image.

    Returns:
        The binary's path.
    """
    return os.environ.get(
        "RAY_MX_SERVER_BINARY_PATH", "/usr/local/bin/modelexpress-server"
    )


def get_mx_app_name() -> str:
    """Name of the Serve app that runs the MX server.

    LLM replicas look the server up in this app; set RAY_MX_APP_NAME when
    deploying it under another name.

    Returns:
        The app name.
    """
    return os.environ.get("RAY_MX_APP_NAME", "mx")


# MX binds these ports plus each worker's GPU index on every LLM worker node. Keep them
# above Ray's default worker port range (`ray start --min-worker-port 10002
# --max-worker-port 19999`), where any Ray worker process may already be listening, and
# below the Linux ephemeral range (32768+).
_DEFAULT_MX_METADATA_PORT_BASE = 21555
_DEFAULT_MX_WORKER_GRPC_PORT_BASE = 22555


def get_mx_metadata_port_base() -> int:
    """Base of the LLM replicas' MX NIXL metadata ports (RAY_MX_METADATA_PORT_BASE).

    Returns:
        The base port; MX adds each worker's GPU index.
    """
    return int(
        os.environ.get("RAY_MX_METADATA_PORT_BASE", _DEFAULT_MX_METADATA_PORT_BASE)
    )


def get_mx_worker_grpc_port_base() -> int:
    """Base of the LLM replicas' MX worker gRPC ports (RAY_MX_WORKER_GRPC_PORT_BASE).

    Returns:
        The base port; MX adds each worker's GPU index.
    """
    return int(
        os.environ.get(
            "RAY_MX_WORKER_GRPC_PORT_BASE", _DEFAULT_MX_WORKER_GRPC_PORT_BASE
        )
    )


def get_mx_server_port() -> int:
    """gRPC port of the MX server.

    The server listens on it and advertises it to LLM replicas, so set
    RAY_MX_SERVER_PORT cluster-wide when overriding it.

    Returns:
        The port.
    """
    return int(os.environ.get("RAY_MX_SERVER_PORT", "8931"))
