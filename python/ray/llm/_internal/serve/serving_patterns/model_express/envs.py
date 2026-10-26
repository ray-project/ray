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


def get_mx_server_port() -> int:
    """gRPC port of the MX server.

    The server listens on it and advertises it to LLM replicas, so set
    RAY_MX_SERVER_PORT cluster-wide when overriding it.

    Returns:
        The port.
    """
    return int(os.environ.get("RAY_MX_SERVER_PORT", "8931"))
