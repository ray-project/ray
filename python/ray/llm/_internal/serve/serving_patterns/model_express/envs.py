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
