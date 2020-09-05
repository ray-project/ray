import getpass
import os


def is_ray_cluster():
    """Checks if the bootstrap config file exists.

    This will always exist if using an autoscaling cluster/started
    with the ray cluster launcher.
    """
    return os.path.exists(os.path.expanduser("~/ray_bootstrap_config.yaml"))


def get_ssh_user():
    """Returns ssh username for connecting to cluster workers."""

    return getpass.getuser()


def get_ssh_key():
    """Returns ssh key to connecting to cluster workers.

    If the env var TUNE_CLUSTER_SSH_KEY is provided, then this key
    will be used for syncing across different nodes.
    """
    path = os.environ.get("TUNE_CLUSTER_SSH_KEY",
                          os.path.expanduser("~/ray_bootstrap_key.pem"))
    if os.path.exists(path):
        return path
    return None
