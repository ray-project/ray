from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import getpass
import os


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
