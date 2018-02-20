from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import getpass
import os


def get_ssh_user():
    """Returns ssh username for connecting to cluster workers."""

    return getpass.getuser()


def get_ssh_key():
    """Returns ssh key to connecting to cluster workers."""

    path = os.path.expanduser("~/ray_bootstrap_key.pem")
    if os.path.exists(path):
        return path
    return None
