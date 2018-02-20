from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os


def get_ssh_user():
    return "ubuntu"


def get_ssh_key():
    return os.path.expanduser("~/ray_bootstrap_key.pem")
