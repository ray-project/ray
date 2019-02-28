#!/usr/bin/env python
"""This script allows you to develop RLlib without needing to compile Ray."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import os
import subprocess

import ray


def do_link(package):
    package_home = os.path.abspath(
        os.path.join(ray.__file__, "../{}".format(package)))
    local_home = os.path.abspath(
        os.path.join(__file__, "../../{}".format(package)))
    assert os.path.isdir(package_home), package_home
    assert os.path.isdir(local_home), local_home
    if not click.confirm(
            "This will replace:\n  {}\nwith a symlink to:\n  {}".format(
                package_home, local_home),
            default=True):
        return
    if os.access(os.path.dirname(package_home), os.W_OK):
        subprocess.check_call(["rm", "-rf", package_home])
        subprocess.check_call(["ln", "-s", local_home, package_home])
    else:
        print("You don't have write permission to {}, using sudo:".format(
            package_home))
        subprocess.check_call(["sudo", "rm", "-rf", package_home])
        subprocess.check_call(["sudo", "ln", "-s", local_home, package_home])


if __name__ == "__main__":
    do_link("rllib")
    do_link("tune")
    do_link("autoscaler")
    print("Created links.\n\nIf you run into issues initializing Ray, please "
          "ensure that your local repo and the installed Ray are in sync "
          "(pip install -U the latest wheels at "
          "https://ray.readthedocs.io/en/latest/installation.html, "
          "and ensure you are up-to-date on the master branch on git).\n\n"
          "Note that you may need to delete the package symlinks when pip "
          "installing new Ray versions to prevent pip from overwriting files "
          "in your git repo.")
