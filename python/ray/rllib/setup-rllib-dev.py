#!/usr/bin/env python
"""This script allows you to develop RLlib without needing to compile Ray."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import click
import os
import subprocess

import ray

if __name__ == "__main__":
    rllib_home = os.path.abspath(os.path.join(ray.__file__, "../rllib"))
    local_home = os.path.abspath(os.path.dirname(__file__))
    assert os.path.isdir(rllib_home), rllib_home
    assert os.path.isdir(local_home), local_home
    click.confirm(
        "This will replace:\n  {}\nwith a symlink to:\n  {}".format(
            rllib_home, local_home),
        abort=True)
    if os.access(os.path.dirname(rllib_home), os.W_OK):
        subprocess.check_call(["rm", "-rf", rllib_home])
        subprocess.check_call(["ln", "-s", local_home, rllib_home])
    else:
        print("You don't have write permission to {}, using sudo:".format(
            rllib_home))
        subprocess.check_call(["sudo", "rm", "-rf", rllib_home])
        subprocess.check_call(["sudo", "ln", "-s", local_home, rllib_home])
    print("Created links.\n\nIf you run into issues initializing Ray, please "
          "ensure that your local repo and the installed Ray is in sync "
          "(pip install -U the latest wheels at "
          "https://ray.readthedocs.io/en/latest/installation.html, "
          "and ensure you are up-to-date on the master branch on git).\n\n"
          "Note that you may need to delete the rllib symlink when pip "
          "installing new Ray versions to prevent pip from overwriting files "
          "in your git repo.")
