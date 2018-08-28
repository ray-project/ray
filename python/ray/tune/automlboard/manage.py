#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from django.core.management import execute_from_command_line
import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE",
                          "ray.tune.automlboard.settings")
    execute_from_command_line(sys.argv)
