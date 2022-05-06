#!/usr/bin/env python
from django.core.management import execute_from_command_line
import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ray.tune.automlboard.settings")
    execute_from_command_line(sys.argv)
