#!/home/ubuntu/.conda/envs/rayturbo/bin/python3.9
# -*- coding: utf-8 -*-
import re
import sys
from ray.serve.scripts import cli

if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    sys.exit(cli())
