# flake8: noqa
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import sys

try:
    import rllib
    from rllib import *
    print(">>> Using development RLlib at", rllib.__file__)
    sys.modules["ray.rllib"] = rllib
except ImportError as e:
    if not re.match("No module named.*rllib", str(e)):
        raise

if "ray.rllib" not in sys.modules:
    from ray import rllib_builtin
    from ray.rllib_builtin import *
    print("Using built-in RLlib at", rllib_builtin.__file__)
    sys.modules["ray.rllib"] = rllib_builtin
