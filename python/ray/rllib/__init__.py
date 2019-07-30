# flake8: noqa F811
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import sys

the_rllib = None

try:
    import rllib
    from rllib import (agents, contrib, env, evaluation, models, policy,
                       optimizers, offline, utils)
    print(">>> Using development RLlib at", rllib.__file__)
    the_rllib = rllib
except ImportError as e:
    if not re.match("No module named.*rllib", str(e)):
        raise

if the_rllib is None:
    from ray import rllib_builtin
    from rllib_builtin import (agents, contrib, env, evaluation, models,
                               policy, optimizers, offline, utils)
    print("Using built-in RLlib at", rllib_builtin.__file__)
    the_rllib = rllib_builtin

__all__ = [
    "agents", "contrib", "env", "evaluation", "models", "policy", "optimizers",
    "offline", "utils"
]

for package in __all__:
    sys.modules["ray.rllib.{}".format(package)] = getattr(the_rllib, package)
