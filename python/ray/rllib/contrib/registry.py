"""Registry of algorithm names for `rllib train --run=<alg_name>`"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


def _import_random_agent():
    from ray.rllib.contrib.random_agent.random_agent import RandomTrainer
    return RandomTrainer


CONTRIBUTED_ALGORITHMS = {
    "contrib/RandomTrainer": _import_random_agent,
}
