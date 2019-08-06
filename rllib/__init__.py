from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import sys

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.tune.registry import register_trainable

from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.sample_batch import SampleBatch


def _setup_logger():
    logger = logging.getLogger("ray.rllib")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
        ))
    logger.addHandler(handler)
    logger.propagate = False

    if sys.version_info[0] < 3:
        logger.warn(
            "RLlib Python 2 support is deprecated, and will be removed "
            "in a future release.")


def _register_all():

    from ray.rllib.agents.registry import ALGORITHMS
    from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS
    for key in list(ALGORITHMS.keys()) + list(CONTRIBUTED_ALGORITHMS.keys(
    )) + ["__fake", "__sigmoid_fake_data", "__parameter_tuning"]:
        from ray.rllib.agents.registry import get_agent_class
        register_trainable(key, get_agent_class(key))


_setup_logger()
_register_all()

__all__ = [
    "Policy",
    "PolicyGraph",
    "TFPolicy",
    "TFPolicyGraph",
    "RolloutWorker",
    "PolicyEvaluator",
    "SampleBatch",
    "BaseEnv",
    "MultiAgentEnv",
    "VectorEnv",
    "ExternalEnv",
]
