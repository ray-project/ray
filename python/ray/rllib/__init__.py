from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.tune.registry import register_trainable

from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.env.async_vector_env import AsyncVectorEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.sample_batch import SampleBatch


def _setup_logger():
    logger = logging.getLogger("ray.rllib")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
        ))
    logger.addHandler(handler)
    logger.propagate = False


def _register_all():

    for key in [
            "PPO", "ES", "DQN", "APEX", "A3C", "PG", "DDPG", "APEX_DDPG",
            "IMPALA", "ARS", "A2C", "__fake", "__sigmoid_fake_data",
            "__parameter_tuning"
    ]:
        from ray.rllib.agents.agent import get_agent_class
        register_trainable(key, get_agent_class(key))


_setup_logger()
_register_all()

__all__ = [
    "PolicyGraph",
    "TFPolicyGraph",
    "PolicyEvaluator",
    "SampleBatch",
    "AsyncVectorEnv",
    "MultiAgentEnv",
    "VectorEnv",
    "ExternalEnv",
]
