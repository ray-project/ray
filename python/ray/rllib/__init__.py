from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.tune.registry import register_trainable

from ray.rllib.utils.policy_graph import PolicyGraph
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator
from ray.rllib.utils.async_vector_env import AsyncVectorEnv
from ray.rllib.utils.vector_env import VectorEnv
from ray.rllib.utils.serving_env import ServingEnv
from ray.rllib.optimizers.sample_batch import SampleBatch


def _register_all():
    for key in ["PPO", "ES", "DQN", "APEX", "A3C", "BC", "PG", "DDPG",
                "APEX_DDPG", "__fake", "__sigmoid_fake_data",
                "__parameter_tuning"]:
        from ray.rllib.agent import get_agent_class
        register_trainable(key, get_agent_class(key))


_register_all()

__all__ = [
    "PolicyGraph", "TFPolicyGraph", "CommonPolicyEvaluator", "SampleBatch",
    "AsyncVectorEnv", "VectorEnv", "ServingEnv",
]
