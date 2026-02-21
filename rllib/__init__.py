import logging

from ray._common.usage import usage_lib

# Note: do not introduce unnecessary library dependencies here, e.g. gym.
# This file is imported from the tune module in order to register RLlib agents.
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.tune.registry import register_trainable


def _setup_logger():
    logger = logging.getLogger("ray.rllib")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"
        )
    )
    logger.addHandler(handler)
    logger.propagate = False


def _register_all():
    from ray.rllib.algorithms.registry import ALGORITHMS, _get_algorithm_class

    for key, get_trainable_class_and_config in ALGORITHMS.items():
        register_trainable(key, get_trainable_class_and_config()[0])

    for key in ["__fake", "__sigmoid_fake_data", "__parameter_tuning"]:
        register_trainable(key, _get_algorithm_class(key))


_setup_logger()

usage_lib.record_library_usage("rllib")

__all__ = [
    "Policy",
    "TFPolicy",
    "TorchPolicy",
    "RolloutWorker",
    "SampleBatch",
    "BaseEnv",
    "MultiAgentEnv",
    "VectorEnv",
    "ExternalEnv",
]
