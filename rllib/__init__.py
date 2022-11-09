import logging

from ray._private.usage import usage_lib

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
    from ray.rllib.algorithms.algorithm import Algorithm
    from ray.rllib.algorithms.registry import ALGORITHMS, _get_algorithm_class
    from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS

    for key, get_trainable_class_and_config in list(ALGORITHMS.items()) + list(
        CONTRIBUTED_ALGORITHMS.items()
    ):
        register_trainable(key, get_trainable_class_and_config()[0])

    for key in ["__fake", "__sigmoid_fake_data", "__parameter_tuning"]:
        register_trainable(key, _get_algorithm_class(key))

    def _see_contrib(name):
        """Returns dummy agent class warning algo is in contrib/."""

        class _SeeContrib(Algorithm):
            def setup(self, config):
                raise NameError("Please run `contrib/{}` instead.".format(name))

        return _SeeContrib

    # Also register the aliases minus contrib/ to give a good error message.
    for key in list(CONTRIBUTED_ALGORITHMS.keys()):
        assert key.startswith("contrib/")
        alias = key.split("/", 1)[1]
        if alias not in ALGORITHMS:
            register_trainable(alias, _see_contrib(alias))


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
