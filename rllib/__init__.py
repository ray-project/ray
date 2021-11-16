import logging

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
        ))
    logger.addHandler(handler)
    logger.propagate = False


def _register_all():
    from ray.rllib.agents.trainer import Trainer, with_common_config
    from ray.rllib.agents.registry import ALGORITHMS, get_trainer_class
    from ray.rllib.contrib.registry import CONTRIBUTED_ALGORITHMS

    for key in list(ALGORITHMS.keys()) + list(CONTRIBUTED_ALGORITHMS.keys(
    )) + ["__fake", "__sigmoid_fake_data", "__parameter_tuning"]:
        register_trainable(key, get_trainer_class(key))

    def _see_contrib(name):
        """Returns dummy agent class warning algo is in contrib/."""

        class _SeeContrib(Trainer):
            _name = "SeeContrib"
            _default_config = with_common_config({})

            def setup(self, config):
                raise NameError(
                    "Please run `contrib/{}` instead.".format(name))

        return _SeeContrib

    # also register the aliases minus contrib/ to give a good error message
    for key in list(CONTRIBUTED_ALGORITHMS.keys()):
        assert key.startswith("contrib/")
        alias = key.split("/", 1)[1]
        register_trainable(alias, _see_contrib(alias))


_setup_logger()
_register_all()

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
