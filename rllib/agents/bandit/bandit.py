import logging
from typing import Type

from ray.rllib.agents.bandit.bandit_tf_policy import BanditTFPolicy
from ray.rllib.agents.bandit.bandit_torch_policy import BanditTorchPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    "framework": "torch",

    # Do online learning one step at a time.
    "rollout_fragment_length": 1,
    "train_batch_size": 1,

    # Make sure, a `train()` call performs at least 100 env sampling timesteps, before
    # reporting results. Not setting this (default is 0) would significantly slow down
    # the Bandit Trainer.
    "min_sample_timesteps_per_reporting": 100,
})
# __sphinx_doc_end__
# fmt: on


class BanditLinTSTrainer(Trainer):
    """Bandit Trainer using ThompsonSampling exploration."""

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        config = Trainer.merge_trainer_configs(
            DEFAULT_CONFIG,
            {
                # Use ThompsonSampling exploration.
                "exploration_config": {"type": "ThompsonSampling"}
            },
        )
        return config

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return BanditTorchPolicy
        elif config["framework"] == "tf2":
            return BanditTFPolicy
        else:
            raise NotImplementedError()


class BanditLinUCBTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return Trainer.merge_trainer_configs(
            DEFAULT_CONFIG,
            {
                # Use UpperConfidenceBound exploration.
                "exploration_config": {"type": "UpperConfidenceBound"}
            },
        )

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return BanditTorchPolicy
        elif config["framework"] == "tf2":
            return BanditTFPolicy
        else:
            raise NotImplementedError()
