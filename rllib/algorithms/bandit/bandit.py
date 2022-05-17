import logging
from typing import Type, Union

from ray.rllib.algorithms.bandit.bandit_tf_policy import BanditTFPolicy
from ray.rllib.algorithms.bandit.bandit_torch_policy import BanditTorchPolicy
from ray.rllib.agents.trainer import Trainer
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.utils.deprecation import Deprecated

logger = logging.getLogger(__name__)


class BanditConfig(TrainerConfig):
    """Defines a contextual bandit configuration class from which
    a contexual bandit algorithm can be built. Note this config is shared
    between BanditLinUCBTrainer and BanditLinTSTrainer. You likely
    want to use the child classes BanditLinTSConfig or BanditLinUCBConfig
    instead.
    """

    def __init__(
        self, trainer_class: Union["BanditLinTSTrainer", "BanditLinUCBTrainer"] = None
    ):
        super().__init__(trainer_class=trainer_class)
        # fmt: off
        # __sphinx_doc_begin__
        # Override some of TrainerConfig's default values with bandit-specific values.
        self.framework_str = "torch"
        self.num_workers = 0
        self.rollout_fragment_length = 1
        self.train_batch_size = 1
        # Make sure, a `train()` call performs at least 100 env sampling
        # timesteps, before reporting results. Not setting this (default is 0)
        # would significantly slow down the Bandit Trainer.
        self.min_sample_timesteps_per_reporting = 100
        # __sphinx_doc_end__
        # fmt: on


class BanditLinTSConfig(BanditConfig):
    """Defines a configuration class from which a Thompson-sampling bandit can be built.

    Example:
        >>> from ray.rllib.algorithms.bandit import BanditLinTSConfig
        >>> from ray.rllib.examples.env.bandit_envs_discrete import WheelBanditEnv
        >>> config = BanditLinTSConfig().rollouts(num_rollout_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env=WheelBanditEnv)
        >>> trainer.train()
    """

    def __init__(self):
        super().__init__(trainer_class=BanditLinTSTrainer)
        # fmt: off
        # __sphinx_doc_begin__
        # Override some of TrainerConfig's default values with bandit-specific values.
        self.exploration_config = {"type": "ThompsonSampling"}
        # __sphinx_doc_end__
        # fmt: on


class BanditLinUCBConfig(BanditConfig):
    """Defines a config class from which an upper confidence bound bandit can be built.

    Example:
        >>> from ray.rllib.algorithms.bandit import BanditLinUCBConfig
        >>> from ray.rllib.examples.env.bandit_envs_discrete import WheelBanditEnv
        >>> config = BanditLinUCBConfig().rollouts(num_rollout_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env=WheelBanditEnv)
        >>> trainer.train()
    """

    def __init__(self):
        super().__init__(trainer_class=BanditLinUCBTrainer)
        # fmt: off
        # __sphinx_doc_begin__
        # Override some of TrainerConfig's default values with bandit-specific values.
        self.exploration_config = {"type": "UpperConfidenceBound"}
        # __sphinx_doc_end__
        # fmt: on


class BanditLinTSTrainer(Trainer):
    """Bandit Trainer using ThompsonSampling exploration."""

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> BanditLinTSConfig:
        return BanditLinTSConfig().to_dict()

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return BanditTorchPolicy
        elif config["framework"] == "tf2":
            return BanditTFPolicy
        else:
            raise NotImplementedError("Only `framework=[torch|tf2]` supported!")


class BanditLinUCBTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> BanditLinUCBConfig:
        return BanditLinUCBConfig().to_dict()

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            return BanditTorchPolicy
        elif config["framework"] == "tf2":
            return BanditTFPolicy
        else:
            raise NotImplementedError("Only `framework=[torch|tf2]` supported!")


# Deprecated: Use ray.rllib.algorithms.bandit.BanditLinUCBConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(BanditLinUCBConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.bandit.bandit.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.bandit.bandit.BanditLin[UCB|TS]Config(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
