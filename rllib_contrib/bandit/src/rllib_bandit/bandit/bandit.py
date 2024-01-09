import logging
from typing import Optional, Type, Union

from rllib_bandit.bandit.bandit_tf_policy import BanditTFPolicy
from rllib_bandit.bandit.bandit_torch_policy import BanditTorchPolicy

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class BanditConfig(AlgorithmConfig):
    """Defines a contextual bandit configuration class from which
    a contexual bandit algorithm can be built. Note this config is shared
    between BanditLinUCB and BanditLinTS. You likely
    want to use the child classes BanditLinTSConfig or BanditLinUCBConfig
    instead.
    """

    def __init__(self, algo_class: Union["BanditLinTS", "BanditLinUCB"] = None):
        super().__init__(algo_class=algo_class)
        # fmt: off
        # __sphinx_doc_begin__
        # Override some of AlgorithmConfig's default values with bandit-specific values.
        self.framework_str = "torch"
        self.rollout_fragment_length = 1
        self.train_batch_size = 1
        # Make sure, a `train()` call performs at least 100 env sampling
        # timesteps, before reporting results. Not setting this (default is 0)
        # would significantly slow down the Bandit Algorithm.
        self.min_sample_timesteps_per_iteration = 100
        # __sphinx_doc_end__
        # fmt: on


class BanditLinTSConfig(BanditConfig):
    """Defines a configuration class from which a Thompson-sampling bandit can be built.

    Example:
        >>> from ray.rllib.algorithms.bandit import BanditLinTSConfig # doctest: +SKIP
        >>> from ray.rllib.examples.env.bandit_envs_discrete import WheelBanditEnv
        >>> config = BanditLinTSConfig().rollouts(num_rollout_workers=4)# doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env=WheelBanditEnv)  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP
    """

    def __init__(self):
        super().__init__(algo_class=BanditLinTS)
        # fmt: off
        # __sphinx_doc_begin__
        # Override some of AlgorithmConfig's default values with bandit-specific values.
        self.exploration_config = {"type": "ThompsonSampling"}
        # __sphinx_doc_end__
        # fmt: on


class BanditLinUCBConfig(BanditConfig):
    """Defines a config class from which an upper confidence bound bandit can be built.

    Example:
        >>> from ray.rllib.algorithms.bandit import BanditLinUCBConfig# doctest: +SKIP
        >>> from ray.rllib.examples.env.bandit_envs_discrete import WheelBanditEnv
        >>> config = BanditLinUCBConfig()  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=4) # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env=WheelBanditEnv)  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP
    """

    def __init__(self):
        super().__init__(algo_class=BanditLinUCB)
        # fmt: off
        # __sphinx_doc_begin__
        # Override some of AlgorithmConfig's default values with bandit-specific values.
        self.exploration_config = {"type": "UpperConfidenceBound"}
        # __sphinx_doc_end__
        # fmt: on


class BanditLinTS(Algorithm):
    """Bandit Algorithm using ThompsonSampling exploration."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> BanditLinTSConfig:
        return BanditLinTSConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return BanditTorchPolicy
        elif config["framework"] == "tf2":
            return BanditTFPolicy
        else:
            raise NotImplementedError("Only `framework=[torch|tf2]` supported!")


class BanditLinUCB(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> BanditLinUCBConfig:
        return BanditLinUCBConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            return BanditTorchPolicy
        elif config["framework"] == "tf2":
            return BanditTFPolicy
        else:
            raise NotImplementedError("Only `framework=[torch|tf2]` supported!")
