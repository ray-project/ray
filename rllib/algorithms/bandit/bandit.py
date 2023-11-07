from typing import Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class BanditConfig(AlgorithmConfig):
    def __init__(self, algo_class: Union["BanditLinTS", "BanditLinUCB"] = None):
        super().__init__(algo_class=algo_class)
        # fmt: off
        # __sphinx_doc_begin__
        self.framework_str = "torch"
        self.rollout_fragment_length = 1
        self.train_batch_size = 1
        self.min_sample_timesteps_per_iteration = 100
        # __sphinx_doc_end__
        # fmt: on


class BanditLinTSConfig(BanditConfig):
    def __init__(self):
        super().__init__(algo_class=BanditLinTS)
        # fmt: off
        # __sphinx_doc_begin__
        self.exploration_config = {"type": "ThompsonSampling"}
        # __sphinx_doc_end__
        # fmt: on


class BanditLinUCBConfig(BanditConfig):
    def __init__(self):
        super().__init__(algo_class=BanditLinUCB)
        # fmt: off
        # __sphinx_doc_begin__
        self.exploration_config = {"type": "UpperConfidenceBound"}
        # __sphinx_doc_end__
        # fmt: on


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditLinTS(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> BanditLinTSConfig:
        return BanditLinTSConfig()


@Deprecated(
    old="rllib/algorithms/bandit/",
    new="rllib_contrib/bandit/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class BanditLinUCB(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> BanditLinUCBConfig:
        return BanditLinUCBConfig()
