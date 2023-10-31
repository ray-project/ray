from typing import List, Optional, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class PGConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or PG)

        # fmt: off
        # __sphinx_doc_begin__
        self.lr_schedule = None
        self.lr = 0.0004
        self.rollout_fragment_length = "auto"
        self.train_batch_size = 200
        self._disable_preprocessor_api = True
        self.exploration_config = {
            "type": "StochasticSampling",
        }
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        **kwargs,
    ) -> "PGConfig":
        super().training(**kwargs)
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule

        return self


@Deprecated(
    old="rllib/algorithms/pg/",
    new="rllib_contrib/pg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class PG(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return PGConfig()
