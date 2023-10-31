from typing import Optional

from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class ARSConfig(AlgorithmConfig):
    def __init__(self):
        super().__init__(algo_class=ARS)

        # fmt: off
        # __sphinx_doc_begin__

        self.action_noise_std = 0.0
        self.noise_stdev = 0.02
        self.num_rollouts = 32
        self.rollouts_used = 32
        self.sgd_stepsize = 0.01
        self.noise_size = 250000000
        self.eval_prob = 0.03
        self.report_length = 10
        self.offset = 0
        self.tf_single_threaded = True
        self.num_rollout_workers = 2
        self.observation_filter = "MeanStdFilter"
        self.evaluation(
            evaluation_config=AlgorithmConfig.overrides(
                num_envs_per_worker=1,
                observation_filter="NoFilter",
            )
        )
        self.exploration_config = {
            "type": "StochasticSampling",
        }
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        action_noise_std: Optional[float] = NotProvided,
        noise_stdev: Optional[float] = NotProvided,
        num_rollouts: Optional[int] = NotProvided,
        rollouts_used: Optional[int] = NotProvided,
        sgd_stepsize: Optional[float] = NotProvided,
        noise_size: Optional[int] = NotProvided,
        eval_prob: Optional[float] = NotProvided,
        report_length: Optional[int] = NotProvided,
        offset: Optional[int] = NotProvided,
        tf_single_threaded: Optional[bool] = NotProvided,
        **kwargs,
    ) -> "ARSConfig":
        super().training(**kwargs)

        if action_noise_std is not NotProvided:
            self.action_noise_std = action_noise_std
        if noise_stdev is not NotProvided:
            self.noise_stdev = noise_stdev
        if num_rollouts is not NotProvided:
            self.num_rollouts = num_rollouts
        if rollouts_used is not NotProvided:
            self.rollouts_used = rollouts_used
        if sgd_stepsize is not NotProvided:
            self.sgd_stepsize = sgd_stepsize
        if noise_size is not NotProvided:
            self.noise_size = noise_size
        if eval_prob is not NotProvided:
            self.eval_prob = eval_prob
        if report_length is not NotProvided:
            self.report_length = report_length
        if offset is not NotProvided:
            self.offset = offset
        if tf_single_threaded is not NotProvided:
            self.tf_single_threaded = tf_single_threaded

        return self


@Deprecated(
    old="rllib/algorithms/ars/",
    new="rllib_contrib/ars/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ARS(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return ARSConfig()
