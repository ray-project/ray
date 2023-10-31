from typing import Optional

from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class ESConfig(AlgorithmConfig):
    def __init__(self):
        super().__init__(algo_class=ES)
        # fmt: off
        # __sphinx_doc_begin__
        self.action_noise_std = 0.01
        self.l2_coeff = 0.005
        self.noise_stdev = 0.02
        self.episodes_per_batch = 1000
        self.eval_prob = 0.03
        self.stepsize = 0.01
        self.noise_size = 250000000
        self.report_length = 10
        self.tf_single_threaded = True
        self.train_batch_size = 10000
        self.num_rollout_workers = 10
        self.evaluation(
            evaluation_config=AlgorithmConfig.overrides(
                num_envs_per_worker=1,
                observation_filter="NoFilter",
            )
        )
        self.observation_filter = "MeanStdFilter"
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
        l2_coeff: Optional[float] = NotProvided,
        noise_stdev: Optional[int] = NotProvided,
        episodes_per_batch: Optional[int] = NotProvided,
        eval_prob: Optional[float] = NotProvided,
        stepsize: Optional[float] = NotProvided,
        noise_size: Optional[int] = NotProvided,
        report_length: Optional[int] = NotProvided,
        tf_single_threaded: Optional[bool] = NotProvided,
        **kwargs,
    ) -> "ESConfig":
        super().training(**kwargs)

        if action_noise_std is not NotProvided:
            self.action_noise_std = action_noise_std
        if l2_coeff is not NotProvided:
            self.l2_coeff = l2_coeff
        if noise_stdev is not NotProvided:
            self.noise_stdev = noise_stdev
        if episodes_per_batch is not NotProvided:
            self.episodes_per_batch = episodes_per_batch
        if eval_prob is not NotProvided:
            self.eval_prob = eval_prob
        if stepsize is not NotProvided:
            self.stepsize = stepsize
        if noise_size is not NotProvided:
            self.noise_size = noise_size
        if report_length is not NotProvided:
            self.report_length = report_length
        if tf_single_threaded is not NotProvided:
            self.tf_single_threaded = tf_single_threaded

        return self


@Deprecated(
    old="rllib/algorithms/es/",
    new="rllib_contrib/es/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class ES(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return ESConfig()
