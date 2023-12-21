from typing import Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.simple_q.simple_q import SimpleQ, SimpleQConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    deprecation_warning,
    ALGO_DEPRECATION_WARNING,
)


class QMixConfig(SimpleQConfig):
    def __init__(self):
        super().__init__(algo_class=QMix)

        # fmt: off
        # __sphinx_doc_begin__
        self.mixer = "qmix"
        self.mixing_embed_dim = 32
        self.double_q = True
        self.optim_alpha = 0.99
        self.optim_eps = 0.00001
        self.grad_clip = 10.0
        self.grad_clip_by = "global_norm"
        self.simple_optimizer = True
        self.lr = 0.0005
        self.train_batch_size = 32
        self.target_network_update_freq = 500
        self.num_steps_sampled_before_learning_starts = 1000
        self.replay_buffer_config = {
            "type": "ReplayBuffer",
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": 1000,
            "storage_unit": "fragments",
            "worker_side_prioritization": False,
        }
        self.model = {
            "lstm_cell_size": 64,
            "max_seq_len": 999999,
        }
        self.framework_str = "torch"
        self.rollout_fragment_length = 4
        self.batch_mode = "complete_episodes"
        self.min_time_s_per_iteration = 1
        self.min_sample_timesteps_per_iteration = 1000
        self.exploration_config = {
            "type": "EpsilonGreedy",
            "initial_epsilon": 1.0,
            "final_epsilon": 0.01,
            "epsilon_timesteps": 40000,
        }
        self.evaluation(
            evaluation_config=AlgorithmConfig.overrides(explore=False)
        )
        # __sphinx_doc_end__
        # fmt: on
        self.worker_side_prioritization = DEPRECATED_VALUE

    @override(SimpleQConfig)
    def training(
        self,
        *,
        mixer: Optional[str] = NotProvided,
        mixing_embed_dim: Optional[int] = NotProvided,
        double_q: Optional[bool] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        optim_alpha: Optional[float] = NotProvided,
        optim_eps: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        grad_norm_clipping=DEPRECATED_VALUE,
        **kwargs,
    ) -> "QMixConfig":
        super().training(**kwargs)

        if grad_norm_clipping != DEPRECATED_VALUE:
            deprecation_warning(
                old="grad_norm_clipping",
                new="grad_clip",
                help="Parameter `grad_norm_clipping` has been "
                "deprecated in favor of grad_clip in QMix. "
                "This is now the same parameter as in other "
                "algorithms. `grad_clip` will be overwritten by "
                "`grad_norm_clipping={}`".format(grad_norm_clipping),
                error=True,
            )
            grad_clip = grad_norm_clipping

        if mixer is not NotProvided:
            self.mixer = mixer
        if mixing_embed_dim is not NotProvided:
            self.mixing_embed_dim = mixing_embed_dim
        if double_q is not NotProvided:
            self.double_q = double_q
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config = replay_buffer_config
        if optim_alpha is not NotProvided:
            self.optim_alpha = optim_alpha
        if optim_eps is not NotProvided:
            self.optim_eps = optim_eps
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip

        return self


@Deprecated(
    old="rllib/algorithms/qmix/",
    new="rllib_contrib/qmix/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class QMix(SimpleQ):
    @classmethod
    @override(SimpleQ)
    def get_default_config(cls) -> AlgorithmConfig:
        return QMixConfig()
