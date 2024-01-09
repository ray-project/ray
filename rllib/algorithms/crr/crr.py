from typing import List, Optional

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING


class CRRConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or CRR)

        # fmt: off
        # __sphinx_doc_begin__
        self.weight_type = "bin"
        self.temperature = 1.0
        self.max_weight = 20.0
        self.advantage_type = "mean"
        self.n_action_sample = 4
        self.twin_q = True
        self.train_batch_size = 128
        self.target_network_update_freq = None
        # __sphinx_doc_end__
        # fmt: on
        self.actor_hiddens = [256, 256]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [256, 256]
        self.critic_hidden_activation = "relu"
        self.critic_lr = 3e-4
        self.actor_lr = 3e-4
        self.tau = 5e-3
        self.framework_str = "torch"
        self.num_rollout_workers = 4
        self.offline_sampling = True
        self.min_time_s_per_iteration = 10.0
        self.td_error_loss_fn = "mse"
        self.categorical_distribution_temperature = 1.0
        self.exploration_config = {
            "type": "StochasticSampling",
        }

    def training(
        self,
        *,
        weight_type: Optional[str] = NotProvided,
        temperature: Optional[float] = NotProvided,
        max_weight: Optional[float] = NotProvided,
        advantage_type: Optional[str] = NotProvided,
        n_action_sample: Optional[int] = NotProvided,
        twin_q: Optional[bool] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        actor_hiddens: Optional[List[int]] = NotProvided,
        actor_hidden_activation: Optional[str] = NotProvided,
        critic_hiddens: Optional[List[int]] = NotProvided,
        critic_hidden_activation: Optional[str] = NotProvided,
        tau: Optional[float] = NotProvided,
        td_error_loss_fn: Optional[str] = NotProvided,
        categorical_distribution_temperature: Optional[float] = NotProvided,
        **kwargs,
    ) -> "CRRConfig":
        super().training(**kwargs)

        if weight_type is not NotProvided:
            self.weight_type = weight_type
        if temperature is not NotProvided:
            self.temperature = temperature
        if max_weight is not NotProvided:
            self.max_weight = max_weight
        if advantage_type is not NotProvided:
            self.advantage_type = advantage_type
        if n_action_sample is not NotProvided:
            self.n_action_sample = n_action_sample
        if twin_q is not NotProvided:
            self.twin_q = twin_q
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if actor_hiddens is not NotProvided:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not NotProvided:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not NotProvided:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not NotProvided:
            self.critic_hidden_activation = critic_hidden_activation
        if tau is not NotProvided:
            self.tau = tau
        if td_error_loss_fn is not NotProvided:
            self.td_error_loss_fn = td_error_loss_fn
        if categorical_distribution_temperature is not NotProvided:
            self.categorical_distribution_temperature = (
                categorical_distribution_temperature
            )

        return self


@Deprecated(
    old="rllib/algorithms/crr/",
    new="rllib_contrib/crr/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class CRR(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return CRRConfig()
