from typing import List, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn import DQN
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class MADDPGConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or MADDPG)

        # fmt: off
        # __sphinx_doc_begin__
        self.agent_id = None
        self.use_local_critic = False
        self.use_state_preprocessor = False
        self.actor_hiddens = [64, 64]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [64, 64]
        self.critic_hidden_activation = "relu"
        self.n_step = 1
        self.good_policy = "maddpg"
        self.adv_policy = "maddpg"
        self.replay_buffer_config = {
            "type": "MultiAgentReplayBuffer",
            "prioritized_replay": DEPRECATED_VALUE,
            "capacity": int(1e6),
            "replay_mode": "lockstep",
        }
        self.training_intensity = None
        self.num_steps_sampled_before_learning_starts = 1024 * 25
        self.critic_lr = 1e-2
        self.actor_lr = 1e-2
        self.target_network_update_freq = 0
        self.tau = 0.01
        self.actor_feature_reg = 0.001
        self.grad_norm_clipping = 0.5
        self.rollout_fragment_length = 100
        self.train_batch_size = 1024
        self.num_rollout_workers = 1
        self.min_time_s_per_iteration = 0
        self.exploration_config = {
            "type": "StochasticSampling",
        }
        # fmt: on
        # __sphinx_doc_end__

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        agent_id: Optional[str] = NotProvided,
        use_local_critic: Optional[bool] = NotProvided,
        use_state_preprocessor: Optional[bool] = NotProvided,
        actor_hiddens: Optional[List[int]] = NotProvided,
        actor_hidden_activation: Optional[str] = NotProvided,
        critic_hiddens: Optional[List[int]] = NotProvided,
        critic_hidden_activation: Optional[str] = NotProvided,
        n_step: Optional[int] = NotProvided,
        good_policy: Optional[str] = NotProvided,
        adv_policy: Optional[str] = NotProvided,
        replay_buffer_config: Optional[dict] = NotProvided,
        training_intensity: Optional[float] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        critic_lr: Optional[float] = NotProvided,
        actor_lr: Optional[float] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        tau: Optional[float] = NotProvided,
        actor_feature_reg: Optional[float] = NotProvided,
        grad_norm_clipping: Optional[float] = NotProvided,
        **kwargs,
    ) -> "MADDPGConfig":
        super().training(**kwargs)

        if agent_id is not NotProvided:
            self.agent_id = agent_id
        if use_local_critic is not NotProvided:
            self.use_local_critic = use_local_critic
        if use_state_preprocessor is not NotProvided:
            self.use_state_preprocessor = use_state_preprocessor
        if actor_hiddens is not NotProvided:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not NotProvided:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not NotProvided:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not NotProvided:
            self.critic_hidden_activation = critic_hidden_activation
        if n_step is not NotProvided:
            self.n_step = n_step
        if good_policy is not NotProvided:
            self.good_policy = good_policy
        if adv_policy is not NotProvided:
            self.adv_policy = adv_policy
        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config = replay_buffer_config
        if training_intensity is not NotProvided:
            self.training_intensity = training_intensity
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )
        if critic_lr is not NotProvided:
            self.critic_lr = critic_lr
        if actor_lr is not NotProvided:
            self.actor_lr = actor_lr
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if tau is not NotProvided:
            self.tau = tau
        if actor_feature_reg is not NotProvided:
            self.actor_feature_reg = actor_feature_reg
        if grad_norm_clipping is not NotProvided:
            self.grad_norm_clipping = grad_norm_clipping

        return self


@Deprecated(
    old="rllib/algorithms/maddpg/",
    new="rllib_contrib/maddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class MADDPG(DQN):
    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return MADDPGConfig()
