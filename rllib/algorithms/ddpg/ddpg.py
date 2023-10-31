from typing import List, Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.simple_q.simple_q import SimpleQ, SimpleQConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class DDPGConfig(SimpleQConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or DDPG)

        # fmt: off
        # __sphinx_doc_begin__
        self.twin_q = False
        self.policy_delay = 1
        self.smooth_target_policy = False
        self.target_noise = 0.2
        self.target_noise_clip = 0.5
        self.use_state_preprocessor = False
        self.actor_hiddens = [400, 300]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [400, 300]
        self.critic_hidden_activation = "relu"
        self.n_step = 1
        self.training_intensity = None
        self.critic_lr = 1e-3
        self.actor_lr = 1e-3
        self.tau = 0.002
        self.use_huber = False
        self.huber_threshold = 1.0
        self.l2_reg = 1e-6
        self.exploration_config = {
            "type": "OrnsteinUhlenbeckNoise",
            "random_timesteps": 1000,
            "ou_base_scale": 0.1,
            "ou_theta": 0.15,
            "ou_sigma": 0.2,
            "initial_scale": 1.0,
            "final_scale": 0.02,
            "scale_timesteps": 10000,
        }

        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 50000,
            "prioritized_replay": DEPRECATED_VALUE,
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_eps": 1e-6,
            "worker_side_prioritization": False,
        }
        self.grad_clip = None
        self.train_batch_size = 256
        self.target_network_update_freq = 0
        self.num_steps_sampled_before_learning_starts = 1500
        self.rollout_fragment_length = "auto"
        self.compress_observations = False

        # __sphinx_doc_end__
        # fmt: on
        self.worker_side_prioritization = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        twin_q: Optional[bool] = NotProvided,
        policy_delay: Optional[int] = NotProvided,
        smooth_target_policy: Optional[bool] = NotProvided,
        target_noise: Optional[bool] = NotProvided,
        target_noise_clip: Optional[float] = NotProvided,
        use_state_preprocessor: Optional[bool] = NotProvided,
        actor_hiddens: Optional[List[int]] = NotProvided,
        actor_hidden_activation: Optional[str] = NotProvided,
        critic_hiddens: Optional[List[int]] = NotProvided,
        critic_hidden_activation: Optional[str] = NotProvided,
        n_step: Optional[int] = NotProvided,
        critic_lr: Optional[float] = NotProvided,
        actor_lr: Optional[float] = NotProvided,
        tau: Optional[float] = NotProvided,
        use_huber: Optional[bool] = NotProvided,
        huber_threshold: Optional[float] = NotProvided,
        l2_reg: Optional[float] = NotProvided,
        training_intensity: Optional[float] = NotProvided,
        **kwargs,
    ) -> "DDPGConfig":
        super().training(**kwargs)

        if twin_q is not NotProvided:
            self.twin_q = twin_q
        if policy_delay is not NotProvided:
            self.policy_delay = policy_delay
        if smooth_target_policy is not NotProvided:
            self.smooth_target_policy = smooth_target_policy
        if target_noise is not NotProvided:
            self.target_noise = target_noise
        if target_noise_clip is not NotProvided:
            self.target_noise_clip = target_noise_clip
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
        if critic_lr is not NotProvided:
            self.critic_lr = critic_lr
        if actor_lr is not NotProvided:
            self.actor_lr = actor_lr
        if tau is not NotProvided:
            self.tau = tau
        if use_huber is not NotProvided:
            self.use_huber = use_huber
        if huber_threshold is not NotProvided:
            self.huber_threshold = huber_threshold
        if l2_reg is not NotProvided:
            self.l2_reg = l2_reg
        if training_intensity is not NotProvided:
            self.training_intensity = training_intensity

        return self


@Deprecated(
    old="rllib/algorithms/ddpg/",
    new="rllib_contrib/ddpg/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class DDPG(SimpleQ):
    @classmethod
    @override(SimpleQ)
    def get_default_config(cls) -> AlgorithmConfig:
        return DDPGConfig()
