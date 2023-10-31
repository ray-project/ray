from typing import Any, Dict, List, Optional, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn import DQN
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    ALGO_DEPRECATION_WARNING,
)


class SlateQConfig(AlgorithmConfig):
    def __init__(self):
        super().__init__(algo_class=SlateQ)

        # fmt: off
        # __sphinx_doc_begin__
        self.fcnet_hiddens_per_candidate = [256, 32]
        self.target_network_update_freq = 3200
        self.tau = 1.0
        self.use_huber = False
        self.huber_threshold = 1.0
        self.training_intensity = None
        self.lr_schedule = None
        self.lr_choice_model = 1e-3
        self.rmsprop_epsilon = 1e-5
        self.grad_clip = None
        self.n_step = 1
        self.replay_buffer_config = {
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 100000,
            "prioritized_replay_alpha": 0.6,
            "prioritized_replay_beta": 0.4,
            "prioritized_replay_eps": 1e-6,
            "replay_sequence_length": 1,
            "worker_side_prioritization": False,
        }
        self.num_steps_sampled_before_learning_starts = 20000
        self.exploration_config = {
            "type": "SlateEpsilonGreedy",
            "warmup_timesteps": 20000,
            "epsilon_timesteps": 250000,
            "final_epsilon": 0.01,
        }
        self.evaluation_config = {"explore": False}
        self.rollout_fragment_length = 4
        self.train_batch_size = 32
        self.lr = 0.00025
        self.min_sample_timesteps_per_iteration = 1000
        self.min_time_s_per_iteration = 1
        self.compress_observations = False
        self._disable_preprocessor_api = True
        self.evaluation(evaluation_config=AlgorithmConfig.overrides(explore=False))
        # __sphinx_doc_end__
        # fmt: on

        self.learning_starts = DEPRECATED_VALUE

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        replay_buffer_config: Optional[Dict[str, Any]] = NotProvided,
        fcnet_hiddens_per_candidate: Optional[List[int]] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        tau: Optional[float] = NotProvided,
        use_huber: Optional[bool] = NotProvided,
        huber_threshold: Optional[float] = NotProvided,
        training_intensity: Optional[float] = NotProvided,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        lr_choice_model: Optional[bool] = NotProvided,
        rmsprop_epsilon: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        n_step: Optional[int] = NotProvided,
        num_steps_sampled_before_learning_starts: Optional[int] = NotProvided,
        **kwargs,
    ) -> "SlateQConfig":
        super().training(**kwargs)

        if replay_buffer_config is not NotProvided:
            self.replay_buffer_config.update(replay_buffer_config)
        if fcnet_hiddens_per_candidate is not NotProvided:
            self.fcnet_hiddens_per_candidate = fcnet_hiddens_per_candidate
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if tau is not NotProvided:
            self.tau = tau
        if use_huber is not NotProvided:
            self.use_huber = use_huber
        if huber_threshold is not NotProvided:
            self.huber_threshold = huber_threshold
        if training_intensity is not NotProvided:
            self.training_intensity = training_intensity
        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if lr_choice_model is not NotProvided:
            self.lr_choice_model = lr_choice_model
        if rmsprop_epsilon is not NotProvided:
            self.rmsprop_epsilon = rmsprop_epsilon
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if n_step is not NotProvided:
            self.n_step = n_step
        if num_steps_sampled_before_learning_starts is not NotProvided:
            self.num_steps_sampled_before_learning_starts = (
                num_steps_sampled_before_learning_starts
            )

        return self


@Deprecated(
    old="rllib/algorithms/slate_q/",
    new="rllib_contrib/slate_q/",
    help=ALGO_DEPRECATION_WARNING,
    error=True,
)
class SlateQ(DQN):
    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return SlateQConfig()
