import abc
from dataclasses import dataclass
from typing import List, Optional, Union

from ray.rllib.core.learner.learner import LearnerHyperparameters
from ray.rllib.utils.annotations import override


@dataclass
class DQNLearnerHyperparameters(LearnerHyperparameters):
    """Hyperparameters for the DQN Learner sub-classes (framework-specific).
    
    This should never been set directly by the user. Instead, use the `DQNConfig`
    class to configure your algorithm.
    See `ray.rllib.algorithms.dqn.dqn::DQNConfig.training()` for more details
    on individual properties.
    """

    # TODO (simon): As soon as SimpleQ is deprecated change this to 
    # `target_network_update_freq_ts`.
    target_network_update_freq: int = None
    replay_buffer_config: dict = None
    num_steps_sampled_before_learning_starts: int  = None
    store_buffer_in_checkpoints: bool = None
    lr_schedule: Optional[List[List[Union[int, float]]]] = None
    adam_epsilon: float = None
    grad_clip: bool = None
    grad_clip_by: str = None
    tau: float = None
    num_atoms: int = None
    v_min: float = None
    v_max: float = None
    noisy: bool = None
    sigma0: float = None
    hiddens: float = None
    double_q: bool = None
    n_step: int = None
    before_learn_on_batch: function = None
    training_intensity: int = None
    td_error_loss_fn: str = None
    categorical_distribution_temperature: float = None
