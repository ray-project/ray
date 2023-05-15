"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from dataclasses import dataclass
from typing import Any, Dict

from ray.rllib.core.learner.learner import Learner, LearnerHyperparameters
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override


@dataclass
class DreamerV3Hyperparameters(LearnerHyperparameters):
    """Hyperparameters for the DreamerV3Learner sub-classes (framework specific).

    These should never be set directly by the user. Instead, use the PPOConfig
    class to configure your algorithm.
    See `ray.rllib.algorithms.dreamerv3.dreamerv3::DreamerV3Config::training()` for
    more details on the individual properties.
    """

    model_dimension: str = None
    training_ratio: float = None
    batch_size_B: int = None
    batch_length_T: int = None
    horizon_H: int = None
    gamma: float = None
    gae_lambda: float = None
    entropy_scale: float = None
    return_normalization_decay: float = None
    # symlog_obs: bool = None
    world_model_lr: float = None
    actor_lr: float = None
    critic_lr: float = None
    train_critic: bool = None
    train_actor: bool = None
    use_curiosity: bool = None
    intrinsic_rewards_scale: float = None
    world_model_grad_clip_by_global_norm: float = None
    actor_grad_clip_by_global_norm: float = None
    critic_grad_clip_by_global_norm: float = None
    # disagree_grad_clip_by_global_norm: float = None


class DreamerV3Learner(Learner):
    @override(Learner)
    def additional_update_per_module(
        self, module_id: ModuleID, sampled_kl_values: dict, timestep: int
    ) -> Dict[str, Any]:
        """Updates the EMA weights of the critic network."""
        results = super().additional_update_per_module(
            module_id,
            sampled_kl_values=sampled_kl_values,
            timestep=timestep,
        )

        # Update EMA weights of the critic.
        self.module[module_id].critic.update_ema()

        return results
