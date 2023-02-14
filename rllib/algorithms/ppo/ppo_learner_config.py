from dataclasses import dataclass
from typing import List, Optional, Union

from ray.rllib.core.learner.learner import LearnerHPs


@dataclass
class PPOLearnerHPs(LearnerHPs):
    """Hyperparameters for the PPO RL Trainer"""

    kl_coeff: float = 0.2
    kl_target: float = 0.01
    use_critic: bool = True
    clip_param: float = 0.3
    vf_clip_param: float = 10.0
    entropy_coeff: float = 0.0
    vf_loss_coeff: float = 1.0

    # experimental placeholder for things that could be part of the base LearnerHPs
    lr_schedule: Optional[List[List[Union[int, float]]]] = None
    entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None
