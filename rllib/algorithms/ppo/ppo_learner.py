from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from ray.rllib.core.learner.learner import LearnerHyperparameters
from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules.piecewise_schedule import PiecewiseSchedule


LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = "vf_loss_unclipped"
LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = "vf_explained_var"
LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY = "vf_loss_unclipped"
LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = "vf_explained_var"
LEARNER_RESULTS_KL_KEY = "mean_kl_loss"
LEARNER_RESULTS_CURR_KL_COEFF_KEY = "curr_kl_coeff"
LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY = "curr_entropy_coeff"


@dataclass
class PPOLearnerHyperparameters(LearnerHyperparameters):
    """Hyperparameters for the PPOLearner sub-classes (framework specific).

    These should never be set directly by the user. Instead, use the PPOConfig
    class to configure your algorithm.
    See `ray.rllib.algorithms.ppo.ppo::PPOConfig::training()` for more details on the
    individual properties.
    """

    kl_coeff: float = None
    kl_target: float = None
    use_critic: bool = None
    clip_param: float = None
    vf_clip_param: float = None
    entropy_coeff: float = None
    entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None
    vf_loss_coeff: float = None

    # TODO: Move to base LearnerHyperparameter class (and handling of this setting
    #  into base Learners).
    lr_schedule: Optional[List[List[Union[int, float]]]] = None


class PPOLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Build entropy coeff scheduling tools.
        self.entropy_coeff_scheduler = None
        if self.hps.entropy_coeff_schedule:
            # Custom schedule, based on list of
            # ([ts], [value to be reached by ts])-tuples.
            self.entropy_coeff_schedule_per_module = defaultdict(
                lambda: PiecewiseSchedule(
                    self.hps.entropy_coeff_schedule,
                    outside_value=self.hps.entropy_coeff_schedule[-1][-1],
                    framework=None,
                )
            )
            self.curr_entropy_coeffs_per_module = defaultdict(
                lambda: self._get_tensor_variable(self.hps.entropy_coeff)
            )
        # If no schedule, pin entropy coeff to its given (fixed) value.
        else:
            self.curr_entropy_coeffs_per_module = defaultdict(
                lambda: self.hps.entropy_coeff
            )

        # Set up KL coefficient variables (per module).
        # Note that the KL coeff is not controlled by a schedul, but seeks
        # to stay close to a given kl_target value.
        self.curr_kl_coeffs_per_module = defaultdict(
            lambda: self._get_tensor_variable(self.hps.kl_coeff)
        )
