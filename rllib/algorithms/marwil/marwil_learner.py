from typing import Dict

from ray.rllib.core.learner.learner import Learner, LearnerHyperparameters
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.schedules.scheduler import Scheduler

LEARNER_RESULTS_MOVING_AVG_SQD_ADV_NORM_KEY = "moving_avg_sqd_adv_norm"
LEARNER_RESULTS_VF_EXPLAINED_VARIANCE_KEY = "vf_explained_variance"

class MARWILLearnerHyperparameters(LearnerHyperparameters):
    """Hyperparameters for the MARWILLearner saub-classes (framework-specific).

    These parameters should never be set directly by the user. Instead, the
    MARWILConfig class should be used for algorithm configuration.
    See `ray.rllib.algorithms.marwil.marwil::MARWILConfig::training()` for
    more details on individual properties.
    """

    beta: float = None
    bc_logstd_coeff: float = None
    # TODO (simon): Abbreviate to ma_sqd_adv_norm_update_rate.
    moving_average_sqd_adv_norm_update_rate: float = None
    moving_average_sqd_avg_norm_start: float = None
    use_gae: bool = None
    vf_coeff: float = None
    grad_clip: float = None


# TODO (simon): Check, if the norm update should be done inside
# the Learner.
class MARWILLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective advantages moving averages.
        self.moving_avg_sqd_adv_norms_per_module: Dict[
            ModuleID, Scheduler
        ] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                self.hps.get_hps_for_module(module_id).moving_average_sqd_adv_norm_start
            )
        )

    @override(Learner)
    def remove_module(self, module_id: ModuleID) -> None:
        super().remove_module(module_id)
        self.moving_avg_sqd_adv_norms_per_module.pop(module_id)
