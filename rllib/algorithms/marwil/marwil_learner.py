from typing import Dict

from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.typing import ModuleID, TensorType

LEARNER_RESULTS_MOVING_AVG_SQD_ADV_NORM_KEY = "moving_avg_sqd_adv_norm"
LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY = "vf_explained_variance"


# TODO (simon): Check, if the norm update should be done inside
# the Learner.
class MARWILLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super().build()

        # Dict mapping module IDs to the respective moving averages of squared
        # advantages.
        self.moving_avg_sqd_adv_norms_per_module: Dict[
            ModuleID, TensorType
        ] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                self.config.get_config_for_module(
                    module_id
                ).moving_average_sqd_adv_norm_start
            )
        )

    @override(Learner)
    def remove_module(self, module_id: ModuleID) -> None:
        super().remove_module(module_id)
        self.moving_avg_sqd_adv_norms_per_module.pop(module_id)
