from typing import Dict, Optional

from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.typing import ModuleID, ShouldModuleBeUpdatedFn, TensorType

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
    def remove_module(
        self,
        module_id: ModuleID,
        *,
        new_should_module_be_updated: Optional[ShouldModuleBeUpdatedFn] = None,
    ) -> None:
        super().remove_module(
            module_id,
            new_should_module_be_updated=new_should_module_be_updated,
        )
        # In case of BC (beta==0.0 and this property never being used),
        self.moving_avg_sqd_adv_norms_per_module.pop(module_id, None)

    @classmethod
    @override(Learner)
    def rl_module_required_apis(cls) -> list[type]:
        # In order for a PPOLearner to update an RLModule, it must implement the
        # following APIs:
        return [ValueFunctionAPI]
