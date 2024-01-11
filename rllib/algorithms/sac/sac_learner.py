import math

from typing import Dict

from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.lambda_defaultdict import LambdaDefaultDict
from ray.rllib.utils.schedules.scheduler import Scheduler
from ray.rllib.utils.typing import ModuleID

QF_PREDS = "qf_preds"
QF_TARGET_PREDS = "qf_target_preds"



class SACLearner(Learner):
    @override(Learner)
    def build(self) -> None:
        super.build()

        # Store the current alpha in log form. We need it during optimization
        # in log form.
        self.curr_log_alpha: Dict[ModuleID, Scheduler] = LambdaDefaultDict(
            lambda module_id: self._get_tensor_variable(
                math.log(self.config.get_config_for_module(module_id).initial_alpha)
            )
        )
