from typing import Any, DefaultDict, Dict, Mapping

import numpy as np

from ray.rllib.core.learner.learner import Learner, LearnerHyperparameters
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType


class BaseTestingLearnerHyperparameters(LearnerHyperparameters):
    # A test setting to activate metrics on mean weights.
    report_mean_weights: bool = True


class BaseTestingLearner(Learner):
    @override(Learner)
    def __init__(
        self,
        *,
        module_spec=None,
        module=None,
        learner_group_scaling_config=None,
        learner_hyperparameters=None,
        framework_hyperparameters=None,
    ):
        learner_hyperparameters = (
            learner_hyperparameters or BaseTestingLearnerHyperparameters()
        )
        super().__init__(
            module_spec=module_spec,
            module=module,
            learner_group_scaling_config=learner_group_scaling_config,
            learner_hyperparameters=learner_hyperparameters,
            framework_hyperparameters=framework_hyperparameters,
        )

    @override(Learner)
    def compile_results(
        self,
        *,
        batch: NestedDict,
        fwd_out: Mapping[str, Any],
        loss_per_module: Mapping[str, TensorType],
        metrics_per_module: DefaultDict[ModuleID, Dict[str, Any]],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch=batch,
            fwd_out=fwd_out,
            loss_per_module=loss_per_module,
            metrics_per_module=metrics_per_module,
        )
        # This is to check if in the multi-gpu case, the weights across workers are
        # the same. It is really only needed during testing.
        if self.hps.report_mean_weights:
            mean_ws = {}
            for module_id in self.module.keys():
                m = self.module[module_id]
                parameters = convert_to_numpy(self.get_parameters(m))
                mean_ws[module_id] = np.mean([w.mean() for w in parameters])
                results[module_id]["mean_weight"] = mean_ws[module_id]

        return results
