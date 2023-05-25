from typing import Any, DefaultDict, Dict, Mapping

import numpy as np

from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import TensorType


class BaseTestingLearner(Learner):
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
        # this is to check if in the multi-gpu case, the weights across workers are
        # the same. It is really only needed during testing.
        mean_ws = {}
        for module_id in self._module.keys():
            m = self._module[module_id]
            parameters = convert_to_numpy(self.get_parameters(m))
            mean_ws[module_id] = np.mean([w.mean() for w in parameters])
            results[module_id]["mean_weight"] = mean_ws[module_id]

        return results
