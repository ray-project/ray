from typing import Any, Mapping, Union

import numpy as np

from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import LEARNER_STATS_KEY
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
        loss_or_loss_stats: Union[TensorType, Mapping[str, Any]],
        postprocessed_gradients: Mapping[str, Any],
        compute_grad_stats: Mapping[str, Any],
        postprocess_grad_stats: Mapping[str, Any],
        apply_grad_stats: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch=batch,
            fwd_out=fwd_out,
            loss_or_loss_stats=loss_or_loss_stats,
            postprocessed_gradients=postprocessed_gradients,
            compute_grad_stats=compute_grad_stats,
            postprocess_grad_stats=postprocess_grad_stats,
            apply_grad_stats=apply_grad_stats,
        )
        # this is to check if in the multi-gpu case, the weights across workers are
        # the same. It is really only needed during testing.
        mean_ws = {}
        for module_id in self._module.keys():
            m = self._module[module_id]
            parameters = convert_to_numpy(self.get_parameters(m))
            mean_ws[module_id] = np.mean([w.mean() for w in parameters])
            results[module_id][LEARNER_STATS_KEY]["mean_weight"] = mean_ws[module_id]

        return results
