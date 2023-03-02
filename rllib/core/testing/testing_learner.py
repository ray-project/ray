from typing import Mapping, Any
import numpy as np

from ray.rllib.core.learner.learner import Learner
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.metrics import LEARNER_STATS_KEY


class BaseTestingLearner(Learner):
    def compile_results(
        self,
        batch: NestedDict,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        postprocessed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch, fwd_out, postprocessed_loss, postprocessed_gradients
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
