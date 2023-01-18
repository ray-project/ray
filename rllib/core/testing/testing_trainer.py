from typing import Mapping, Any
import numpy as np

from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy

class BaseTestingTrainer(RLTrainer):

    def compile_results(self, batch: NestedDict, fwd_out: Mapping[str, Any], postprocessed_loss: Mapping[str, Any], post_processed_gradients: Mapping[str, Any]) -> Mapping[str, Any]:
        results = super().compile_results(batch, fwd_out, postprocessed_loss, post_processed_gradients)
        # this is to check if in the multi-gpu case, the weights across workers are
        # the same. It is really only needed during testing.
        mean_ws = {}
        for module_id in self._module.keys():
            m = self._module[module_id]
            parameters = convert_to_numpy(self.get_parameters(m))
            mean_ws[module_id] = np.mean([w.mean() for w in parameters])
        results["mean_weight"] = mean_ws

        return results