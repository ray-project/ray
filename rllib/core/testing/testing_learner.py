from typing import Type

import numpy as np

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.marl_module import (
    MultiAgentRLModule,
    MultiAgentRLModuleSpec,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import RLModuleSpec


class BaseTestingAlgorithmConfig(AlgorithmConfig):
    # A test setting to activate metrics on mean weights.
    report_mean_weights: bool = True

    @override(AlgorithmConfig)
    def get_default_learner_class(self) -> Type["Learner"]:
        if self.framework_str == "tf2":
            from ray.rllib.core.testing.tf.bc_learner import BCTfLearner

            return BCTfLearner
        elif self.framework_str == "torch":
            from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner

            return BCTorchLearner
        else:
            raise ValueError(f"Unsupported framework: {self.framework_str}")

    @override(AlgorithmConfig)
    def get_default_rl_module_spec(self) -> "RLModuleSpec":
        if self.framework_str == "tf2":
            from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule

            cls = DiscreteBCTFModule
        elif self.framework_str == "torch":
            from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule

            cls = DiscreteBCTorchModule
        else:
            raise ValueError(f"Unsupported framework: {self.framework_str}")

        spec = SingleAgentRLModuleSpec(
            module_class=cls,
            model_config_dict={"fcnet_hiddens": [32]},
        )

        if self.is_multi_agent():
            # TODO (Kourosh): Make this more multi-agent for example with policy ids
            #  "1" and "2".
            return MultiAgentRLModuleSpec(
                marl_module_class=MultiAgentRLModule,
                module_specs={DEFAULT_MODULE_ID: spec},
            )
        else:
            return spec


class BaseTestingLearner(Learner):
    @override(Learner)
    def compute_loss_for_module(self, *, module_id, config=None, batch, fwd_out):
        # This is to check if in the multi-gpu case, the weights across workers are
        # the same. It is really only needed during testing.
        if config.report_mean_weights:
            parameters = convert_to_numpy(self.get_parameters(self.module[module_id]))
            mean_ws = np.mean([w.mean() for w in parameters])
            self.metrics.log_value((module_id, "mean_weight"), mean_ws, window=1)
