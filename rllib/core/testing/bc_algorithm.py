"""Contains example implementation of a custom algorithm.

Note: It doesn't include any real use-case functionality; it only serves as an example
to test the algorithm construction and customization.
"""

from ray.rllib.algorithms import Algorithm, AlgorithmConfig
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import ResultDict


class BCConfigTest(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or BCAlgorithmTest)

    def get_default_rl_module_spec(self):
        if self.framework_str == "torch":
            return RLModuleSpec(module_class=DiscreteBCTorchModule)

    def get_default_learner_class(self):
        if self.framework_str == "torch":
            return BCTorchLearner


class BCAlgorithmTest(Algorithm):
    @classmethod
    def get_default_policy_class(cls, config: AlgorithmConfig):
        if config.framework_str == "torch":
            return TorchPolicyV2
        else:
            raise ValueError("Unknown framework: {}".format(config.framework_str))

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # do nothing.
        return {}
