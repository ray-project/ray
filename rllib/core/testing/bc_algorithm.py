"""Contains example implementation of a custom algorithm.

Note: It doesn't include any real use-case functionality; it only serves as an example
to test the algorithm construction and customization.
"""

from ray.rllib.algorithms import Algorithm, AlgorithmConfig
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.torch.bc_learner import BCTorchLearner
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_learner import BCTfLearner
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec


class BCConfigTest(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or BCAlgorithmTest)

    def get_default_rl_module_spec(self):
        if self.framework_str == "torch":
            return SingleAgentRLModuleSpec(module_class=DiscreteBCTorchModule)
        elif self.framework_str == "tf2":
            return SingleAgentRLModuleSpec(module_class=DiscreteBCTFModule)

    def get_default_learner_class(self):
        if self.framework_str == "torch":
            return BCTorchLearner
        elif self.framework_str == "tf2":
            return BCTfLearner


class BCAlgorithmTest(Algorithm):
    @classmethod
    def get_default_policy_class(cls, config: AlgorithmConfig):
        if config.framework_str == "torch":
            return TorchPolicyV2
        elif config.framework_str == "tf2":
            return EagerTFPolicyV2
        else:
            raise ValueError("Unknown framework: {}".format(config.framework_str))

    def training_step(self):
        # do nothing.
        return {}
