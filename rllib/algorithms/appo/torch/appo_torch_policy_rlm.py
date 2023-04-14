import logging

from ray.rllib.algorithms.ppo.ppo_torch_policy import validate_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


# TODO: Remove once we have a RLModule capable sampler class that can replace
#  `Policy.compute_actions_from_input_dict()`.
class APPOTorchPolicyWithRLModule(TorchPolicyV2):
    def __init__(self, observation_space, action_space, config):
        validate_config(config)
        TorchPolicyV2.__init__(self, observation_space, action_space, config)
        self._initialize_loss_from_dummy_batch()

    @override(TorchPolicyV2)
    def loss(self, model, dist_class, train_batch):
        self.stats = {}
        train_batch[SampleBatch.ACTION_LOGP]
        train_batch[SampleBatch.ACTIONS]
        train_batch[SampleBatch.REWARDS]
        train_batch[SampleBatch.TERMINATEDS]
        return 0.0

    @override(TorchPolicyV2)
    def get_batch_divisibility_req(self) -> int:
        return self.config["rollout_fragment_length"]
