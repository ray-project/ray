from typing import List

from ray.rllib.algorithms.appo.appo_learner import (
    OLD_ACTION_DIST_LOGITS_KEY,
)
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.models.base import ACTOR
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict


class APPOTorchRLModule(PPOTorchRLModule, RLModuleWithTargetNetworksInterface):
    def setup(self):
        super().setup()
        catalog = self.config.get_catalog()
        # Old pi and old encoder are the "target networks" that are used for
        # the stabilization of the updates of the current pi and encoder.
        self.old_pi = catalog.build_pi_head(framework=self.framework)
        self.old_encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.old_pi.load_state_dict(self.pi.state_dict())
        self.old_encoder.load_state_dict(self.encoder.state_dict())
        self.old_pi.trainable = False
        self.old_encoder.trainable = False

    @override(RLModuleWithTargetNetworksInterface)
    def get_target_network_pairs(self):
        return [(self.old_pi, self.pi), (self.old_encoder, self.encoder)]

    @override(PPOTorchRLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST_INPUTS,
            OLD_ACTION_DIST_LOGITS_KEY,
            SampleBatch.VF_PREDS,
        ]

    @override(PPOTorchRLModule)
    def _forward_train(self, batch: NestedDict):
        outs = super()._forward_train(batch)
        old_pi_inputs_encoded = self.old_encoder(batch)[ENCODER_OUT][ACTOR]
        old_action_dist_logits = self.old_pi(old_pi_inputs_encoded)
        outs[OLD_ACTION_DIST_LOGITS_KEY] = old_action_dist_logits
        return outs
