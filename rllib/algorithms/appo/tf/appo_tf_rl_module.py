from typing import List


from ray.rllib.algorithms.ppo.tf.ppo_tf_rl_module import PPOTfRLModule
from ray.rllib.core.models.base import ACTOR
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

TARGET_ACTION_DIST_KEY = "target_action_dist"
TARGET_ACTION_DIST_LOGITS_KEY = "target_action_dist_logits"

class APPOTfRLModule(PPOTfRLModule):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        catalog = self.config.get_catalog()
        self.target_pi = catalog.build_pi_head(framework=self.framework)
        self.target_encoder = catalog.build_actor_critic_encoder(
            framework=self.framework)

    @override(PPOTfRLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
            TARGET_ACTION_DIST_KEY,
        ]
    
    def _forward_train(self, batch: NestedDict):
        outs = super()._forward_train(batch)
        target_pi_inputs_encoded = self.target_encoder(batch)[ENCODER_OUT][ACTOR]
        target_action_dist_logits, _ = self.target_pi(target_pi_inputs_encoded)
        target_action_dist = self.action_dist_cls(target_action_dist_logits)
        outs[TARGET_ACTION_DIST_KEY] = target_action_dist
        outs[TARGET_ACTION_DIST_LOGITS_KEY] = target_action_dist_logits
        return outs

