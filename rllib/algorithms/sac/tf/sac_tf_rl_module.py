from typing import Any, Dict

from ray.rllib.algorithms.sac.sac_learner import QF_PREDS, QF_TARGET_PREDS
from ray.rllib.algorithms.sac.sac_rl_module import SACRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.core.models.base import ACTOR, CRITIC, ENCODER_OUT, STATE_OUT, STATE_IN
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

_, tf, _ = try_import_tf()


class SACTfRLModule(TfRLModule, SACRLModule):
    framework: str = "tf2"

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Dict[str, Any]:
        output = {}

        # Encoder forward pass.
        pi_and_qf_encoder_outs = self.pi_and_qf_encoder(batch)        

        # Pi head.
        output[SampleBatch.ACTION_DIST_INPUTS] = self.pi(
            pi_and_qf_encoder_outs[ENCODER_OUT][ACTOR]
        )

        return output

    def _forward_exploration(self, batch: NestedDict) -> Dict[str, Any]:
        self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Dict(str, Any):
        output = {}

        # SAC needs also Q function values and action logits for next observations.
        batch_next = NestedDict({SampleBatch.NEXT_OBS: {batch[SampleBatch.NEXT_OBS]}})
        batch_curr = NestedDict({SampleBatch.OBS: {batch[SampleBatch.OBS]}})

        # Encoder forward pass.
        pi_and_qf_encoder_outs = self.pi_and_qf_encoder(batch_curr)
        qf_target_encoder_outs = self.qf_target_encoder(batch_curr)
        # Also encode the next observations.
        pi_and_qf_encoder_next_outs = self.pi_and_qf_encoder(batch_next)

        # Q heads.
        qf_out = self.qf(pi_and_qf_encoder_outs[ENCODER_OUT][CRITIC])
        qf_target_out = self.qf_target(qf_target_encoder_outs[ENCODER_OUT])
        # Also get the Q-value for the next observations.
        qf_next_out = self.qf(pi_and_qf_encoder_next_outs[ENCODER_OUT][CRITIC])
        # Squeeze out last dimension (Q function node).
        output[QF_PREDS] = tf.squeeze(qf_out, axis=-1)
        output[QF_TARGET_PREDS] = tf.squeeze(qf_target_out, axis=-1)
        output["qf_preds_next"] = tf.squeeze(qf_next_out, axis=-1)

        # Policy head.
        action_logits = self.pi(pi_and_qf_encoder_outs[ENCODER_OUT][ACTOR])
        # Also get the action logits for the next observations.
        action_logits_next = self.pi(pi_and_qf_encoder_next_outs[ENCODER_OUT][ACTOR])
        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits
        output["action_dist_inputs_next"] = action_logits_next

        return output
