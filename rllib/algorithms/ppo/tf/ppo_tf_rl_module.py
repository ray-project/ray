from typing import Mapping, Any

from ray.rllib.algorithms.ppo.ppo_base_rl_module import PPORLModuleBase
from ray.rllib.core.models.base import ACTOR, CRITIC
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

tf1, tf, _ = try_import_tf()


class PPOTfRLModule(PPORLModuleBase, TfRLModule):
    framework: str = "tf2"

    def __init__(self, *args, **kwargs):
        TfRLModule.__init__(self, *args, **kwargs)
        PPORLModuleBase.__init__(self, *args, **kwargs)

    # TODO(Artur): Comment in as soon as we support RNNs from Polciy side
    # @override(RLModule)
    # def get_initial_state(self) -> NestedDict:
    #     if hasattr(self.encoder, "get_initial_state"):
    #         return self.encoder.get_initial_state()
    #     else:
    #         return NestedDict({})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        # if self.encoder.config.shared:
        #     batch[STATE_IN] = None
        # else:
        #     batch[STATE_IN] = {
        #         ACTOR: None,
        #         CRITIC: None,
        #     }
        # batch[SampleBatch.SEQ_LENS] = None

        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Actions
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_dist = self.action_dist_cls.from_logits(action_logits)
        output[SampleBatch.ACTION_DIST] = action_dist.to_deterministic()

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """PPO forward pass during exploration.

        Besides the action distribution, this method also returns the parameters of the
        policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        # if self.encoder.config.shared:
        #     batch[STATE_IN] = None
        # else:
        #     batch[STATE_IN] = {
        #         ACTOR: None,
        #         CRITIC: None,
        #     }
        # batch[SampleBatch.SEQ_LENS] = None

        # Shared encoder
        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])

        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits
        output[SampleBatch.ACTION_DIST] = self.action_dist_cls.from_logits(
            logits=action_logits
        )

        return output

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict):
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        # if self.encoder.config.shared:
        #     batch[STATE_IN] = None
        # else:
        #     batch[STATE_IN] = {
        #         ACTOR: None,
        #         CRITIC: None,
        #     }
        # batch[SampleBatch.SEQ_LENS] = None

        # Shared encoder
        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        pi_out = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        action_logits = pi_out
        action_dist = self.action_dist_cls.from_logits(logits=action_logits)
        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()

        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits
        output[SampleBatch.ACTION_DIST] = action_dist
        output[SampleBatch.ACTION_LOGP] = logp
        output["entropy"] = entropy

        return output
