from typing import Mapping, Any, List

from ray.rllib.algorithms.ppo.ppo_base_rl_module import PPORLModuleBase
from ray.rllib.core.models.base import ACTOR, CRITIC, STATE_IN
from ray.rllib.core.models.tf.encoder import ENCODER_OUT
from ray.rllib.models.tf.tf_distributions import (
    TfCategorical,
    TfDiagGaussian,
    TfDeterministic,
)

from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.tf.tf_rl_module import TfRLModule
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

tf1, tf, _ = try_import_tf()
tf1.enable_eager_execution()


class PPOTfRLModule(PPORLModuleBase, TfRLModule):
    framework = "tf"

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
    def input_specs_train(self) -> List[str]:
        return [SampleBatch.OBS, SampleBatch.ACTIONS]

    @override(RLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
        ]

    @override(RLModule)
    def input_specs_exploration(self):
        return []

    @override(RLModule)
    def output_specs_exploration(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST,
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST_INPUTS,
        ]

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict({SampleBatch.ACTION_DIST: TfDeterministic})

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        if self.encoder.config.shared:
            batch[STATE_IN] = None
        else:
            batch[STATE_IN] = {
                ACTOR: None,
                CRITIC: None,
            }
        batch[SampleBatch.SEQ_LENS] = None

        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Actions
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        if self._is_discrete:
            action = tf.math.argmax(action_logits, axis=-1)
        else:
            action, _ = tf.split(action_logits, num_or_size_splits=2, axis=1)

        action_dist = TfDeterministic(loc=action)
        output[SampleBatch.ACTION_DIST] = action_dist

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
        if self.encoder.config.shared:
            batch[STATE_IN] = None
        else:
            batch[STATE_IN] = {
                ACTOR: None,
                CRITIC: None,
            }
        batch[SampleBatch.SEQ_LENS] = None

        # Shared encoder
        encoder_outs = self.encoder(batch)
        # TODO (Artur): Un-uncomment once Policy supports RNN
        # output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Value head
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        output[SampleBatch.VF_PREDS] = tf.squeeze(vf_out, axis=-1)

        # Policy head
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        if self._is_discrete:
            action_dist = TfCategorical(logits=action_logits)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"logits": action_logits}
        else:
            loc, log_std = tf.split(action_logits, num_or_size_splits=2, axis=1)
            scale = tf.math.exp(log_std)
            action_dist = TfDiagGaussian(loc=loc, scale=scale)
            output[SampleBatch.ACTION_DIST_INPUTS] = {"loc": loc, "scale": scale}

        output[SampleBatch.ACTION_DIST] = action_dist

        return output

    @override(TfRLModule)
    def _forward_train(self, batch: NestedDict):
        output = {}

        # TODO (Artur): Remove this once Policy supports RNN
        if self.encoder.config.shared:
            batch[STATE_IN] = None
        else:
            batch[STATE_IN] = {
                ACTOR: None,
                CRITIC: None,
            }
        batch[SampleBatch.SEQ_LENS] = None

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
        if self._is_discrete:
            action_dist = TfCategorical(logits=action_logits)
        else:
            loc, log_std = tf.split(action_logits, num_or_size_splits=2, axis=1)
            scale = tf.math.exp(log_std)
            action_dist = TfDiagGaussian(loc=loc, scale=scale)

        logp = action_dist.logp(batch[SampleBatch.ACTIONS])
        entropy = action_dist.entropy()
        output[SampleBatch.ACTION_DIST] = action_dist
        output[SampleBatch.ACTION_LOGP] = logp
        output["entropy"] = entropy

        return output
