from typing import Any, Dict

from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ACTOR, CRITIC, ENCODER_OUT
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class PPOTorchRLModule(TorchRLModule, PPORLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        output = {}

        # Encoder forward pass.
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Pi head.
        output[Columns.ACTION_DIST_INPUTS] = self.pi(encoder_outs[ENCODER_OUT][ACTOR])

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """PPO forward pass during exploration.

        Besides the action distribution, this method also returns the parameters of
        the policy distribution to be used for computing KL divergence between the old
        policy and the new policy during training.
        """
        # TODO (sven): Make this the only behavior once PPO has been migrated
        #  to new API stack (including EnvRunners).
        if self.config.model_config_dict.get("uses_new_env_runners"):
            return self._forward_inference(batch)

        output = {}

        # Shared encoder
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Value head
        if not self.config.inference_only:
            # If not for inference/exploration only, we need to compute the
            # value function.
            vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
            output[Columns.VF_PREDS] = vf_out.squeeze(-1)

        # Policy head
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        output[Columns.ACTION_DIST_INPUTS] = action_logits

        return output

    @override(RLModule)
    def _forward_train(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.inference_only:
            raise RuntimeError(
                "Trying to train a module that is not a learner module. Set the "
                "flag `inference_only=False` when building the module."
            )
        output = {}

        # Shared encoder.
        encoder_outs = self.encoder(batch)
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        # Value head.
        vf_out = self.vf(encoder_outs[ENCODER_OUT][CRITIC])
        # Squeeze out last dim (value function node).
        output[Columns.VF_PREDS] = vf_out.squeeze(-1)

        # Policy head.
        action_logits = self.pi(encoder_outs[ENCODER_OUT][ACTOR])
        output[Columns.ACTION_DIST_INPUTS] = action_logits

        return output

    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, Any]) -> TensorType:
        # Separate vf-encoder.
        if hasattr(self.encoder, "critic_encoder"):
            batch_ = batch
            if self.is_stateful():
                # The recurrent encoders expect a `(state_in, h)`  key in the
                # input dict while the key returned is `(state_in, critic, h)`.
                batch_ = batch.copy()
                batch_[Columns.STATE_IN] = batch[Columns.STATE_IN][CRITIC]
            encoder_outs = self.encoder.critic_encoder(batch_)[ENCODER_OUT]
        # Shared encoder.
        else:
            encoder_outs = self.encoder(batch)[ENCODER_OUT][CRITIC]
        # Value head.
        vf_out = self.vf(encoder_outs)
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)
