from typing import Any, Collection, Dict, Optional, Union

from ray.rllib.algorithms.ppo.ppo_rl_module import PPORLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ACTOR, CRITIC, ENCODER_OUT
from ray.rllib.core.rl_module.apis.value_function_api import ValueFunctionAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import StateDict, TensorType

torch, nn = try_import_torch()


class PPOTorchRLModule(TorchRLModule, PPORLModule):
    framework: str = "torch"

    @override(PPORLModule)
    def setup(self):
        super().setup()

        # If not an inference-only module (e.g., for evaluation), set up the
        # parameter names to be removed or renamed when syncing from the state dict.
        if not self.config.inference_only:
            # Set the expected and unexpected keys for the inference-only module.
            self._set_inference_only_state_dict_keys()

    @override(TorchRLModule)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        inference_only: bool = False,
        **kwargs,
    ) -> StateDict:
        state = super(PPOTorchRLModule, self).get_state(
            components=components, not_components=not_components, **kwargs
        )
        # If this module is not for inference, but the state dict is.
        # Note, for stateful modules, we need the full state dict.
        if not self.config.inference_only and not self.is_stateful() and inference_only:
            # Call the local hook to remove parameters not needed in `inference_only`
            # mode.
            state = self._inference_only_get_state_hook(state)
            return state

        # Otherwise, the state dict is for checkpointing or saving the model.
        return state

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

    # TODO (sven): Try to move entire GAE computation into PPO's loss function (similar
    #  to IMPALA's v-trace architecture). This would also get rid of the second
    #  Connector pass currently necessary.
    @override(ValueFunctionAPI)
    def compute_values(self, batch: Dict[str, Any]) -> TensorType:
        # Separate vf-encoder.
        if hasattr(self.encoder, "critic_encoder"):
            if self.is_stateful():
                # The recurrent encoders expect a `(state_in, h)`  key in the
                # input dict while the key returned is `(state_in, critic, h)`.
                batch[Columns.STATE_IN] = batch[Columns.STATE_IN][CRITIC]
            encoder_outs = self.encoder.critic_encoder(batch)[ENCODER_OUT]
        # Shared encoder.
        else:
            encoder_outs = self.encoder(batch)[ENCODER_OUT][CRITIC]
        # Value head.
        vf_out = self.vf(encoder_outs)
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)

    @override(TorchRLModule)
    def _set_inference_only_state_dict_keys(self) -> None:
        # Get the model_parameters.
        state_dict = self.state_dict()
        # Note, these keys are only known to the learner module. Furthermore,
        # we want this to be run once during setup and not for each worker.
        self._inference_only_state_dict_keys["unexpected_keys"] = [
            name
            for name in state_dict
            if "vf" in name or name.startswith("encoder.critic_encoder")
        ]

    @override(TorchRLModule)
    def _inference_only_get_state_hook(
        self, state_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        # If we have keys in the state dict to take care of.
        if self._inference_only_state_dict_keys:
            # If we have unexpected keys remove them.
            if self._inference_only_state_dict_keys.get("unexpected_keys"):
                for param in self._inference_only_state_dict_keys["unexpected_keys"]:
                    del state_dict[param]
        return state_dict
