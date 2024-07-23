from typing import Any, Collection, Dict, Optional, Union

from ray.rllib.algorithms.sac.sac_learner import (
    ACTION_DIST_INPUTS_NEXT,
    QF_PREDS,
    QF_TWIN_PREDS,
)
from ray.rllib.algorithms.sac.sac_rl_module import SACRLModule
from ray.rllib.core.models.base import ENCODER_OUT, Encoder, Model
from ray.rllib.core.rl_module.apis.target_network_api import TargetNetworkAPI
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import StateDict

torch, nn = try_import_torch()


class SACTorchRLModule(TorchRLModule, SACRLModule):
    framework: str = "torch"

    @override(SACRLModule)
    def setup(self):
        super().setup()

        # If not an inference-only module (e.g., for evaluation), set up the
        # parameter names to be removed or renamed when syncing from the state dict
        # when syncing.
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
        state = super(SACTorchRLModule, self).get_state(
            components=components, not_components=not_components, **kwargs
        )
        # If this module is not for inference, but the state dict is.
        if not self.config.inference_only and inference_only:
            # Call the local hook to remove or rename the parameters.
            return self._inference_only_get_state_hook(state)
        # Otherwise, the state dict is for checkpointing or saving the model.
        else:
            # Return the state dict as is.
            return state

    @override(RLModule)
    def _forward_inference(self, batch: Dict) -> Dict[str, Any]:
        output = {}

        # Pi encoder forward pass.
        pi_encoder_outs = self.pi_encoder(batch)

        # Pi head.
        output[SampleBatch.ACTION_DIST_INPUTS] = self.pi(pi_encoder_outs[ENCODER_OUT])

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: Dict, **kwargs) -> Dict[str, Any]:
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch: Dict) -> Dict[str, Any]:
        if self.config.inference_only:
            raise RuntimeError(
                "Trying to train a module that is not a learner module. Set the "
                "flag `inference_only=False` when building the module."
            )
        output = {}

        # SAC needs also Q function values and action logits for next observations.
        batch_curr = {SampleBatch.OBS: batch[SampleBatch.OBS]}
        batch_next = {SampleBatch.OBS: batch[SampleBatch.NEXT_OBS]}

        # Encoder forward passes.
        pi_encoder_outs = self.pi_encoder(batch_curr)

        # Also encode the next observations (and next actions for the Q net).
        pi_encoder_next_outs = self.pi_encoder(batch_next)

        # Q-network(s) forward passes.
        batch_curr.update({SampleBatch.ACTIONS: batch[SampleBatch.ACTIONS]})
        output[QF_PREDS] = self._qf_forward_train_helper(
            batch_curr, self.qf_encoder, self.qf
        )  # self._qf_forward_train(batch_curr)[QF_PREDS]
        # If necessary make a forward pass through the twin Q network.
        if self.twin_q:
            output[QF_TWIN_PREDS] = self._qf_forward_train_helper(
                batch_curr, self.qf_twin_encoder, self.qf_twin
            )

        # Policy head.
        action_logits = self.pi(pi_encoder_outs[ENCODER_OUT])
        # Also get the action logits for the next observations.
        action_logits_next = self.pi(pi_encoder_next_outs[ENCODER_OUT])
        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits
        output[ACTION_DIST_INPUTS_NEXT] = action_logits_next

        # Return the network outputs.
        return output

    @override(TargetNetworkAPI)
    def forward_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        target_qvs = self._qf_forward_train_helper(
            batch, self.target_qf_encoder, self.target_qf
        )

        # If a twin Q network should be used, calculate twin Q-values and use the
        # minimum.
        if self.twin_q:
            target_qvs = torch.min(
                target_qvs,
                self._qf_forward_train_helper(
                    batch, self.target_qf_twin_encoder, self.target_qf_twin
                ),
            )

        return target_qvs

    # TODO (sven): Create `ValueFunctionAPI` and subclass from this.
    def compute_q_values(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        qvs = self._qf_forward_train_helper(batch, self.qf_encoder, self.qf)
        # If a twin Q network should be used, calculate twin Q-values and use the
        # minimum.
        if self.twin_q:
            qvs = torch.min(
                qvs,
                self._qf_forward_train_helper(
                    batch, self.qf_twin_encoder, self.qf_twin
                ),
            )
        return qvs

    @override(SACRLModule)
    def _qf_forward_train_helper(
        self, batch: Dict[str, Any], encoder: Encoder, head: Model
    ) -> Dict[str, Any]:
        """Executes the forward pass for Q networks.

        Args:
            batch: Dict containing a concatenated tensor with observations
                and actions under the key `SampleBatch.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            The estimated (single) Q-value.
        """
        # Construct batch. Note, we need to feed observations and actions.
        qf_batch = {
            SampleBatch.OBS: torch.concat(
                (batch[SampleBatch.OBS], batch[SampleBatch.ACTIONS]), dim=-1
            )
        }
        # Encoder forward pass.
        qf_encoder_outs = encoder(qf_batch)

        # Q head forward pass.
        qf_out = head(qf_encoder_outs[ENCODER_OUT])

        # Squeeze out the last dimension (Q function node).
        return qf_out.squeeze(dim=-1)

    @override(TorchRLModule)
    def _set_inference_only_state_dict_keys(self) -> None:
        # Get the model parameters.
        state_dict = self.state_dict()
        # Note, these keys are only known to the learner module. Furthermore,
        # we want this to be run once during setup and not for each worker.
        # TODO (simon): Check, if we can also remove the value network.
        self._inference_only_state_dict_keys["unexpected_keys"] = [
            name for name in state_dict if "qf" in name
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
            # If we have expected keys, rename.
            if self._inference_only_state_dict_keys.get("expected_keys"):
                for param in self._inference_only_state_dict_keys["expected_keys"]:
                    state_dict[
                        self._inference_only_state_dict_keys["expected_keys"][param]
                    ] = state_dict.pop(param)
        return state_dict
