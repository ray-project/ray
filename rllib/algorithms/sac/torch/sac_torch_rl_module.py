from typing import Any, Dict, List, Tuple

from ray.rllib.algorithms.sac.sac_rl_module import (
    ACTION_DIST_INPUTS_NEXT,
    QF_PREDS,
    QF_TWIN_PREDS,
)
from ray.rllib.algorithms.sac.sac_rl_module import SACRLModule
from ray.rllib.core.models.base import ENCODER_OUT, Encoder, Model
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import NetworkType


torch, nn = try_import_torch()


class SACTorchRLModule(TorchRLModule, SACRLModule):
    framework: str = "torch"

    @override(SACRLModule)
    def setup(self):
        super().setup()

        # If not an inference-only module (e.g., for evaluation), set up the
        # parameter names to be removed or renamed when syncing from the state dict
        # when synching.
        if not self.inference_only:
            # Set the expected and unexpected keys for the inference-only module.
            self._set_inference_only_state_dict_keys()

    @override(TorchRLModule)
    def get_state(self, inference_only: bool = False) -> Dict[str, Any]:
        state_dict = self.state_dict()
        # If this module is not for inference, but the state dict is.
        if not self.inference_only and inference_only:
            # Call the local hook to remove or rename the parameters.
            return self._inference_only_get_state_hook(state_dict)
        # Otherwise, the state dict is for checkpointing or saving the model.
        else:
            # Return the state dict as is.
            return state_dict

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Dict[str, Any]:
        output = {}

        # Pi encoder forward pass.
        pi_encoder_outs = self.pi_encoder(batch)

        # Pi head.
        output[SampleBatch.ACTION_DIST_INPUTS] = self.pi(pi_encoder_outs[ENCODER_OUT])

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict, **kwargs) -> Dict[str, Any]:
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        if self.inference_only:
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

        # Q-network forward pass.
        # TODO (simon): Use here `_qf_forward_train` instead of the helper.
        batch_curr.update({SampleBatch.ACTIONS: batch[SampleBatch.ACTIONS]})
        output[QF_PREDS] = self._qf_forward_train(batch_curr)[QF_PREDS]
        # If necessary make a forward pass through the twin Q network.
        if self.twin_q:
            output[QF_TWIN_PREDS] = self._qf_forward_train_helper(
                batch_curr, self.qf_twin_encoder, self.qf_twin
            )[QF_PREDS]

        # Policy head.
        action_logits = self.pi(pi_encoder_outs[ENCODER_OUT])
        # Also get the action logits for the next observations.
        action_logits_next = self.pi(pi_encoder_next_outs[ENCODER_OUT])
        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits
        output[ACTION_DIST_INPUTS_NEXT] = action_logits_next

        # Return the network outputs.
        return output

    @override(SACRLModule)
    def _qf_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through Q network.

        Note, this is only used in training.
        """
        return self._qf_forward_train_helper(batch, self.qf_encoder, self.qf)

    @override(SACRLModule)
    def _qf_target_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through Q target network.

        Note, this is only used in training.
        """
        return self._qf_forward_train_helper(
            batch, self.qf_target_encoder, self.qf_target
        )

    @override(SACRLModule)
    def _qf_twin_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through twin Q network.

        Note, this is only used in training if `twin_q=True`.
        """
        return (
            self._qf_forward_train_helper(batch, self.qf_twin_encoder, self.qf_twin)
            if self.twin_q
            else {}
        )

    @override(SACRLModule)
    def _qf_target_twin_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through twin Q target network.

        Note, this is only used in training if `twin_q=True`.
        """
        return (
            self._qf_forward_train_helper(
                batch, self.qf_target_twin_encoder, self.qf_target_twin
            )
            if self.twin_q
            else {}
        )

    @override(RLModuleWithTargetNetworksInterface)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns target Q and Q network(s) to update the target network(s)."""
        return [
            (self.qf_target_encoder, self.qf_encoder),
            (self.qf_target, self.qf),
        ] + (
            # If we have twin networks we need to update them, too.
            [
                (self.qf_target_twin_encoder, self.qf_twin_encoder),
                (self.qf_target_twin, self.qf_twin),
            ]
            if self.twin_q
            else []
        )

    @override(SACRLModule)
    def _qf_forward_train_helper(
        self, batch: NestedDict, encoder: Encoder, head: Model
    ) -> Dict[str, Any]:
        """Executes the forward pass for Q networks.

        Args:
            batch: NestedDict containing a concatencated tensor with observations
                and actions under the key `SampleBatch.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            A `dict` cotnaining the estimated Q-values in the key `QF_PREDS`.
        """
        output = {}

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
        output[QF_PREDS] = qf_out.squeeze(dim=-1)

        # Return Q values.
        return output

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
