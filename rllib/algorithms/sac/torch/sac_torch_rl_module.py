from typing import Any, Dict, List, Tuple

from ray.rllib.algorithms.sac.sac_learner import QF_PREDS, QF_TARGET_PREDS
from ray.rllib.algorithms.sac.sac_rl_module import SACRLModule
from ray.rllib.core.models.base import ENCODER_OUT
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
        self.log_alpha = nn.Parameter(torch.Tensor(self.alpha))


    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Dict[str, Any]:
        output = {}

        # Pi encoder forward pass.
        pi_encoder_outs = self.pi_encoder(batch)

        # Pi head.
        output[SampleBatch.ACTION_DIST_INPUTS] = self.pi(pi_encoder_outs[ENCODER_OUT])

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Dict[str, Any]:
        return self._forward_inference(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        output = {}

        # SAC needs also Q function values and action logits for next observations.
        # TODO (simon): Check, if we need to override the Encoder input_sp
        batch_curr = NestedDict({SampleBatch.OBS: batch[SampleBatch.OBS]})
        # TODO (sven): If we deprecate 'SampleBatch.NEXT_OBS` we need to change
        # # this.
        batch_next = NestedDict({SampleBatch.OBS: batch[SampleBatch.NEXT_OBS]})

        # Encoder forward passes.
        pi_encoder_outs = self.pi_encoder(batch_curr)
        batch_curr.update(
            {
                SampleBatch.OBS: torch.concat(
                    (batch_curr[SampleBatch.OBS], batch[SampleBatch.ACTIONS]), dim=-1
                )
            }
        )
        qf_encoder_outs = self.qf_encoder(batch_curr)
        # Also encode the next observations (and next actions for the Q net).
        pi_encoder_next_outs = self.pi_encoder(batch_next)

        # Q head.
        qf_out = self.qf(qf_encoder_outs[ENCODER_OUT])
        # Squeeze out last dimension (Q function node).
        output[QF_PREDS] = qf_out.squeeze(dim=-1)

        # Policy head.
        action_logits = self.pi(pi_encoder_outs[ENCODER_OUT])
        # Also get the action logits for the next observations.
        action_logits_next = self.pi(pi_encoder_next_outs[ENCODER_OUT])
        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits
        output["action_dist_inputs_next"] = action_logits_next

        # Return the network outputs.
        return output

    def _qf_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through Q network.

        Note, this is only used in training.
        """
        output = {}

        # Construct batch. Note, we need to feed observations and actions.
        qf_batch = NestedDict(
            {
                SampleBatch.OBS: torch.concat(
                    (batch[SampleBatch.OBS], batch[SampleBatch.ACTIONS]), dim=-1
                )
            }
        )
        # Encoder forward pass.
        qf_encoder_outs = self.qf_encoder(qf_batch)

        # Q head forward pass.
        qf_out = self.qf(qf_encoder_outs[ENCODER_OUT])
        # Squeeze out the last dimension (Q function node).
        output[QF_PREDS] = qf_out.squeeze(dim=-1)

        # Return Q values.
        return output

    def _qf_target_forward_train(self, batch: NestedDict) -> Dict[str, Any]:
        """Forward pass through Q target network.

        Note, this is only used in training.
        """
        output = {}

        # Construct batch. Note, we need to feed observations and actions.
        qf_target_batch = NestedDict(
            {
                SampleBatch.OBS: torch.concat(
                    (batch[SampleBatch.OBS], batch[SampleBatch.ACTIONS]), dim=-1
                )
            }
        )
        # Encoder forward pass.
        qf_target_encoder_outs = self.qf_target_encoder(qf_target_batch)

        # Target Q head forward pass.
        qf_target_out = self.qf_target(qf_target_encoder_outs[ENCODER_OUT])
        # Squeeze out the last dimension (Q function node).
        output[QF_PREDS] = qf_target_out.squeeze(dim=-1)

        # Return target Q values.
        return output

    @override(RLModuleWithTargetNetworksInterface)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns target Q and Q network(s) to update the target network(s)."""
        return [(self.qf_target_encoder, self.qf_encoder), (self.qf_target, self.qf)]
