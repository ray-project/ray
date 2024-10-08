from typing import Any, Dict

from ray.rllib.algorithms.sac.sac_learner import (
    ACTION_DIST_INPUTS_NEXT,
    QF_PREDS,
    QF_TWIN_PREDS,
)
from ray.rllib.algorithms.sac.sac_rl_module import SACRLModule
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT, Encoder, Model
from ray.rllib.core.rl_module.apis.target_network_api import TargetNetworkAPI
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class SACTorchRLModule(TorchRLModule, SACRLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: Dict) -> Dict[str, Any]:
        output = {}

        # Pi encoder forward pass.
        pi_encoder_outs = self.pi_encoder(batch)

        # Pi head.
        output[Columns.ACTION_DIST_INPUTS] = self.pi(pi_encoder_outs[ENCODER_OUT])

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
        batch_curr = {Columns.OBS: batch[Columns.OBS]}
        batch_next = {Columns.OBS: batch[Columns.NEXT_OBS]}

        # Encoder forward passes.
        pi_encoder_outs = self.pi_encoder(batch_curr)

        # Also encode the next observations (and next actions for the Q net).
        pi_encoder_next_outs = self.pi_encoder(batch_next)

        # Q-network(s) forward passes.
        batch_curr.update({Columns.ACTIONS: batch[Columns.ACTIONS]})
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
        output[Columns.ACTION_DIST_INPUTS] = action_logits
        output[ACTION_DIST_INPUTS_NEXT] = action_logits_next

        # Get the train action distribution for the current policy and current state.
        # This is needed for the policy (actor) loss in SAC.
        action_dist_class = self.get_train_action_dist_cls()
        action_dist_curr = action_dist_class.from_logits(action_logits)
        # Get the train action distribution for the current policy and next state.
        # For the Q (critic) loss in SAC, we need to sample from the current policy at
        # the next state.
        action_dist_next = action_dist_class.from_logits(action_logits_next)

        # Sample actions for the current state. Note that we need to apply the
        # reparameterization trick (`rsample()` instead of `sample()`) to avoid the
        # expectation over actions.
        actions_resampled = action_dist_curr.rsample()
        # Compute the log probabilities for the current state (for the critic loss).
        output["logp_resampled"] = action_dist_curr.logp(actions_resampled)

        # Sample actions for the next state.
        actions_next_resampled = action_dist_next.sample().detach()
        # Compute the log probabilities for the next state.
        output["logp_next_resampled"] = (
            action_dist_next.logp(actions_next_resampled)
        ).detach()

        # Compute Q-values for the current policy in the current state with
        # the sampled actions.
        q_batch_curr = {
            Columns.OBS: batch[Columns.OBS],
            Columns.ACTIONS: actions_resampled,
        }
        # Make sure we perform a "straight-through gradient" pass here,
        # ignoring the gradients of the q-net, however, still recording
        # the gradients of the policy net (which was used to rsample the actions used
        # here). This is different from doing `.detach()` or `with torch.no_grads()`,
        # as these two methds would fully block all gradient recordings, including
        # the needed policy ones.
        all_params = list(self.qf.parameters()) + list(self.qf_encoder.parameters())
        if self.twin_q:
            all_params += list(self.qf_twin.parameters()) + list(
                self.qf_twin_encoder.parameters()
            )

        for param in all_params:
            param.requires_grad = False
        output["q_curr"] = self.compute_q_values(q_batch_curr)
        for param in all_params:
            param.requires_grad = True

        # Compute Q-values from the target Q network for the next state with the
        # sampled actions for the next state.
        q_batch_next = {
            Columns.OBS: batch[Columns.NEXT_OBS],
            Columns.ACTIONS: actions_next_resampled,
        }
        output["q_target_next"] = self.forward_target(q_batch_next).detach()

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
                and actions under the key `Columns.OBS`.
            encoder: An `Encoder` model for the Q state-action encoder.
            head: A `Model` for the Q head.

        Returns:
            The estimated (single) Q-value.
        """
        # Construct batch. Note, we need to feed observations and actions.
        qf_batch = {
            Columns.OBS: torch.concat(
                (batch[Columns.OBS], batch[Columns.ACTIONS]), dim=-1
            )
        }
        # Encoder forward pass.
        qf_encoder_outs = encoder(qf_batch)

        # Q head forward pass.
        qf_out = head(qf_encoder_outs[ENCODER_OUT])

        # Squeeze out the last dimension (Q function node).
        return qf_out.squeeze(dim=-1)
