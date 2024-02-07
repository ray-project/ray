from typing import Dict

from ray.rllib.algorithms.dqn.dqn_rainbow_rl_module import DQNRainbowRLModule
from ray.rllib.algorithms.sac.sac_rl_module import QF_PREDS
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


# TODO (simon): Maybe call logits qf_logits, and probs qf_probs.
class DQNRainbowTorchRLModule(TorchRLModule, DQNRainbowRLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: Dict) -> Dict:
        output = {}

        # Encoder forward pass.
        encoder_outs = self.base_encoder(batch)

        # Q-head.
        qf_head_output = self.qf(encoder_outs[ENCODER_OUT])

        # In inference we only need the Q-values.
        output[QF_PREDS] = qf_head_output[QF_PREDS]

        return output

    @override(RLModule)
    def _forward_exploration(self, batch: Dict) -> Dict:
        output = {}

        return output

    @override(RLModule)
    def _forward_train(self, batch: Dict) -> Dict:
        output = {}

        return output

    def qf(self, batch: Dict) -> Dict:
        """Computes Q-values."""
        output = {}

        if self.is_dueling:
            af_output = self.af(batch)
            vf_output = self.vf(batch)
            if self.num_atoms > 1:
                af_dist_output = self.af_dist(af_output)
                # Center the advantage stream distribution.
                centered_af_logits = af_dist_output["logits"] - af_dist_output[
                    "logits"
                ].mean(dim=1)
                # Calculate the Q-value distribution by adding advantage and
                # value stream.
                qf_logits = centered_af_logits + vf_output
                # Calculate probabilites for the Q-value distribution along
                # the support given by the atoms.
                qf_probs = nn.functional.softmax(qf_logits, dim=-1)
                # Calculate the Q-values by the weighted sum over the atoms.
                output[QF_PREDS] = torch.sum(af_dist_output["atoms"] * qf_probs, dim=1)
                output["logits"] = qf_logits
                output["probs"] = qf_probs
            else:
                # Center advantages.
                af_output -= torch.nan_to_num(af_output, neginf=torch.nan).nanmean(
                    dim=1
                )
                # TODO (simon): Check if unsqueeze is necessary.
                # Add advantage and value stream. Note, we broadcast here.
                output[QF_PREDS] = af_output + vf_output
        else:
            output[QF_PREDS] = self.af(batch)

        return output

    def af_dist(self, batch) -> Dict:
        """Compute the advantage distribution."""
        output = {}
        if self.num_atoms > 1:
            # Distributional Q-learning uses a discrete support `z`
            # to represent the action value distribution.
            # TODO (simon): Check, if we still need here the device for torch.
            z = torch.arange(0.0, self.num_atoms, dtype=torch.float32).to(
                batch.device,
            )
            # Rescale the support.
            z = self.v_min + z * (self.v_max - self.v_min) / float(self.num_atoms - 1)
            # Reshape the action values.
            logits_per_action_per_atom = torch.reshape(
                batch,
                shape=(-1, self.config.action_space.n, self.num_atoms),
            )
            # Calculate the probability per action value atom.
            prob_per_action_per_atom = nn.functional.softmax(
                logits_per_action_per_atom,
                dim=1,
            )
            # Compute expected action value by weighted sum.
            # TODO (simon): Check, if these outputs are needed.
            # output["af_preds"] = torch.sum(z * prob_per_action_per_atom, dim=-1)
            output["atoms"] = z
            output["logits"] = logits_per_action_per_atom
            output["probs"] = prob_per_action_per_atom
        else:
            output["logits"] = torch.unsqueeze(torch.ones_like(batch), dim=-1)
            output[QF_PREDS] = batch

        return output
