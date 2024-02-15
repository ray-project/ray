from typing import Dict, List, Tuple, Union

from ray.rllib.algorithms.dqn.dqn_rainbow_rl_module import (
    DQNRainbowRLModule,
    ATOMS,
    QF_LOGITS,
    QF_PROBS,
    QF_TARGET_NEXT_PREDS,
    QF_TARGET_NEXT_PROBS,
)
from ray.rllib.algorithms.sac.sac_rl_module import QF_PREDS
from ray.rllib.core.models.base import Encoder, ENCODER_OUT, Model
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import NetworkType

torch, nn = try_import_torch()


# TODO (simon): Maybe call logits qf_logits, and probs qf_probs.
class DQNRainbowTorchRLModule(TorchRLModule, DQNRainbowRLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: Dict) -> Dict:
        output = {}

        # Encoder forward pass.
        encoder_outs = self.encoder(batch)

        # Q-head.
        qf_outs = self.qf(encoder_outs[ENCODER_OUT])

        # Get action distribution.
        action_dist_cls = self.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(qf_outs[QF_PREDS])
        # Note, the deterministic version of the categorical distribution
        # outputs directly the `argmax` of the logits.
        exploit_actions = action_dist.to_deterministic().sample()

        # In inference we only need the exploitation actions.
        output[SampleBatch.ACTIONS] = exploit_actions

        return output

    # TODO (simon): Implement the timestep for more complex exploration
    # schedules with epsilon greedy.
    @override(RLModule)
    def _forward_exploration(self, batch: Dict) -> Dict:
        output = {}

        # Q-network forward pass.
        qf_outs = self.qf(batch)

        # Get action distribution.
        action_dist_cls = self.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(qf_outs[QF_PREDS])
        # Note, the deterministic version of the categorical distribution
        # outputs directly the `argmax` of the logits.
        exploit_actions = action_dist.to_deterministic().sample()

        # Apply epsilon-greedy exploration.
        # TODO (simon): Implement sampling for nested spaces.
        # TODO (simon): Implement different epsilon and schedules.
        epsilon = 0.1
        B = qf_outs[QF_PREDS].shape[0]
        random_actions = torch.squeeze(
            torch.multinomial(
                (torch.nan_to_num(qf_outs[QF_PREDS], neginf=0.0) != 0).float(),
                num_samples=1,
            ),
            dim=1,
        )
        output[SampleBatch.ACTIONS] = torch.where(
            torch.rand((B,)) < epsilon,
            random_actions,
            exploit_actions,
        )

        return output

    @override(RLModule)
    def _forward_train(self, batch: Dict) -> Dict:
        output = {}

        # DQN needs the Q-values for the current and the next observation.
        batch_curr = {SampleBatch.OBS: batch[SampleBatch.OBS]}
        batch_next = {SampleBatch.OBS: batch[SampleBatch.NEXT_OBS]}

        # Q-network forward passes.
        qf_outs = self.qf(batch_curr)
        output[QF_PREDS] = qf_outs[QF_PREDS]
        # The target Q-values for the next observations.
        qf_target_next_outs = self.qf_target(batch_next)
        output[QF_TARGET_NEXT_PREDS] = qf_target_next_outs[QF_PREDS]
        # We are learning a Q-value distribution.
        if self.num_atoms > 1:
            # Add distribution artefacts to the output.
            # Distribution support.
            output[ATOMS] = qf_target_next_outs[ATOMS]
            # Original logits from the Q-head.
            output[QF_LOGITS] = qf_outs[QF_LOGITS]
            # Probabilities of the Q-value distribution of the next state.
            output[QF_TARGET_NEXT_PROBS] = qf_target_next_outs[QF_PROBS]

        return output

    @override(DQNRainbowRLModule)
    def qf(self, batch: Dict) -> Dict:
        """Computes Q-values.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.
        """
        # If we have a dueling architecture we have to add the value stream.
        return self._qf_forward_helper(
            batch,
            self.encoder,
            {"af": self.af, "vf": self.vf} if self.is_dueling else self.af,
        )

    @override(DQNRainbowRLModule)
    def qf_target(self, batch: Dict) -> Dict:
        """Computes Q-values from the target network.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.
        """
        # If we have a dueling architecture we have to add the value stream.
        return self._qf_forward_helper(
            batch,
            self.target_encoder,
            {"af": self.af_target, "vf": self.vf_target}
            if self.is_dueling
            else self.af_target,
        )

    @override(DQNRainbowRLModule)
    def af_dist(self, batch: Dict) -> Dict:
        """Compute the advantage distribution."""
        output = {}
        # TODO (simon): Check, if we still need the test for atoms here.
        # if self.num_atoms > 1:
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
        # Calculate the probability for each action value atom. Note,
        # the sum along action value atoms of a single action value
        # must sum to one.
        prob_per_action_per_atom = nn.functional.softmax(
            logits_per_action_per_atom,
            dim=-1,
        )
        # Compute expected action value by weighted sum.
        # output["preds"] = torch.sum(z * prob_per_action_per_atom, dim=-1)
        output[ATOMS] = z
        output["logits"] = logits_per_action_per_atom
        output["probs"] = prob_per_action_per_atom

        return output

    @override(RLModuleWithTargetNetworksInterface)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns target Q and Q network(s) to update the target network(s)."""
        return [(self.target_encoder, self.encoder), (self.af_target, self.af)] + (
            # If we have a dueling architecture we need to update the value stream
            # target, too.
            [
                (self.vf_target, self.vf),
            ]
            if self.is_dueling
            else []
        )

    @override(DQNRainbowRLModule)
    def _qf_forward_helper(
        self, batch: Dict, encoder: Encoder, head: Union[Model, Dict[str, Model]]
    ) -> Dict:
        """Computes Q-values."""
        output = {}

        # Encoder forward pass.
        encoder_outs = encoder(batch)

        # Do we have a dueling architecture.
        if self.is_dueling:
            # Head forward passes for advantage and value stream.
            qf_outs = head["af"](encoder_outs[ENCODER_OUT])
            vf_outs = head["vf"](encoder_outs[ENCODER_OUT])
            # We learn a Q-value distribution.
            if self.num_atoms > 1:
                # Compute the advantage stream distribution.
                af_dist_output = self.af_dist(qf_outs)
                # Center the advantage stream distribution.
                centered_af_logits = af_dist_output["logits"] - af_dist_output[
                    "logits"
                ].mean(dim=1, keepdim=True)
                # Calculate the Q-value distribution by adding advantage and
                # value stream.
                qf_logits = centered_af_logits + vf_outs.unsqueeze(dim=-1)
                # Calculate probabilites for the Q-value distribution along
                # the support given by the atoms.
                qf_probs = nn.functional.softmax(qf_logits, dim=-1)
                # Calculate the Q-values by the weighted sum over the atoms.
                output[ATOMS] = af_dist_output[ATOMS]
                output[QF_PREDS] = torch.sum(af_dist_output[ATOMS] * qf_probs, dim=-1)
                output["qf_logits"] = qf_logits
                output[QF_PROBS] = qf_probs
            # Otherwise we learn an expectation.
            else:
                # Center advantages. Note, we cannot do an in-place operation here
                # b/c we backpropagate through these values. See for a discussion
                # https://discuss.pytorch.org/t/gradient-computation-issue-due-to-
                # inplace-operation-unsure-how-to-debug-for-custom-model/170133
                af_outs_mean = torch.unsqueeze(
                    torch.nan_to_num(qf_outs, neginf=torch.nan).nanmean(dim=1), dim=1
                )
                qf_outs = qf_outs - af_outs_mean
                # TODO (simon): Check if unsqueeze is necessary.
                # Add advantage and value stream. Note, we broadcast here.
                output[QF_PREDS] = qf_outs + vf_outs
        # No dueling architecture.
        else:
            # Note, in this case the advantage network is the Q-network.
            # Forward pass through Q-head.
            qf_outs = head(encoder_outs[ENCODER_OUT])
            # We learn a Q-value distribution.
            if self.num_atoms > 1:
                # Note in a non-dueling architecture the advantage distribution is
                # the Q-value distribution.
                # Get the Q-value distribution.
                qf_dist_outs = self.af_dist(qf_outs)
                # Get the support of the Q-value distribution.
                output[ATOMS] = qf_dist_outs[ATOMS]
                # Calculate the Q-values by the weighted sum over the atoms.
                output[QF_PREDS] = torch.sum(
                    qf_dist_outs[ATOMS] * qf_dist_outs["probs"], dim=-1
                )
                output["qf_logits"] = qf_dist_outs["logits"]  # qf_logits
                output[QF_PROBS] = qf_dist_outs["probs"]  # qf_probs
            # Otherwise we learn an expectation.
            else:
                # In this case we have a Q-head of dimension (1, action_space.n).
                output[QF_PREDS] = qf_outs

        return output
