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
from ray.rllib.utils.typing import NetworkType, TensorType, TensorStructType

torch, nn = try_import_torch()


class DQNRainbowTorchRLModule(TorchRLModule, DQNRainbowRLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        output = {}

        # Encoder forward pass.
        encoder_outs = self.encoder(batch)

        # Q-head.
        qf_outs = self._qf(encoder_outs[ENCODER_OUT])

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
    def _forward_exploration(
        self, batch: Dict[str, TensorType], t: int
    ) -> Dict[str, TensorType]:
        output = {}

        # Q-network forward pass.
        qf_outs = self._qf(batch)

        # Get action distribution.
        action_dist_cls = self.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(qf_outs[QF_PREDS])
        # Note, the deterministic version of the categorical distribution
        # outputs directly the `argmax` of the logits.
        exploit_actions = action_dist.to_deterministic().sample()

        # In case of noisy networks the parameter noise is sufficient for
        # variation in exploration.
        if self.uses_noisy:
            # Use the exploitation action (coming from the noisy network).
            output[SampleBatch.ACTIONS] = exploit_actions
        # Otherwise we need epsilon greedy to support exploration.
        else:
            # TODO (simon): Implement sampling for nested spaces.
            # Update scheduler.
            self.epsilon_schedule.update(t)
            # Get the actual epsilon,
            epsilon = self.epsilon_scheduler.get_current_value()
            # Apply epsilon-greedy exploration.
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
    def _forward_train(
        self, batch: Dict[str, TensorType]
    ) -> Dict[str, TensorStructType]:
        output = {}

        # DQN needs the Q-values for the current and the next observation.
        batch_curr = {SampleBatch.OBS: batch[SampleBatch.OBS]}
        batch_next = {SampleBatch.OBS: batch[SampleBatch.NEXT_OBS]}

        # Q-network forward passes.
        qf_outs = self._qf(batch_curr)
        output[QF_PREDS] = qf_outs[QF_PREDS]
        # The target Q-values for the next observations.
        qf_target_next_outs = self._qf_target(batch_next)
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
    def _qf(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch recevied in the forward pass.

        Results:
            A dictionary containing the Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the Q-value
            predictions ("qf_preds") the support atoms ("atoms"), the Q-logits
            ("qf_logits"), and the probabilities ("qf_probs").
        """
        # If we have a dueling architecture we have to add the value stream.
        return self._qf_forward_helper(
            batch,
            self.encoder,
            {"af": self.af, "vf": self.vf} if self.is_dueling else self.af,
        )

    @override(DQNRainbowRLModule)
    def _qf_target(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values from the target network.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.

        Args:
            batch: The batch recevied in the forward pass.

        Results:
            A dictionary containing the target Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the target
            Q-value predictions ("qf_preds") the support atoms ("atoms"), the target
            Q-logits  ("qf_logits"), and the probabilities ("qf_probs").
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
    def _af_dist(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Compute the advantage distribution.

        Note this distribution is identical to the Q-distribution in
        case no dueling architecture is used.

        Args:
            batch: A dictionary containing a tensor with the outputs of the
                forward pass of the Q-head or advantage stream head.

        Returns:
            A `dict` containing the support of the discrete distribution for
            either Q-values or advantages (in case of a dueling architecture),
            ("atoms"), the logits per action and atom and the probabilities
            of the discrete distribution (per action and atom of the support).
        """
        output = {}
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
        self,
        batch: Dict[str, TensorType],
        encoder: Encoder,
        head: Union[Model, Dict[str, Model]],
    ) -> Dict[str, TensorType]:
        """Computes Q-values.

        This is a helper function that takes care of all different cases,
        i.e. if we use a dueling architecture or not and if we use distributional
        Q-learning or not.

        Args:
            batch: The batch recevied in the forward pass.
            encoder: The encoder network to use. Here we have a single encoder
                for all heads (Q or advantages and value in case of a dueling
                architecture).
            head: Either a head model or a dictionary of head model (dueling
            architecture) containing advantage and value stream heads.

        Returns:
            In case of expectation learning the Q-value predictions ("qf_preds")
            and in case of distributional Q-learning in addition to the predictions
            the atoms ("atoms"), the Q-value predictions ("qf_preds"), the Q-logits
            ("qf_logits") and the probabilities for the support atoms ("qf_probs").
        """
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
                af_dist_output = self._af_dist(qf_outs)
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
                # Return also the support as we need it in the learner.
                output[ATOMS] = af_dist_output[ATOMS]
                # Calculate the Q-values by the weighted sum over the atoms.
                output[QF_PREDS] = torch.sum(af_dist_output[ATOMS] * qf_probs, dim=-1)
                output[QF_LOGITS] = qf_logits
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
                qf_dist_outs = self._af_dist(qf_outs)
                # Get the support of the Q-value distribution.
                output[ATOMS] = qf_dist_outs[ATOMS]
                # Calculate the Q-values by the weighted sum over the atoms.
                output[QF_PREDS] = torch.sum(
                    qf_dist_outs[ATOMS] * qf_dist_outs["probs"], dim=-1
                )
                output[QF_LOGITS] = qf_dist_outs["logits"]
                output[QF_PROBS] = qf_dist_outs["probs"]
            # Otherwise we learn an expectation.
            else:
                # In this case we have a Q-head of dimension (1, action_space.n).
                output[QF_PREDS] = qf_outs

        return output
