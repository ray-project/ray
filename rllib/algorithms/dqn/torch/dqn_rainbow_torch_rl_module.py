from typing import Any, Dict, Union

from ray.rllib.algorithms.dqn.dqn_rainbow_rl_module import (
    DQNRainbowRLModule,
    ATOMS,
    QF_LOGITS,
    QF_NEXT_PREDS,
    QF_PREDS,
    QF_PROBS,
    QF_TARGET_NEXT_PREDS,
    QF_TARGET_NEXT_PROBS,
)
from ray.rllib.algorithms.dqn.torch.dqn_rainbow_torch_noisy_net import (
    TorchNoisyMLPEncoder,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import Encoder, ENCODER_OUT, Model
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType, TensorStructType

torch, nn = try_import_torch()


class DQNRainbowTorchRLModule(TorchRLModule, DQNRainbowRLModule):
    framework: str = "torch"

    @override(DQNRainbowRLModule)
    def setup(self):
        super().setup()

        # If we use a noisy encoder. Note, only if the observation
        # space is a flat space we can use a noisy encoder.
        self.uses_noisy_encoder = isinstance(self.encoder, TorchNoisyMLPEncoder)
        # If we have target networks we need to sync them.
        if not self.inference_only:
            # We do not want to train the target networks.
            # AND sync all target nets with the actual (trained) ones.
            self.target_encoder.requires_grad_(False)
            self.target_encoder.load_state_dict(self.encoder.state_dict())

            self.af_target.requires_grad_(False)
            self.af_target.load_state_dict(self.af.state_dict())

            if self.uses_dueling:
                self.vf_target.requires_grad_(False)
                self.vf_target.load_state_dict(self.vf.state_dict())

            # Set the expected and unexpected keys for the inference-only module.
            self._set_inference_only_state_dict_keys()

    @override(RLModuleWithTargetNetworksInterface)
    def sync_target_networks(self, tau: float) -> None:
        pairs = [(self.target_encoder, self.encoder), (self.af_target, self.af)] + (
            # If we have a dueling architecture we need to update the value stream
            # target, too.
            [
                (self.vf_target, self.vf),
            ]
            if self.uses_dueling
            else []
        )
        # Loop through all individual networks that have a corresponding target net.
        for target_net, main_net in pairs:
            # Get the current parameters from the main network.
            state_dict = main_net.state_dict()
            # Use here Polyak averaging.
            new_target_state_dict = {
                k: tau * state_dict[k] + (1 - tau) * v
                for k, v in target_net.state_dict().items()
            }
            # Apply the new parameters to the target Q network.
            target_net.load_state_dict(new_target_state_dict)

    # TODO (simon): Refactor to parent method.
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
    def _forward_inference(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        output = {}

        # Set the module into evaluation mode, if needed.
        if self.uses_noisy and self.training:
            # This sets the weigths and bias to their constant version.
            self.eval()

        # Q-network forward pass.
        qf_outs = self._qf(batch)

        # Get action distribution.
        action_dist_cls = self.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(qf_outs[QF_PREDS])
        # Note, the deterministic version of the categorical distribution
        # outputs directly the `argmax` of the logits.
        exploit_actions = action_dist.to_deterministic().sample()

        # In inference we only need the exploitation actions.
        output[Columns.ACTIONS] = exploit_actions

        return output

    @override(RLModule)
    def _forward_exploration(
        self, batch: Dict[str, TensorType], t: int
    ) -> Dict[str, TensorType]:
        output = {}

        # Resample the noise for the noisy layers, if needed.
        if self.uses_noisy:
            # We want to resample the noise everytime we step.
            self._reset_noise(target=False)
            if not self.training:
                # Set the module into training mode. This sets
                # the weigths and bias to their noisy version.
                self.train(True)

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
            output[Columns.ACTIONS] = exploit_actions
        # Otherwise we need epsilon greedy to support exploration.
        else:
            # TODO (simon): Implement sampling for nested spaces.
            # Update scheduler.
            self.epsilon_schedule.update(t)
            # Get the actual epsilon,
            epsilon = self.epsilon_schedule.get_current_value()
            # Apply epsilon-greedy exploration.
            B = qf_outs[QF_PREDS].shape[0]
            random_actions = torch.squeeze(
                torch.multinomial(
                    (torch.nan_to_num(qf_outs[QF_PREDS], neginf=0.0) != 0.0).float(),
                    num_samples=1,
                ),
                dim=1,
            )
            output[Columns.ACTIONS] = torch.where(
                torch.rand((B,)) < epsilon,
                random_actions,
                exploit_actions,
            )

        return output

    @override(RLModule)
    def _forward_train(
        self, batch: Dict[str, TensorType]
    ) -> Dict[str, TensorStructType]:
        if self.inference_only:
            raise RuntimeError(
                "Trying to train a module that is not a learner module. Set the "
                "flag `inference_only=False` when building the module."
            )
        output = {}

        # Set module into training mode.
        if self.uses_noisy and not self.training:
            # This sets the weigths and bias to their noisy version.
            self.train(True)

        # If we use a double-Q setup.
        if self.uses_double_q:
            # Then we need to make a single forward pass with both,
            # current and next observations.
            batch_base = {
                Columns.OBS: torch.concat(
                    [batch[Columns.OBS], batch[Columns.NEXT_OBS]], dim=0
                ),
            }
        # Otherwise we can just use the current observations.
        else:
            batch_base = {Columns.OBS: batch[Columns.OBS]}
        batch_target = {Columns.OBS: batch[Columns.NEXT_OBS]}

        # Q-network forward passes.
        qf_outs = self._qf(batch_base)
        if self.uses_double_q:
            output[QF_PREDS], output[QF_NEXT_PREDS] = torch.chunk(
                qf_outs[QF_PREDS], chunks=2, dim=0
            )
        else:
            output[QF_PREDS] = qf_outs[QF_PREDS]
        # The target Q-values for the next observations.
        qf_target_next_outs = self._qf_target(batch_target)
        output[QF_TARGET_NEXT_PREDS] = qf_target_next_outs[QF_PREDS]
        # We are learning a Q-value distribution.
        if self.num_atoms > 1:
            # Add distribution artefacts to the output.
            # Distribution support.
            output[ATOMS] = qf_target_next_outs[ATOMS]
            # Original logits from the Q-head.
            output[QF_LOGITS] = qf_outs[QF_LOGITS]
            # Probabilities of the Q-value distribution of the current state.
            output[QF_PROBS] = qf_outs[QF_PROBS]
            # Probabilities of the target Q-value distribution of the next state.
            output[QF_TARGET_NEXT_PROBS] = qf_target_next_outs[QF_PROBS]

        return output

    @override(DQNRainbowRLModule)
    def _qf(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values.

        Note, these can be accompanied by logits and probabilities
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
            {"af": self.af, "vf": self.vf} if self.uses_dueling else self.af,
        )

    @override(DQNRainbowRLModule)
    def _qf_target(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        """Computes Q-values from the target network.

        Note, these can be accompanied by logits and probabilities
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
            (
                {"af": self.af_target, "vf": self.vf_target}
                if self.uses_dueling
                else self.af_target
            ),
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
        # NOTE: Handcrafted action shape.
        logits_per_action_per_atom = torch.reshape(
            batch, shape=(-1, self.config.action_space.n, self.num_atoms)
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

    # TODO (simon): Test, if providing the function with a `return_probs`
    # improves performance significantly.
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
        if self.uses_dueling:
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
                # Has to be a mean for each batch element.
                af_outs_mean = torch.unsqueeze(
                    torch.nan_to_num(qf_outs, neginf=torch.nan).nanmean(dim=1), dim=1
                )
                qf_outs = qf_outs - af_outs_mean
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

    @override(DQNRainbowRLModule)
    def _reset_noise(self, target: bool = False) -> None:
        """Reset the noise of all noisy layers.

        Args:
            target: Whether to reset the noise of the target networks.
        """
        if self.uses_noisy:
            if self.uses_noisy_encoder:
                self.encoder._reset_noise()
            self.af._reset_noise()
            # If we have a dueling architecture we need to reset the noise
            # of the value stream, too.
            if self.uses_dueling:
                self.vf._reset_noise()
            # Reset the noise of the target networks, if requested.
            if target:
                if self.uses_noisy_encoder:
                    self.target_encoder._reset_noise()
                self.af_target._reset_noise()
                # If we have a dueling architecture we need to reset the noise
                # of the value stream, too.
                if self.uses_dueling:
                    self.vf_target._reset_noise()

    @override(TorchRLModule)
    def _set_inference_only_state_dict_keys(self) -> None:
        # Get the model parameters.
        state_dict = self.state_dict()
        # Note, these keys are only known to the learner module. Furthermore,
        # we want this to be run once during setup and not for each worker.
        # TODO (simon): Check, if we can also remove the value network.
        self._inference_only_state_dict_keys["unexpected_keys"] = [
            name for name in state_dict if "target" in name
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
