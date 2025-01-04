import tree
from typing import Dict, Union

from ray.rllib.algorithms.dqn.default_dqn_rl_module import (
    DefaultDQNRLModule,
    ATOMS,
    QF_LOGITS,
    QF_NEXT_PREDS,
    QF_PREDS,
    QF_PROBS,
    QF_TARGET_NEXT_PREDS,
    QF_TARGET_NEXT_PROBS,
)
from ray.rllib.algorithms.dqn.dqn_catalog import DQNCatalog
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import Encoder, ENCODER_OUT, Model
from ray.rllib.core.rl_module.apis.q_net_api import QNetAPI
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType, TensorStructType
from ray.util.annotations import DeveloperAPI

torch, nn = try_import_torch()


@DeveloperAPI
class DefaultDQNTorchRLModule(TorchRLModule, DefaultDQNRLModule):
    framework: str = "torch"

    def __init__(self, *args, **kwargs):
        catalog_class = kwargs.pop("catalog_class", None)
        if catalog_class is None:
            catalog_class = DQNCatalog
        super().__init__(*args, **kwargs, catalog_class=catalog_class)

    @override(RLModule)
    def _forward_inference(self, batch: Dict[str, TensorType]) -> Dict[str, TensorType]:
        # Q-network forward pass.
        qf_outs = self.compute_q_values(batch)

        # Get action distribution.
        action_dist_cls = self.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(qf_outs[QF_PREDS])
        # Note, the deterministic version of the categorical distribution
        # outputs directly the `argmax` of the logits.
        exploit_actions = action_dist.to_deterministic().sample()

        output = {Columns.ACTIONS: exploit_actions}
        if Columns.STATE_OUT in qf_outs:
            output[Columns.STATE_OUT] = qf_outs[Columns.STATE_OUT]

        # In inference, we only need the exploitation actions.
        return output

    @override(RLModule)
    def _forward_exploration(
        self, batch: Dict[str, TensorType], t: int
    ) -> Dict[str, TensorType]:
        # Define the return dictionary.
        output = {}

        # Q-network forward pass.
        qf_outs = self.compute_q_values(batch)

        # Get action distribution.
        action_dist_cls = self.get_exploration_action_dist_cls()
        action_dist = action_dist_cls.from_logits(qf_outs[QF_PREDS])
        # Note, the deterministic version of the categorical distribution
        # outputs directly the `argmax` of the logits.
        exploit_actions = action_dist.to_deterministic().sample()

        # We need epsilon greedy to support exploration.
        # TODO (simon): Implement sampling for nested spaces.
        # Update scheduler.
        self.epsilon_schedule.update(t)
        # Get the actual epsilon,
        epsilon = self.epsilon_schedule.get_current_value()
        # Apply epsilon-greedy exploration.
        B = qf_outs[QF_PREDS].shape[0]
        random_actions = torch.squeeze(
            torch.multinomial(
                (
                    torch.nan_to_num(
                        qf_outs[QF_PREDS].reshape(-1, qf_outs[QF_PREDS].size(-1)),
                        neginf=0.0,
                    )
                    != 0.0
                ).float(),
                num_samples=1,
            ),
            dim=1,
        )

        actions = torch.where(
            torch.rand((B,)) < epsilon,
            random_actions,
            exploit_actions,
        )

        # Add the actions to the return dictionary.
        output[Columns.ACTIONS] = actions

        # If this is a stateful module, add output states.
        if Columns.STATE_OUT in qf_outs:
            output[Columns.STATE_OUT] = qf_outs[Columns.STATE_OUT]

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

        # If we use a double-Q setup.
        if self.uses_double_q:
            # Then we need to make a single forward pass with both,
            # current and next observations.
            batch_base = {
                Columns.OBS: torch.concat(
                    [batch[Columns.OBS], batch[Columns.NEXT_OBS]], dim=0
                ),
            }
            # If this is a stateful module add the input states.
            if Columns.STATE_IN in batch:
                # Add both, the input state for the actual observation and
                # the one for the next observation.
                batch_base.update(
                    {
                        Columns.STATE_IN: tree.map_structure(
                            lambda t1, t2: torch.cat([t1, t2], dim=0),
                            batch[Columns.STATE_IN],
                            batch[Columns.NEXT_STATE_IN],
                        )
                    }
                )
        # Otherwise we can just use the current observations.
        else:
            batch_base = {Columns.OBS: batch[Columns.OBS]}
            # If this is a stateful module add the input state.
            if Columns.STATE_IN in batch:
                batch_base.update({Columns.STATE_IN: batch[Columns.STATE_IN]})

        batch_target = {Columns.OBS: batch[Columns.NEXT_OBS]}

        # If we have a stateful encoder, add the states for the target forward
        # pass.
        if Columns.NEXT_STATE_IN in batch:
            batch_target.update({Columns.STATE_IN: batch[Columns.NEXT_STATE_IN]})

        # Q-network forward passes.
        qf_outs = self.compute_q_values(batch_base)
        if self.uses_double_q:
            output[QF_PREDS], output[QF_NEXT_PREDS] = torch.chunk(
                qf_outs[QF_PREDS], chunks=2, dim=0
            )
        else:
            output[QF_PREDS] = qf_outs[QF_PREDS]
        # The target Q-values for the next observations.
        qf_target_next_outs = self.forward_target(batch_target)
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

        # Add the states to the output, if the module is stateful.
        if Columns.STATE_OUT in qf_outs:
            output[Columns.STATE_OUT] = qf_outs[Columns.STATE_OUT]
        # For correctness, also add the output states from the target forward pass.
        # Note, we do not backpropagate through this state.
        if Columns.STATE_OUT in qf_target_next_outs:
            output[Columns.NEXT_STATE_OUT] = qf_target_next_outs[Columns.STATE_OUT]

        return output

    @override(QNetAPI)
    def compute_advantage_distribution(
        self,
        batch: Dict[str, TensorType],
    ) -> Dict[str, TensorType]:
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
            batch, shape=(*batch.shape[:-1], self.action_space.n, self.num_atoms)
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
    #  improves performance significantly.
    @override(DefaultDQNRLModule)
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
            batch: The batch received in the forward pass.
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
                af_dist_output = self.compute_advantage_distribution(qf_outs)
                # Center the advantage stream distribution.
                centered_af_logits = af_dist_output["logits"] - af_dist_output[
                    "logits"
                ].mean(dim=-1, keepdim=True)
                # Calculate the Q-value distribution by adding advantage and
                # value stream.
                qf_logits = centered_af_logits + vf_outs.view(
                    -1, *((1,) * (centered_af_logits.dim() - 1))
                )
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
                af_outs_mean = torch.nan_to_num(qf_outs, neginf=torch.nan).nanmean(
                    dim=-1, keepdim=True
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
                qf_dist_outs = self.compute_advantage_distribution(qf_outs)
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

        # If we have a stateful encoder add the output states to the return
        # dictionary.
        if Columns.STATE_OUT in encoder_outs:
            output[Columns.STATE_OUT] = encoder_outs[Columns.STATE_OUT]

        return output
