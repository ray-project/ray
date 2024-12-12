import tree
from typing import Any, Dict, Optional

from ray.rllib.algorithms.sac.sac_learner import (
    QF_PREDS,
    QF_TWIN_PREDS,
)
from ray.rllib.algorithms.sac.torch.sac_torch_rl_module import SACTorchRLModule

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class CQLTorchRLModule(SACTorchRLModule):
    @override(SACTorchRLModule)
    def _forward_train(self, batch: Dict) -> Dict[str, Any]:
        # Call the super method.
        fwd_out = super()._forward_train(batch)

        # Make sure we perform a "straight-through gradient" pass here,
        # ignoring the gradients of the q-net, however, still recording
        # the gradients of the policy net (which was used to rsample the actions used
        # here). This is different from doing `.detach()` or `with torch.no_grads()`,
        # as these two methds would fully block all gradient recordings, including
        # the needed policy ones.
        all_params = list(self.pi_encoder.parameters()) + list(self.pi.parameters())
        # if self.twin_q:
        #     all_params += list(self.qf_twin.parameters()) + list(
        #         self.qf_twin_encoder.parameters()
        #     )

        for param in all_params:
            param.requires_grad = False

        # Compute the repeated actions, action log-probabilites and Q-values for all
        # observations.
        # First for the random actions (from the mu-distribution as named by Kumar et
        # al. (2020)).
        low = torch.tensor(
            self.action_space.low,
            device=fwd_out[QF_PREDS].device,
        )
        high = torch.tensor(
            self.action_space.high,
            device=fwd_out[QF_PREDS].device,
        )
        num_samples = batch[Columns.ACTIONS].shape[0] * self.model_config["num_actions"]
        actions_rand_repeat = low + (high - low) * torch.rand(
            (num_samples, low.shape[0]), device=fwd_out[QF_PREDS].device
        )

        # First for the random actions (from the mu-distribution as named in Kumar
        # et al. (2020)) using repeated observations.
        rand_repeat_out = self._repeat_actions(batch[Columns.OBS], actions_rand_repeat)
        (fwd_out["actions_rand_repeat"], fwd_out["q_rand_repeat"]) = (
            rand_repeat_out[Columns.ACTIONS],
            rand_repeat_out[QF_PREDS],
        )
        # Sample current and next actions (from the pi distribution as named in Kumar
        # et al. (2020)) using repeated observations
        # Second for the current observations and the current action distribution.
        curr_repeat_out = self._repeat_actions(batch[Columns.OBS])
        (
            fwd_out["actions_curr_repeat"],
            fwd_out["logps_curr_repeat"],
            fwd_out["q_curr_repeat"],
        ) = (
            curr_repeat_out[Columns.ACTIONS],
            curr_repeat_out[Columns.ACTION_LOGP],
            curr_repeat_out[QF_PREDS],
        )
        # Then, for the next observations and the current action distribution.
        next_repeat_out = self._repeat_actions(batch[Columns.NEXT_OBS])
        (
            fwd_out["actions_next_repeat"],
            fwd_out["logps_next_repeat"],
            fwd_out["q_next_repeat"],
        ) = (
            next_repeat_out[Columns.ACTIONS],
            next_repeat_out[Columns.ACTION_LOGP],
            next_repeat_out[QF_PREDS],
        )
        if self.twin_q:
            # First for the random actions from the mu-distribution.
            fwd_out["q_twin_rand_repeat"] = rand_repeat_out[QF_TWIN_PREDS]
            # Second for the current observations and the current action distribution.
            fwd_out["q_twin_curr_repeat"] = curr_repeat_out[QF_TWIN_PREDS]
            # Then, for the next observations and the current action distribution.
            fwd_out["q_twin_next_repeat"] = next_repeat_out[QF_TWIN_PREDS]
        # Reset the gradient requirements for all Q-function parameters.
        for param in all_params:
            param.requires_grad = True

        return fwd_out

    def _repeat_tensor(self, tensor: TensorType, repeat: int) -> TensorType:
        """Generates a repeated version of a tensor.

        The repetition is done similar `np.repeat` and repeats each value
        instead of the complete vector.

        Args:
            tensor: The tensor to be repeated.
            repeat: How often each value in the tensor should be repeated.

        Returns:
            A tensor holding `repeat`  repeated values of the input `tensor`
        """
        # Insert the new dimension at axis 1 into the tensor.
        t_repeat = tensor.unsqueeze(1)
        # Repeat the tensor along the new dimension.
        t_repeat = torch.repeat_interleave(t_repeat, repeat, dim=1)
        # Stack the repeated values into the batch dimension.
        t_repeat = t_repeat.view(-1, *tensor.shape[1:])
        # Return the repeated tensor.
        return t_repeat

    def _repeat_actions(
        self, obs: TensorType, actions: Optional[TensorType] = None
    ) -> Dict[str, TensorType]:
        """Generated actions and Q-values for repeated observations.

        The `self.model_config["num_actions"]` define a multiplier
        used for generating `num_actions` as many actions as the batch size.
        Observations are repeated and then a model forward pass is made.

        Args:
            obs: A batched observation tensor.
            actions: An optional batched actions tensor.

        Returns:
            A dictionary holding the (sampled or passed-in actions), the log
            probabilities (of sampled actions), the Q-values and if available
            the twin-Q values.
        """
        output = {}
        # Receive the batch size.
        batch_size = obs.shape[0]
        # Receive the number of action to sample.
        num_actions = self.model_config["num_actions"]
        # Repeat the observations `num_actions` times.
        obs_repeat = tree.map_structure(
            lambda t: self._repeat_tensor(t, num_actions), obs
        )
        # Generate a batch for the forward pass.
        temp_batch = {Columns.OBS: obs_repeat}
        if actions is None:
            # TODO (simon): Run the forward pass in inference mode.
            # Compute the action logits.
            pi_encoder_outs = self.pi_encoder(temp_batch)
            action_logits = self.pi(pi_encoder_outs[ENCODER_OUT])
            # Generate the squashed Gaussian from the model's logits.
            action_dist = self.get_train_action_dist_cls().from_logits(action_logits)
            # Sample the actions. Note, we want to make a backward pass through
            # these actions.
            output[Columns.ACTIONS] = action_dist.rsample()
            # Compute the action log-probabilities.
            output[Columns.ACTION_LOGP] = action_dist.logp(
                output[Columns.ACTIONS]
            ).view(batch_size, num_actions, 1)
        else:
            output[Columns.ACTIONS] = actions

        # Compute all Q-values.
        temp_batch.update(
            {
                Columns.ACTIONS: output[Columns.ACTIONS],
            }
        )
        output.update(
            {
                QF_PREDS: self._qf_forward_train_helper(
                    temp_batch,
                    self.qf_encoder,
                    self.qf,
                ).view(batch_size, num_actions, 1)
            }
        )
        # If we have a twin-Q network, compute its Q-values, too.
        if self.twin_q:
            output.update(
                {
                    QF_TWIN_PREDS: self._qf_forward_train_helper(
                        temp_batch,
                        self.qf_twin_encoder,
                        self.qf_twin,
                    ).view(batch_size, num_actions, 1)
                }
            )
        del temp_batch

        # Return
        return output
