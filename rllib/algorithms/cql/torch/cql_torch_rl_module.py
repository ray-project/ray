from typing import Any, Dict

from ray.rllib.algorithms.sac.sac_learner import (
    QF_PREDS,
    QF_TWIN_PREDS,
)
from ray.rllib.algorithms.sac.torch.sac_torch_rl_module import SACTorchRLModule

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.utils.annotations import override


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
        all_params = list(self.qf.parameters()) + list(self.qf_encoder.parameters())
        if self.twin_q:
            all_params += list(self.qf_twin.parameters()) + list(
                self.qf_twin_encoder.parameters()
            )

        for param in all_params:
            param.requires_grad = False

        # Compute the repeated actions, action log-probabilites and Q-values for all
        # observations.
        # First for the random actions (from the mu-distribution as named by Kumar et
        # al. (2020)).
        low = torch.tensor(
            self.module[module_id].config.action_space.low,
            device=fwd_out[QF_PREDS].device,
        )
        high = torch.tensor(
            self.module[module_id].config.action_space.high,
            device=fwd_out[QF_PREDS].device,
        )
        num_samples = batch[Columns.ACTIONS].shape[0] * config.num_actions
        actions_rand_repeat = low + (high - low) * torch.rand(
            (num_samples, low.shape[0]), device=fwd_out[QF_PREDS].device
        )
        # TODO (simon): Check, if we need to turn off pi grads, too.
        if self.twin_q:
            # First for the random actions from the mu-distribution.
            (
                fwd_out["actions_rand_repeat"],
                fwd_out["q_rand_repeat"],
                fwd_out["twin_q_rand_repeat"],
            ) = self._repeat_actions(batch[Columns.OBS], actions_rand_repeat)
            # Sample current and next actions (from the pi distribution as named in Kumar
            # et al. (2020)) using repeated observations
            # Second for the current observations and the current action distribution.
            (
                fwd_out["actions_curr_repeat"],
                fwd_out["logps_curr_repeat"],
                fwd_out["q_curr_repeat"],
                fwd_out["twin_q_curr_repeat"],
            ) = self._repeat_actions(batch[Columns.OBS])
            # Then, for the next observations and the current action distribution.
            (
                fwd_out["actions_next_repeat"],
                fwd_out["logps_next_repeat"],
                fwd_out["q_next_repeat"],
                fwd_out["twin_q_next_repeat"],
            ) = self._repeat_actions(batch[Columns.NEXT_OBS])
        else:
            # First for the random actions from the mu-distribution.
            (
                fwd_out["actions_rand_repeat"],
                fwd_out["q_rand_repeat"],
            ) = self._repeat_actions(batch[Columns.OBS], actions_rand_repeat)
            # Sample current and next actions (from the pi distribution as named in Kumar
            # et al. (2020)) using repeated observations
            # Second for the current observations and the current action distribution.
            (
                fwd_out["actions_curr_repeat"],
                fwd_out["logps_curr_repeat"],
                fwd_out["q_curr_repeat"],
            ) = self._repeat_actions(batch[Columns.OBS])
            # Then, for the next observations and the current action distribution.
            (
                fwd_out["actions_next_repeat"],
                fwd_out["logps_next_repeat"],
                fwd_out["q_next_repeat"],
            ) = self._repeat_actions(batch[Columns.NEXT_OBS])
        # Reset the gradient requirements for all Q-function parameters.
        for param in all_params:
            param.requires_grad = True

        return fwd_out

    def _repeat_tensor(self, tensor, repeat):
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

    def _repeat_actions(self, obs, actions):
        """Generated actions for repeated observations.

        The `num_actions` define a multiplier used for generating `num_actions`
        as many actions as the batch size. Observations are repeated and then a
        model forward pass is made.

        Args:
            action_dist_class: The action distribution class to be sued for sampling
                actions.
            obs: A batched observation tensor.
            num_actions: The multiplier for actions, i.e. how much more actions
                than the batch size should be generated.
            module_id: The module ID to be used when calling the forward pass.

        Returns:
            A tuple containing the sampled actions, their log-probabilities and the
            repeated observations.
        """
        output = {}
        # Receive the batch size.
        batch_size = obs.shape[0]
        # Receive the number of action to sample.
        num_actions = self.config.model_config_dict.num_actions
        # Repeat the observations `num_actions` times.
        obs_repeat = tree.map_structure(
            lambda t: self._repeat_tensor(t, num_actions), obs
        )
        # Generate a batch for the forward pass.
        temp_batch = {Columns.OBS: obs_repeat}
        if not actions:
            # TODO (simon): Run the forward pass in inference mode.
            # Compute the action logits.
            pi_encoder_outs = self.pi_encoder(temp_batch)
            action_logits = self.pi(pi_encoder_outs[ENCODER_OUT])
            # Generate the squashed Gaussian from the model's logits.
            action_dist = self.get_train_action_dist_cls().from_logits(action_logits)
            # Sample the actions. Note, we want to make a backward pass through
            # these actions.
            output[Columns.ACTIONS] = (
                action_dist.rsample()
                if not config.model_config_dict._deterministic_loss
                else action_dist.to_deterministic().sample()
            )
            # Compute the action log-probabilities.
            output[Columns.ACTION_LOGP] = action_dist.logp(
                output[Columns.ACTIONS]
            ).view(batch_size, num_actions, 1)

        # Compute all Q-values.
        temp_batch.update(
            {
                Columns.ACTIONS: output[Columns.ACTIONS],
            }
        )
        output.update(
            self._qf_forward_train_helper(
                temp_batch,
                self.qf_encoder,
                self.qf,
            ).view(batch_size, num_actions, 1)
        )
        # If we have a twin-Q network, compute its Q-values, too.
        if self.twin_q:
            output.update(
                self._qf_forward_train_helper(
                    temp_batch,
                    self.qf_twin_encoder,
                    self.qf_twin,
                ).view(batch_size, num_actions, 1)
            )
        del temp_batch

        # Return
        return output
