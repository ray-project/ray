import logging
from typing import Any, Dict, Mapping

from lagrange_ppo.cost_postprocessing import (
    CostValuePostprocessing,
    Postprocessing,
    RewardValuePostprocessing,
)
from lagrange_ppo.l_ppo_learner import (
    D_PART,
    I_PART,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_CURR_LARGANGE_PENALTY_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_MEAN_CONST_VIOL,
    MEAN_ACCUMULATED_COST,
    P_PART,
    PENALTY,
    POLICY_COST_LOSS_KEY,
    POLICY_REWARD_LOSS_KEY,
    SMOOTHED_VIOLATION,
    PPOLagrangeLearner,
    PPOLagrangeLearnerHyperparameters,
)

from ray.rllib.core.learner.learner import ENTROPY_KEY, POLICY_LOSS_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.torch_utils import explained_variance, sequence_mask
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def polyak_update(
    previous_value: TensorType, update_value: TensorType, alpha: float
) -> TensorType:
    return (1 - alpha) * previous_value + alpha * update_value


class PPOLagrangeTorchLearner(PPOLagrangeLearner, TorchLearner):
    """Implements torch-specific PPO loss logic on top of PPOLearner.

    This class implements the ppo loss under `self.compute_loss_for_module()`.
    """

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: PPOLagrangeLearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType],
    ) -> TensorType:
        # TODO (Kourosh): batch type is NestedDict.
        # TODO (Kourosh): We may or may not user module_id. For example if we have an
        # agent based learning rate scheduler, we may want to use module_id to get the
        # learning rate for that agent.

        # RNN case: Mask away 0-padded chunks at end of time axis.
        if self.module[module_id].is_stateful():
            # In the RNN case, we expect incoming tensors to be padded to the maximum
            # sequence length. We infer the max sequence length from the actions
            # tensor.
            maxlen = torch.max(batch[SampleBatch.SEQ_LENS])
            mask = sequence_mask(batch[SampleBatch.SEQ_LENS], maxlen=maxlen)
            num_valid = torch.sum(mask)

            def possibly_masked_mean(t):
                return torch.sum(t[mask]) / num_valid

        # non-RNN case: No masking.
        else:
            mask = None
            possibly_masked_mean = torch.mean

        action_dist_class_train = (
            self.module[module_id].unwrapped().get_train_action_dist_cls()
        )
        action_dist_class_exploration = (
            self.module[module_id].unwrapped().get_exploration_action_dist_cls()
        )

        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        prev_action_dist = action_dist_class_exploration.from_logits(
            batch[SampleBatch.ACTION_DIST_INPUTS]
        )

        logp_ratio = torch.exp(
            curr_action_dist.logp(batch[SampleBatch.ACTIONS])
            - batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if hps.use_kl_loss:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = possibly_masked_mean(action_kl)
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)
        curr_entropy = curr_action_dist.entropy()
        mean_entropy = possibly_masked_mean(curr_entropy)

        def update_value_function(
            current_batch: SampleBatch,
            post_proc: Postprocessing,
            use_critic: bool,
            clip_param: float,
            clip_ratio: bool = True,
        ):
            surrogate_obj = current_batch[post_proc.ADVANTAGES] * logp_ratio
            if clip_ratio:
                surrogate_obj = torch.min(
                    surrogate_obj,
                    current_batch[post_proc.ADVANTAGES]
                    * torch.clamp(logp_ratio, 1 - hps.clip_param, 1 + hps.clip_param),
                )
            surrogate_loss = -surrogate_obj
            # Compute a value function loss.
            if use_critic:
                value_fn_out = fwd_out[post_proc.VF_PREDS]
                vf_loss = torch.pow(
                    value_fn_out - current_batch[post_proc.VALUE_TARGETS], 2.0
                )
                vf_loss_clipped = torch.clamp(vf_loss, 0, clip_param)
                mean_vf_loss = possibly_masked_mean(vf_loss_clipped)
                mean_vf_unclipped_loss = possibly_masked_mean(vf_loss)
            # Ignore the value function.
            else:
                value_fn_out = torch.tensor(0.0).to(surrogate_loss.device)
                mean_vf_unclipped_loss = torch.tensor(0.0).to(surrogate_loss.device)
                vf_loss_clipped = mean_vf_loss = torch.tensor(0.0).to(
                    surrogate_loss.device
                )

            # Creating metrics
            metrics = {
                post_proc.VF_LOSS_KEY: mean_vf_loss,
                post_proc.LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
                post_proc.LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                    current_batch[post_proc.VALUE_TARGETS], value_fn_out
                ),
            }

            return surrogate_loss, vf_loss_clipped, metrics

        reward_surrogate_loss, vf_loss_clipped, value_metrics = update_value_function(
            current_batch=batch,
            post_proc=RewardValuePostprocessing,
            use_critic=hps.use_critic,
            clip_param=hps.vf_clip_param,
            clip_ratio=True,
        )

        cost_surrogate_loss, cvf_loss_clipped, cost_metrics = update_value_function(
            current_batch=batch,
            post_proc=CostValuePostprocessing,
            use_critic=hps.use_cost_critic,
            clip_param=hps.cvf_clip_param,
            clip_ratio=hps.clip_cost_cvf,
        )

        current_lagrange_penalty_coefficients = (
            self.curr_lagrange_penalty_coeffs_per_module[module_id]
        )
        current_penalty = current_lagrange_penalty_coefficients[PENALTY]
        surrogate_loss = (
            reward_surrogate_loss - current_penalty * cost_surrogate_loss
        ) / (1 + current_penalty)

        total_loss = possibly_masked_mean(
            surrogate_loss
            + hps.vf_loss_coeff * vf_loss_clipped
            + hps.cvf_loss_coeff * cvf_loss_clipped
            - (
                self.entropy_coeff_schedulers_per_module[module_id].get_current_value()
                * curr_entropy
            )
        )

        # Add mean_kl_loss (already processed through `possibly_masked_mean`),
        # if necessary.
        if hps.use_kl_loss:
            total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

        # Register important loss stats.
        metrics = {
            POLICY_LOSS_KEY: possibly_masked_mean(surrogate_loss),
            ENTROPY_KEY: mean_entropy.item(),
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
        }
        if hps.track_debuging_values:
            metrics.update(
                {
                    POLICY_REWARD_LOSS_KEY: possibly_masked_mean(reward_surrogate_loss),
                    POLICY_COST_LOSS_KEY: possibly_masked_mean(cost_surrogate_loss),
                }
            )

        # Register metrics for the reward value function
        metrics.update(value_metrics)

        # Register metrics for the cost value function
        metrics.update(cost_metrics)

        self.register_metrics(module_id, metrics)
        # Return the total loss.
        return total_loss

    @override(PPOLagrangeLearner)
    def additional_update_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: PPOLagrangeLearnerHyperparameters,
        timestep: int,
        sampled_kl_values: dict,
        sampled_lp_values: dict,
    ) -> Dict[str, Any]:
        assert sampled_kl_values, "Sampled KL values are empty."

        results = super().additional_update_for_module(
            module_id=module_id,
            hps=hps,
            timestep=timestep,
            sampled_kl_values=sampled_kl_values,
        )

        # Update KL coefficient.
        if hps.use_kl_loss:
            sampled_kl = sampled_kl_values[module_id]
            curr_var = self.curr_kl_coeffs_per_module[module_id]
            if sampled_kl > 2.0 * self.hps.kl_target:
                # TODO (Kourosh) why not 2?
                curr_var.data *= 1.5
            elif sampled_kl < 0.5 * self.hps.kl_target:
                curr_var.data *= 0.5
            results.update({LEARNER_RESULTS_CURR_KL_COEFF_KEY: curr_var.item()})

        # Update Largange coefficient.
        if hps.learn_penalty_coeff:
            current_lagrange_penalty_data = (
                self.curr_lagrange_penalty_coeffs_per_module[module_id]
            )
            # previous infos
            penalty = current_lagrange_penalty_data[PENALTY]
            i_part = current_lagrange_penalty_data[I_PART]
            smoothed_violation = current_lagrange_penalty_data[SMOOTHED_VIOLATION]
            # Low pass filter smoothing constraint violation curve
            acc_costs = (
                torch.tensor(sampled_lp_values[module_id])
                .to(device=smoothed_violation.device)
                .mean()
            )
            mean_constraint_violation = acc_costs - hps.cost_limit * torch.ones_like(
                acc_costs
            ).to(device=smoothed_violation.device)
            new_smoothed_violation = polyak_update(
                previous_value=smoothed_violation.data,
                update_value=mean_constraint_violation,
                alpha=hps.polyak_coeff,
            )
            # computing the gradient with respect to the penalty
            lagrange_grad = 1 - torch.exp(-penalty.data)
            # I part
            i_part.data += (
                hps.penalty_coeff_lr * new_smoothed_violation
            ) * lagrange_grad
            # P part computation
            p_part = hps.p_coeff * new_smoothed_violation * lagrange_grad
            # D part computation
            d_part = (
                hps.d_coeff
                * (new_smoothed_violation - smoothed_violation.data)
                * lagrange_grad
            )
            # updating penalty coefficient
            raw_penalty = torch.nn.functional.softplus(p_part + i_part + d_part)
            # clipping the maximum possible penalty
            penalty.data = torch.clip(raw_penalty, max=hps.max_penalty_coeff)
            # updating smoothed violations
            smoothed_violation.data = new_smoothed_violation

            results.update(
                {
                    LEARNER_RESULTS_CURR_LARGANGE_PENALTY_COEFF_KEY: penalty.item(),
                    LEARNER_RESULTS_MEAN_CONST_VIOL: mean_constraint_violation.item()
                    / hps.cost_limit,
                }
            )

            if hps.track_debuging_values:
                results.update(
                    {
                        SMOOTHED_VIOLATION: new_smoothed_violation.item()
                        / hps.cost_limit,
                        D_PART: d_part.item(),
                        I_PART: i_part.item(),
                        P_PART: p_part.item(),
                        MEAN_ACCUMULATED_COST: acc_costs.item(),
                    }
                )

        return results
