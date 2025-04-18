"""Asynchronous Proximal Policy Optimization (APPO)

The algorithm is described in [1] (under the name of "IMPACT"):

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#appo

[1] IMPACT: Importance Weighted Asynchronous Architectures with Clipped Target Networks.
Luo et al. 2020
https://arxiv.org/pdf/1912.00167
"""
from typing import Dict

from ray.rllib.algorithms.appo.appo import (
    APPOConfig,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
)
from ray.rllib.algorithms.appo.appo_learner import APPOLearner
from ray.rllib.algorithms.impala.torch.impala_torch_learner import IMPALATorchLearner
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    make_time_major,
    vtrace_torch,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.rl_module.apis import (
    TARGET_NETWORK_ACTION_DIST_INPUTS,
    TargetNetworkAPI,
    ValueFunctionAPI,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import ModuleID, TensorType

torch, nn = try_import_torch()


class APPOTorchLearner(APPOLearner, IMPALATorchLearner):
    """Implements APPO loss / update logic on top of IMPALATorchLearner."""

    @override(IMPALATorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: APPOConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        module = self.module[module_id].unwrapped()
        assert isinstance(module, TargetNetworkAPI)
        assert isinstance(module, ValueFunctionAPI)

        # TODO (sven): Now that we do the +1ts trick to be less vulnerable about
        #  bootstrap values at the end of rollouts in the new stack, we might make
        #  this a more flexible, configurable parameter for users, e.g.
        #  `v_trace_seq_len` (independent of `rollout_fragment_length`). Separation
        #  of concerns (sampling vs learning).
        rollout_frag_or_episode_len = config.get_rollout_fragment_length()
        recurrent_seq_len = batch.get("seq_lens")

        loss_mask = batch[Columns.LOSS_MASK].float()
        loss_mask_time_major = make_time_major(
            loss_mask,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        size_loss_mask = torch.sum(loss_mask)

        values = module.compute_values(
            batch, embeddings=fwd_out.get(Columns.EMBEDDINGS)
        )

        action_dist_cls_train = module.get_train_action_dist_cls()
        target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        old_target_policy_dist = action_dist_cls_train.from_logits(
            module.forward_target(batch)[TARGET_NETWORK_ACTION_DIST_INPUTS]
        )
        old_target_policy_actions_logp = old_target_policy_dist.logp(
            batch[Columns.ACTIONS]
        )
        behaviour_actions_logp = batch[Columns.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[Columns.ACTIONS])

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        rewards_time_major = make_time_major(
            batch[Columns.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        assert Columns.VALUES_BOOTSTRAPPED not in batch
        # Use as bootstrap values the vf-preds in the next "batch row", except
        # for the very last row (which doesn't have a next row), for which the
        # bootstrap value does not matter b/c it has a +1ts value at its end
        # anyways. So we chose an arbitrary item (for simplicity of not having to
        # move new data to the device).
        bootstrap_values = torch.cat(
            [
                values_time_major[0][1:],  # 0th ts values from "next row"
                values_time_major[0][0:1],  # <- can use any arbitrary value here
            ],
            dim=0,
        )

        # The discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - make_time_major(
                batch[Columns.TERMINATEDS],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=recurrent_seq_len,
            ).float()
        ) * config.gamma

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_values=bootstrap_values,
            clip_pg_rho_threshold=config.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=config.vtrace_clip_rho_threshold,
        )
        pg_advantages = pg_advantages * loss_mask_time_major

        # The policy gradients loss.
        is_ratio = torch.clip(
            torch.exp(behaviour_actions_logp_time_major - old_actions_logp_time_major),
            0.0,
            2.0,
        )
        logp_ratio = is_ratio * torch.exp(
            target_actions_logp_time_major - behaviour_actions_logp_time_major
        )

        surrogate_loss = torch.minimum(
            pg_advantages * logp_ratio,
            pg_advantages
            * torch.clip(logp_ratio, 1 - config.clip_param, 1 + config.clip_param),
        )

        if config.use_kl_loss:
            action_kl = old_target_policy_dist.kl(target_policy_dist) * loss_mask
            mean_kl_loss = torch.sum(action_kl) / size_loss_mask
        else:
            mean_kl_loss = 0.0
        mean_pi_loss = -(torch.sum(surrogate_loss) / size_loss_mask)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0) * loss_mask_time_major)
        mean_vf_loss = vf_loss / size_loss_mask

        # The entropy loss.
        mean_entropy_loss = (
            -torch.sum(target_policy_dist.entropy() * loss_mask) / size_loss_mask
        )

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + (mean_vf_loss * config.vf_loss_coeff)
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
            + (mean_kl_loss * self.curr_kl_coeffs_per_module[module_id])
        )

        # Log important loss stats.
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: mean_pi_loss,
                VF_LOSS_KEY: mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
                LEARNER_RESULTS_KL_KEY: mean_kl_loss,
                LEARNER_RESULTS_CURR_KL_COEFF_KEY: (
                    self.curr_kl_coeffs_per_module[module_id]
                ),
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # Return the total loss.
        return total_loss

    @override(APPOLearner)
    def _update_module_kl_coeff(self, module_id: ModuleID, config: APPOConfig) -> None:
        # Update the current KL value based on the recently measured value.
        # Increase.
        kl = convert_to_numpy(self.metrics.peek((module_id, LEARNER_RESULTS_KL_KEY)))
        kl_coeff_var = self.curr_kl_coeffs_per_module[module_id]

        if kl > 2.0 * config.kl_target:
            # TODO (Kourosh) why not *2.0?
            kl_coeff_var.data *= 1.5
        # Decrease.
        elif kl < 0.5 * config.kl_target:
            kl_coeff_var.data *= 0.5

        self.metrics.log_value(
            (module_id, LEARNER_RESULTS_CURR_KL_COEFF_KEY),
            kl_coeff_var.item(),
            window=1,
        )
