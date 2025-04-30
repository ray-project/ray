from typing import Dict

from ray.rllib.algorithms.impala.impala import IMPALAConfig
from ray.rllib.algorithms.impala.impala_learner import IMPALALearner
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    vtrace_torch,
    make_time_major,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, TensorType

torch, nn = try_import_torch()


class IMPALATorchLearner(IMPALALearner, TorchLearner):
    """Implements the IMPALA loss function in torch."""

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: IMPALAConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        module = self.module[module_id].unwrapped()

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

        # Behavior actions logp and target actions logp.
        behaviour_actions_logp = batch[Columns.ACTION_LOGP]
        target_policy_dist = module.get_train_action_dist_cls().from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        target_actions_logp = target_policy_dist.logp(batch[Columns.ACTIONS])

        # Values and bootstrap values.
        values = module.compute_values(
            batch, embeddings=fwd_out.get(Columns.EMBEDDINGS)
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

        # TODO(Artur): In the old impala code, actions were unsqueezed if they were
        #  multi_discrete. Find out why and if we need to do the same here.
        #  actions = actions if is_multidiscrete else torch.unsqueeze(actions, dim=1)
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        rewards_time_major = make_time_major(
            batch[Columns.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - make_time_major(
                batch[Columns.TERMINATEDS],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=recurrent_seq_len,
            ).type(dtype=torch.float32)
        ) * config.gamma

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_values=bootstrap_values,
            clip_rho_threshold=config.vtrace_clip_rho_threshold,
            clip_pg_rho_threshold=config.vtrace_clip_pg_rho_threshold,
        )

        # The policy gradients loss.
        pi_loss = -torch.sum(
            target_actions_logp_time_major * pg_advantages * loss_mask_time_major
        )
        mean_pi_loss = pi_loss / size_loss_mask

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0) * loss_mask_time_major)
        mean_vf_loss = vf_loss / size_loss_mask

        # The entropy loss.
        entropy_loss = -torch.sum(target_policy_dist.entropy() * loss_mask)
        mean_entropy_loss = entropy_loss / size_loss_mask

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + mean_vf_loss * config.vf_loss_coeff
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
        )

        # Log important loss stats.
        self.metrics.log_dict(
            {
                "pi_loss": pi_loss,
                "mean_pi_loss": mean_pi_loss,
                "vf_loss": vf_loss,
                "mean_vf_loss": mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # Return the total loss.
        return total_loss


ImpalaTorchLearner = IMPALATorchLearner
