from typing import Mapping

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.appo.appo_learner import (
    AppoLearner,
    LEARNER_RESULTS_KL_KEY,
    OLD_ACTION_DIST_KEY,
)
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    make_time_major,
    vtrace_torch,
)
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class APPOTorchLearner(TorchLearner, AppoLearner):
    """Implements APPO loss / update logic on top of ImpalaTorchLearner."""

    def __init__(self, *args, **kwargs):
        TorchLearner.__init__(self, *args, **kwargs)
        AppoLearner.__init__(self, *args, **kwargs)

    @override(TorchLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:

        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]
        old_target_policy_dist = fwd_out[OLD_ACTION_DIST_KEY]
        old_target_policy_actions_logp = old_target_policy_dist.logp(
            batch[SampleBatch.ACTIONS]
        )
        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=self._hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self._hps.recurrent_seq_len,
            drop_last=self._hps.vtrace_drop_last_ts,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=self._hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self._hps.recurrent_seq_len,
            drop_last=self._hps.vtrace_drop_last_ts,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=self._hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self._hps.recurrent_seq_len,
            drop_last=self._hps.vtrace_drop_last_ts,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=self._hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self._hps.recurrent_seq_len,
            drop_last=self._hps.vtrace_drop_last_ts,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=self._hps.rollout_frag_or_episode_len,
            recurrent_seq_len=self._hps.recurrent_seq_len,
            drop_last=self._hps.vtrace_drop_last_ts,
        )

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            -
            make_time_major(
                batch[SampleBatch.TERMINATEDS],
                trajectory_len=self._hps.rollout_frag_or_episode_len,
                recurrent_seq_len=self._hps.recurrent_seq_len,
                drop_last=self._hps.vtrace_drop_last_ts,
            ).float()
        ) * self._hps.discount_factor

        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self._hps.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=self._hps.vtrace_clip_rho_threshold,
        )

        # The policy gradients loss.
        is_ratio = torch.clip(
            torch.exp(
                behaviour_actions_logp_time_major - old_actions_logp_time_major
            ),
            0.0,
            2.0,
        )
        logp_ratio = is_ratio * torch.exp(
            target_actions_logp_time_major - behaviour_actions_logp_time_major
        )

        surrogate_loss = torch.minimum(
            pg_advantages * logp_ratio,
            pg_advantages * torch.clip(
                logp_ratio, 1 - self._hps.clip_param, 1 + self._hps.clip_param
            ),
        )

        if self._hps.use_kl_loss:
            action_kl = old_target_policy_dist.kl(target_policy_dist)
            mean_kl_loss = torch.mean(action_kl)
        else:
            mean_kl_loss = 0.0
        mean_pi_loss = -torch.mean(surrogate_loss)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        mean_vf_loss = 0.5 * torch.mean(delta**2)

        # The entropy loss.
        mean_entropy_loss = -torch.mean(target_policy_dist.entropy())

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + (mean_vf_loss * self._hps.vf_loss_coeff)
            + (mean_entropy_loss * self._hps.entropy_coeff)
            + (mean_kl_loss * self.kl_coeffs[module_id])
        )

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            POLICY_LOSS_KEY: mean_pi_loss,
            VF_LOSS_KEY: mean_vf_loss,
            ENTROPY_KEY: -mean_entropy_loss,
            LEARNER_RESULTS_KL_KEY: mean_kl_loss,
        }

    def _update_module_target_networks(self, module_id: ModuleID):
        """Update the target policy of each module with the current policy.

        Do that update via polyak averaging.

        Args:
            module_id: The module whose target networks need to be updated.

        """
        module = self.module[module_id]

        target_current_network_pairs = module.get_target_network_pairs()
        for target_network, current_network in target_current_network_pairs:
            current_state_dict = current_network.state_dict()
            new_state_dict = {
                k: self._hps.tau * current_state_dict[k] + (1 - self._hps.tau) * v
                for k, v in target_network.state_dict().items()
            }
            target_network.load_state_dict(new_state_dict)
