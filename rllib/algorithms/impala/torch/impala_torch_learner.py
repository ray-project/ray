from typing import Mapping

from ray.rllib.algorithms.impala.impala_learner import (
    ImpalaLearner,
    ImpalaLearnerHyperparameters,
)
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    vtrace_torch,
    make_time_major,
)
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.core.learner.learner import ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class ImpalaTorchLearner(ImpalaLearner, TorchLearner):
    """Implements the IMPALA loss function in torch."""

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: ImpalaLearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType],
    ) -> TensorType:
        action_dist_class_train = (
            self.module[module_id].unwrapped().get_train_action_dist_cls()
        )
        target_policy_dist = action_dist_class_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        values = fwd_out[SampleBatch.VF_PREDS]

        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])

        # TODO(Artur): In the old impala code, actions were unsqueezed if they were
        #  multi_discrete. Find out why and if we need to do the same here.
        #  actions = actions if is_multidiscrete else torch.unsqueeze(actions, dim=1)

        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        bootstrap_values_time_major = make_time_major(
            batch[SampleBatch.VALUES_BOOTSTRAPPED],
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        bootstrap_value = bootstrap_values_time_major[-1]

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - make_time_major(
                batch[SampleBatch.TERMINATEDS],
                trajectory_len=hps.rollout_frag_or_episode_len,
                recurrent_seq_len=hps.recurrent_seq_len,
            ).type(dtype=torch.float32)
        ) * hps.discount_factor

        # TODO(Artur) Why was there `TorchCategorical if is_multidiscrete else
        #  dist_class` in the old code torch impala policy?
        device = behaviour_actions_logp_time_major[0].device

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_rho_threshold=hps.vtrace_clip_rho_threshold,
            clip_pg_rho_threshold=hps.vtrace_clip_pg_rho_threshold,
        )

        # Sample size is T x B, where T is the trajectory length and B is the batch size
        # We mean over the batch size for consistency with the pre-RLModule
        # implementation of IMPALA
        # TODO(Artur): Mean over trajectory length after migration to RLModules.
        batch_size = (
            convert_to_torch_tensor(target_actions_logp_time_major.shape[-1])
            .float()
            .to(device)
        )

        # The policy gradients loss.
        pi_loss = -torch.sum(target_actions_logp_time_major * pg_advantages)
        mean_pi_loss = pi_loss / batch_size

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0))
        mean_vf_loss = vf_loss / batch_size

        # The entropy loss.
        mean_entropy_loss = -torch.mean(target_policy_dist.entropy())

        # The summed weighted loss.
        total_loss = (
            pi_loss
            + vf_loss * hps.vf_loss_coeff
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
        )

        # Register important loss stats.
        self.register_metrics(
            module_id,
            {
                "pi_loss": mean_pi_loss,
                "vf_loss": mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
            },
        )
        # Return the total loss.
        return total_loss
