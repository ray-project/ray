from typing import Dict

from ray.rllib.algorithms.impala.impala import ImpalaConfig
from ray.rllib.algorithms.impala.impala_learner import ImpalaLearner
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    vtrace_torch,
    make_time_major,
)
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.models.base import CRITIC, ENCODER_OUT
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import ModuleID, TensorType

torch, nn = try_import_torch()


class ImpalaTorchLearner(ImpalaLearner, TorchLearner):
    """Implements the IMPALA loss function in torch."""

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: ImpalaConfig,
        batch: NestedDict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        action_dist_class_train = (
            self.module[module_id].unwrapped().get_train_action_dist_cls()
        )
        target_policy_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        values = fwd_out[Columns.VF_PREDS]

        behaviour_actions_logp = batch[Columns.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[Columns.ACTIONS])
        rollout_frag_or_episode_len = config.get_rollout_fragment_length()
        recurrent_seq_len = None

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
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=recurrent_seq_len,
        )
        if self.config.enable_env_runner_and_connector_v2:
            bootstrap_values = batch[Columns.VALUES_BOOTSTRAPPED]
        else:
            bootstrap_values_time_major = make_time_major(
                batch[Columns.VALUES_BOOTSTRAPPED],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=recurrent_seq_len,
            )
            bootstrap_values = bootstrap_values_time_major[-1]

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
            bootstrap_values=bootstrap_values,
            clip_rho_threshold=config.vtrace_clip_rho_threshold,
            clip_pg_rho_threshold=config.vtrace_clip_pg_rho_threshold,
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
            + vf_loss * config.vf_loss_coeff
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
                "pi_loss": mean_pi_loss,
                "vf_loss": mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
            },
            key=module_id,
            window=1,  # <- single items (should not be mean/ema-reduced over time).
        )
        # Return the total loss.
        return total_loss

    @override(ImpalaLearner)
    def _compute_values(self, batch):
        infos = batch.pop(Columns.INFOS, None)
        batch = convert_to_torch_tensor(batch, device=self._device)
        # batch = tree.map_structure(lambda s: torch.from_numpy(s), batch)
        if infos is not None:
            batch[Columns.INFOS] = infos

        # TODO (sven): Make multi-agent capable.
        module = self.module[DEFAULT_MODULE_ID].unwrapped()

        # Shared encoder.
        encoder_outs = module.encoder(batch)
        # Value head.
        vf_out = module.vf(encoder_outs[ENCODER_OUT][CRITIC])
        # Squeeze out last dimension (single node value head).
        return vf_out.squeeze(-1)
