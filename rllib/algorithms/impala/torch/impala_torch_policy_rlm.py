import logging
from typing import Dict, List, Union

from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    make_time_major,
    vtrace_torch,
)
from ray.rllib.algorithms.ppo.ppo_torch_policy import validate_config
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.policy.torch_mixins import (
    EntropyCoeffSchedule,
    LearningRateSchedule,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override, Deprecated
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    explained_variance,
    global_norm,
)
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class ImpalaTorchPolicyWithRLModule(
    LearningRateSchedule,
    EntropyCoeffSchedule,
    TorchPolicyV2,
):
    def __init__(self, observation_space, action_space, config):
        validate_config(config)
        TorchPolicyV2.__init__(self, observation_space, action_space, config)
        # Initialize MixIns.
        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

    @Deprecated(new="ImpalaTorchLearner.compute_loss_per_module()", error=False)
    @override(TorchPolicyV2)
    def loss(
        self,
        model: PPOTorchRLModule,
        dist_class,
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        seq_len = train_batch.get(SampleBatch.SEQ_LENS)
        rollout_frag_or_episode_len = (
            self.config["rollout_fragment_length"] if not seq_len else None
        )
        drop_last = self.config["vtrace_drop_last_ts"]

        fwd_out = model.forward_train(train_batch)

        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]

        # this is probably a horribly inefficient way to do this. I should be able to
        # compute this in a batch fashion
        behaviour_actions_logp = train_batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(train_batch[SampleBatch.ACTIONS])
        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seq_len,
            drop_last=drop_last,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seq_len,
            drop_last=drop_last,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seq_len,
            drop_last=drop_last,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            train_batch[SampleBatch.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seq_len,
            drop_last=drop_last,
        )

        # how to compute discouts?
        # should they be pre computed?
        discounts_time_major = (
            1.0
            - make_time_major(
                train_batch[SampleBatch.TERMINATEDS],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=seq_len,
                drop_last=drop_last,
            ).type(dtype=torch.float32)
        ) * self.config["gamma"]
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self.config["vtrace_clip_pg_rho_threshold"],
            clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
            discounts=discounts_time_major,
        )

        # The policy gradients loss.
        pi_loss = -torch.sum(target_actions_logp_time_major * pg_advantages)
        mean_pi_loss = -torch.mean(target_actions_logp_time_major * pg_advantages)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0))
        mean_vf_loss = 0.5 * torch.mean(torch.pow(delta, 2.0))

        # The entropy loss.
        entropy_loss = -torch.sum(target_actions_logp_time_major)

        # The summed weighted loss.
        total_loss = (
            pi_loss
            + vf_loss * self.config["vf_loss_coeff"]
            + entropy_loss * self.entropy_coeff
        )
        self.stats = {
            "total_loss": total_loss,
            "pi_loss": mean_pi_loss,
            "vf_loss": mean_vf_loss,
            "values": values_time_major,
            "entropy_loss": entropy_loss,
            "vtrace_adjusted_target_values": vtrace_adjusted_target_values,
        }
        return total_loss

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return {
            "cur_lr": convert_to_torch_tensor(self.cur_lr).type(torch.float64),
            "policy_loss": self.stats["pi_loss"],
            "entropy": self.stats["entropy_loss"],
            "entropy_coeff": convert_to_torch_tensor(self.entropy_coeff).type(
                torch.float64
            ),
            "var_gnorm": global_norm(self.model.parameters()),
            "vf_loss": self.stats["vf_loss"],
            "vf_explained_var": explained_variance(
                torch.reshape(self.stats["vtrace_adjusted_target_values"], [-1]),
                torch.reshape(self.stats["values"], [-1]),
            ),
        }

    @override(TorchPolicyV2)
    def get_batch_divisibility_req(self) -> int:
        return self.config["rollout_fragment_length"]
