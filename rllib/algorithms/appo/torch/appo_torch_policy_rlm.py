import logging

from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    make_time_major,
    vtrace_torch,
)
from ray.rllib.policy.torch_mixins import (
    EntropyCoeffSchedule,
    LearningRateSchedule,
    KLCoeffMixin,
    TargetNetworkMixin,
)
from ray.rllib.algorithms.impala.impala_torch_policy import (
    VTraceOptimizer,
)
from ray.rllib.algorithms.ppo.ppo_torch_policy import validate_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    convert_to_torch_tensor,
    explained_variance,
    global_norm,
)

torch, _ = try_import_torch()

logger = logging.getLogger(__name__)


# TODO: Remove once we have a RLModule capable sampler class that can replace
#  `Policy.compute_actions_from_input_dict()`.
class APPOTorchPolicyWithRLModule(
    VTraceOptimizer,
    LearningRateSchedule,
    KLCoeffMixin,
    EntropyCoeffSchedule,
    TargetNetworkMixin,
    TorchPolicyV2,
):
    def __init__(self, observation_space, action_space, config):
        validate_config(config)
        # Initialize MixIns before super().__init__ because base class will call
        # self.loss, which requires these MixIns to be initialized.
        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )
        # Although this is a no-op, we call __init__ here to make it clear
        # that base.__init__ will use the make_model() call.
        # VTraceClipGradients.__init__(self)
        VTraceOptimizer.__init__(self)
        self.framework = "tf2"
        KLCoeffMixin.__init__(self, config)
        # GradStatsMixin.__init__(self)
        TorchPolicyV2.__init__(self, observation_space, action_space, config)
        # Construct the target model and make its weights the same as the model.
        self.target_model = self.make_rl_module()
        self.target_model.load_state_dict(self.model.state_dict())

        # Initiate TargetNetwork ops after loss initialization.
        self._initialize_loss_from_dummy_batch()
        TargetNetworkMixin.__init__(self)

    @override(TorchPolicyV2)
    def loss(self, model, dist_class, train_batch):
        train_batch[SampleBatch.ACTION_LOGP]
        train_batch[SampleBatch.ACTIONS]
        train_batch[SampleBatch.REWARDS]
        train_batch[SampleBatch.TERMINATEDS]

        seqs_len = train_batch.get(SampleBatch.SEQ_LENS)
        rollout_frag_or_episode_len = (
            self.config["rollout_fragment_length"] if not seqs_len else None
        )
        drop_last = self.config["vtrace_drop_last_ts"]

        target_policy_fwd_out = model.forward_train(train_batch)
        values = target_policy_fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = target_policy_fwd_out[SampleBatch.ACTION_DIST]

        old_target_policy_fwd_out = self.target_model.forward_train(train_batch)
        old_target_policy_dist = old_target_policy_fwd_out[SampleBatch.ACTION_DIST]

        behaviour_actions_logp = train_batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(train_batch[SampleBatch.ACTIONS])
        old_target_actions_logp = old_target_policy_dist.logp(
            train_batch[SampleBatch.ACTIONS]
        )
        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seqs_len,
            drop_last=drop_last,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seqs_len,
            drop_last=drop_last,
        )
        old_target_actions_logp_time_major = make_time_major(
            old_target_actions_logp,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seqs_len,
            drop_last=drop_last,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seqs_len,
            drop_last=drop_last,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            train_batch[SampleBatch.REWARDS],
            trajectory_len=rollout_frag_or_episode_len,
            recurrent_seq_len=seqs_len,
            drop_last=drop_last,
        )

        # how to compute discouts?
        # should they be pre computed?
        discounts_time_major = (
            1.0
            - make_time_major(
                train_batch[SampleBatch.TERMINATEDS],
                trajectory_len=rollout_frag_or_episode_len,
                recurrent_seq_len=seqs_len,
                drop_last=drop_last,
            ).float()
        ) * self.config["gamma"]

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=old_target_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=self.config["vtrace_clip_pg_rho_threshold"],
            clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
        )

        is_ratio = torch.clip(
            torch.exp(
                behaviour_actions_logp_time_major - target_actions_logp_time_major
            ),
            0.0,
            2.0,
        )
        logp_ratio = is_ratio * torch.exp(
            target_actions_logp_time_major - behaviour_actions_logp_time_major
        )

        clip_param = self.config["clip_param"]
        surrogate_loss = torch.minimum(
            pg_advantages * logp_ratio,
            (pg_advantages * torch.clip(logp_ratio, 1 - clip_param, 1 + clip_param)),
        )
        action_kl = old_target_policy_dist.kl(target_policy_dist)
        mean_kl_loss = torch.mean(action_kl)
        mean_pi_loss = -torch.mean(surrogate_loss)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        mean_vf_loss = 0.5 * torch.mean(delta**2)

        # The entropy loss.
        mean_entropy_loss = -torch.mean(target_policy_dist.entropy())

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + (mean_vf_loss * self.config["vf_loss_coeff"])
            + (mean_entropy_loss * self.entropy_coeff)
            + (mean_kl_loss * self.kl_coeff)
        )

        self.stats = {
            "total_loss": total_loss,
            "policy_loss": mean_pi_loss,
            "vf_loss": mean_vf_loss,
            "values": values_time_major,
            "entropy_loss": mean_entropy_loss,
            "vtrace_adjusted_target_values": vtrace_adjusted_target_values,
            "mean_kl": mean_kl_loss,
        }
        return total_loss

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch):
        return {
            "cur_lr": convert_to_torch_tensor(self.cur_lr).type(torch.float64),
            "policy_loss": self.stats["policy_loss"],
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
            "mean_kl": self.stats["mean_kl"],
        }

    @override(TorchPolicyV2)
    def get_batch_divisibility_req(self) -> int:
        return self.config["rollout_fragment_length"]
