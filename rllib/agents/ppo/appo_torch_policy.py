"""Adapted from VTraceTFPolicy to use the PPO surrogate loss.

Keep in sync with changes to VTraceTFPolicy."""

import numpy as np
import logging
import gym

from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
import ray.rllib.agents.impala.vtrace_torch as vtrace
from ray.rllib.agents.impala.vtrace_torch_policy import make_time_major, \
    choose_optimizer
from ray.rllib.agents.ppo.appo_tf_policy import build_appo_model, \
    postprocess_trajectory
from ray.rllib.agents.ppo.ppo_torch_policy import ValueNetworkMixin, \
    KLCoeffMixin
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import LearningRateSchedule
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import global_norm, sequence_mask

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class PPOSurrogateLoss:
    """Loss used when V-trace is disabled.

    Arguments:
        prev_actions_logp: A float32 tensor of shape [T, B].
        actions_logp: A float32 tensor of shape [T, B].
        action_kl: A float32 tensor of shape [T, B].
        actions_entropy: A float32 tensor of shape [T, B].
        values: A float32 tensor of shape [T, B].
        valid_mask: A bool tensor of valid RNN input elements (#2992).
        advantages: A float32 tensor of shape [T, B].
        value_targets: A float32 tensor of shape [T, B].
        vf_loss_coeff (float): Coefficient of the value function loss.
        entropy_coeff (float): Coefficient of the entropy regularizer.
        clip_param (float): Clip parameter.
        cur_kl_coeff (float): Coefficient for KL loss.
        use_kl_loss (bool): If true, use KL loss.
    """

    def __init__(self,
                 prev_actions_logp,
                 actions_logp,
                 action_kl,
                 actions_entropy,
                 values,
                 valid_mask,
                 advantages,
                 value_targets,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01,
                 clip_param=0.3,
                 cur_kl_coeff=None,
                 use_kl_loss=False):

        if valid_mask is not None:
            num_valid = torch.sum(valid_mask)

            def reduce_mean_valid(t):
                return torch.sum(t * valid_mask) / num_valid

        else:

            def reduce_mean_valid(t):
                return torch.mean(t)

        logp_ratio = torch.exp(actions_logp - prev_actions_logp)

        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages * torch.clamp(logp_ratio, 1 - clip_param,
                                     1 + clip_param))

        self.mean_kl = reduce_mean_valid(action_kl)
        self.pi_loss = -reduce_mean_valid(surrogate_loss)

        # The baseline loss
        delta = values - value_targets
        self.value_targets = value_targets
        self.vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

        # The entropy loss
        self.entropy = reduce_mean_valid(actions_entropy)

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss
        if use_kl_loss:
            self.total_loss += cur_kl_coeff * self.mean_kl


class VTraceSurrogateLoss:
    def __init__(self,
                 actions,
                 prev_actions_logp,
                 actions_logp,
                 old_policy_actions_logp,
                 action_kl,
                 actions_entropy,
                 dones,
                 behaviour_logits,
                 old_policy_behaviour_logits,
                 target_logits,
                 discount,
                 rewards,
                 values,
                 bootstrap_value,
                 dist_class,
                 model,
                 valid_mask,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01,
                 clip_rho_threshold=1.0,
                 clip_pg_rho_threshold=1.0,
                 clip_param=0.3,
                 cur_kl_coeff=None,
                 use_kl_loss=False):
        """APPO Loss, with IS modifications and V-trace for Advantage Estimation

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Arguments:
            actions: An int|float32 tensor of shape [T, B, logit_dim].
            prev_actions_logp: A float32 tensor of shape [T, B].
            actions_logp: A float32 tensor of shape [T, B].
            old_policy_actions_logp: A float32 tensor of shape [T, B].
            action_kl: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behaviour_logits: A float32 tensor of shape [T, B, logit_dim].
            old_policy_behaviour_logits: A float32 tensor of shape
            [T, B, logit_dim].
            target_logits: A float32 tensor of shape [T, B, logit_dim].
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
            dist_class: action distribution class for logits.
            model: backing ModelV2 instance
            valid_mask: A bool tensor of valid RNN input elements (#2992).
            vf_loss_coeff (float): Coefficient of the value function loss.
            entropy_coeff (float): Coefficient of the entropy regularizer.
            clip_param (float): Clip parameter.
            cur_kl_coeff (float): Coefficient for KL loss.
            use_kl_loss (bool): If true, use KL loss.
        """

        if valid_mask is not None:
            num_valid = torch.sum(valid_mask)

            def reduce_mean_valid(t):
                return torch.sum(t * valid_mask) / num_valid

        else:

            def reduce_mean_valid(t):
                return torch.mean(t)

        # Compute vtrace on the CPU for better perf.
        self.vtrace_returns = vtrace.multi_from_logits(
            behaviour_policy_logits=behaviour_logits,
            target_policy_logits=old_policy_behaviour_logits,
            actions=torch.unbind(actions, dim=2),
            discounts=(1.0 - dones.float()) * discount,
            rewards=rewards,
            values=values,
            bootstrap_value=bootstrap_value,
            dist_class=dist_class,
            model=model,
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold)

        self.is_ratio = torch.clamp(
            torch.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0)
        logp_ratio = self.is_ratio * torch.exp(actions_logp -
                                               prev_actions_logp)

        advantages = self.vtrace_returns.pg_advantages
        surrogate_loss = torch.min(
            advantages * logp_ratio,
            advantages * torch.clamp(logp_ratio, 1 - clip_param,
                                     1 + clip_param))

        self.mean_kl = reduce_mean_valid(action_kl)
        self.pi_loss = -reduce_mean_valid(surrogate_loss)

        # The baseline loss
        delta = values - self.vtrace_returns.vs
        self.value_targets = self.vtrace_returns.vs
        self.vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

        # The entropy loss
        self.entropy = reduce_mean_valid(actions_entropy)

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss
        if use_kl_loss:
            self.total_loss += cur_kl_coeff * self.mean_kl


def build_appo_surrogate_loss(policy, model, dist_class, train_batch):
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)

    if isinstance(policy.action_space, gym.spaces.Discrete):
        is_multidiscrete = False
        output_hidden_shape = [policy.action_space.n]
    elif isinstance(policy.action_space,
                    gym.spaces.multi_discrete.MultiDiscrete):
        is_multidiscrete = True
        output_hidden_shape = policy.action_space.nvec.astype(np.int32)
    else:
        is_multidiscrete = False
        output_hidden_shape = 1

    def _make_time_major(*args, **kw):
        return make_time_major(policy, train_batch.get("seq_lens"), *args,
                               **kw)

    actions = train_batch[SampleBatch.ACTIONS]
    dones = train_batch[SampleBatch.DONES]
    rewards = train_batch[SampleBatch.REWARDS]
    behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]

    target_model_out, _ = policy.target_model.from_batch(train_batch)
    old_policy_behaviour_logits = target_model_out.detach()

    if isinstance(output_hidden_shape, (list, tuple, np.ndarray)):
        unpacked_behaviour_logits = torch.split(
            behaviour_logits, list(output_hidden_shape), dim=1)
        unpacked_old_policy_behaviour_logits = torch.split(
            old_policy_behaviour_logits, list(output_hidden_shape), dim=1)
        unpacked_outputs = torch.split(
            model_out, list(output_hidden_shape), dim=1)
    else:
        unpacked_behaviour_logits = torch.chunk(
            behaviour_logits, output_hidden_shape, dim=1)
        unpacked_old_policy_behaviour_logits = torch.chunk(
            old_policy_behaviour_logits, output_hidden_shape, dim=1)
        unpacked_outputs = torch.chunk(model_out, output_hidden_shape, dim=1)

    old_policy_action_dist = dist_class(old_policy_behaviour_logits, model)
    prev_action_dist = dist_class(behaviour_logits, policy.model)
    values = policy.model.value_function()

    policy.model_vars = policy.model.variables()
    policy.target_model_vars = policy.target_model.variables()

    if policy.is_recurrent():
        max_seq_len = torch.max(train_batch["seq_lens"]) - 1
        mask = sequence_mask(train_batch["seq_lens"], max_seq_len)
        mask = torch.reshape(mask, [-1])
    else:
        mask = torch.ones_like(rewards)

    if policy.config["vtrace"]:
        logger.debug("Using V-Trace surrogate loss (vtrace=True)")

        # Prepare actions for loss
        loss_actions = actions if is_multidiscrete else torch.unsqueeze(
            actions, dim=1)

        # Prepare KL for Loss
        mean_kl = _make_time_major(
            old_policy_action_dist.kl(action_dist), drop_last=True)

        policy.loss = VTraceSurrogateLoss(
            actions=_make_time_major(loss_actions, drop_last=True),
            prev_actions_logp=_make_time_major(
                prev_action_dist.logp(actions), drop_last=True),
            actions_logp=_make_time_major(
                action_dist.logp(actions), drop_last=True),
            old_policy_actions_logp=_make_time_major(
                old_policy_action_dist.logp(actions), drop_last=True),
            action_kl=mean_kl,
            actions_entropy=_make_time_major(
                action_dist.entropy(), drop_last=True),
            dones=_make_time_major(dones, drop_last=True),
            behaviour_logits=_make_time_major(
                unpacked_behaviour_logits, drop_last=True),
            old_policy_behaviour_logits=_make_time_major(
                unpacked_old_policy_behaviour_logits, drop_last=True),
            target_logits=_make_time_major(unpacked_outputs, drop_last=True),
            discount=policy.config["gamma"],
            rewards=_make_time_major(rewards, drop_last=True),
            values=_make_time_major(values, drop_last=True),
            bootstrap_value=_make_time_major(values)[-1],
            dist_class=TorchCategorical if is_multidiscrete else dist_class,
            model=policy.model,
            valid_mask=_make_time_major(mask, drop_last=True),
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_rho_threshold=policy.config["vtrace_clip_rho_threshold"],
            clip_pg_rho_threshold=policy.config[
                "vtrace_clip_pg_rho_threshold"],
            clip_param=policy.config["clip_param"],
            cur_kl_coeff=policy.kl_coeff,
            use_kl_loss=policy.config["use_kl_loss"])
    else:
        logger.debug("Using PPO surrogate loss (vtrace=False)")

        # Prepare KL for Loss
        mean_kl = _make_time_major(prev_action_dist.kl(action_dist))

        policy.loss = PPOSurrogateLoss(
            prev_actions_logp=_make_time_major(prev_action_dist.logp(actions)),
            actions_logp=_make_time_major(action_dist.logp(actions)),
            action_kl=mean_kl,
            actions_entropy=_make_time_major(action_dist.entropy()),
            values=_make_time_major(values),
            valid_mask=_make_time_major(mask),
            advantages=_make_time_major(
                train_batch[Postprocessing.ADVANTAGES]),
            value_targets=_make_time_major(
                train_batch[Postprocessing.VALUE_TARGETS]),
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_param=policy.config["clip_param"],
            cur_kl_coeff=policy.kl_coeff,
            use_kl_loss=policy.config["use_kl_loss"])

    return policy.loss.total_loss


def stats(policy, train_batch):
    values_batched = make_time_major(
        policy,
        train_batch.get("seq_lens"),
        policy.model.value_function(),
        drop_last=policy.config["vtrace"])

    stats_dict = {
        "cur_lr": policy.cur_lr,
        "policy_loss": policy.loss.pi_loss,
        "entropy": policy.loss.entropy,
        "var_gnorm": global_norm(policy.model.trainable_variables()),
        "vf_loss": policy.loss.vf_loss,
        "vf_explained_var": explained_variance(
            torch.reshape(policy.loss.value_targets, [-1]),
            torch.reshape(values_batched, [-1]),
            framework="torch"),
    }

    if policy.config["vtrace"]:
        is_stat_mean = torch.mean(policy.loss.is_ratio, [0, 1])
        is_stat_var = torch.var(policy.loss.is_ratio, [0, 1])
        stats_dict.update({"mean_IS": is_stat_mean})
        stats_dict.update({"var_IS": is_stat_var})

    if policy.config["use_kl_loss"]:
        stats_dict.update({"kl": policy.loss.mean_kl})
        stats_dict.update({"KL_Coeff": policy.kl_coeff})

    return stats_dict


class TargetNetworkMixin:
    def __init__(self, obs_space, action_space, config):
        def do_update():
            # Update_target_fn will be called periodically to copy Q network to
            # target Q network.
            assert len(self.model_variables) == \
                len(self.target_model_variables), \
                (self.model_variables, self.target_model_variables)
            self.target_model.load_state_dict(self.model.state_dict())

        self.update_target = do_update


def add_values(policy, input_dict, state_batches, model, action_dist):
    out = {}
    if not policy.config["vtrace"]:
        out[SampleBatch.VF_PREDS] = policy.model.value_function()
    return out


def setup_early_mixins(policy, obs_space, action_space, config):
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


def setup_late_mixins(policy, obs_space, action_space, config):
    KLCoeffMixin.__init__(policy, config)
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


AsyncPPOTorchPolicy = build_torch_policy(
    name="AsyncPPOTorchPolicy",
    loss_fn=build_appo_surrogate_loss,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    extra_action_out_fn=add_values,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=choose_optimizer,
    before_init=setup_early_mixins,
    after_init=setup_late_mixins,
    make_model=build_appo_model,
    mixins=[
        LearningRateSchedule, KLCoeffMixin, TargetNetworkMixin,
        ValueNetworkMixin
    ],
    get_batch_divisibility_req=lambda p: p.config["rollout_fragment_length"])
