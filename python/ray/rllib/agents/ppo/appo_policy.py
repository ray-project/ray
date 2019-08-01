"""Adapted from VTraceTFPolicy to use the PPO surrogate loss.

Keep in sync with changes to VTraceTFPolicy."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging
import gym

from ray.rllib.agents.impala import vtrace
from ray.rllib.agents.impala.vtrace_policy import _make_time_major, \
        BEHAVIOUR_LOGITS, clip_gradients, \
        validate_config, choose_optimizer, ValueNetworkMixin
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.utils import try_import_tf
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_policy import LearningRateSchedule
from ray.rllib.agents.ppo.ppo_policy import KLCoeffMixin
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.explained_variance import explained_variance

tf = try_import_tf()

POLICY_SCOPE = "func"
TARGET_POLICY_SCOPE = "target_func"

logger = logging.getLogger(__name__)


class PPOSurrogateLoss(object):
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
        def reduce_mean_valid(t):
            return tf.reduce_mean(tf.boolean_mask(t, valid_mask))

        logp_ratio = tf.exp(actions_logp - prev_actions_logp)

        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))

        self.mean_kl = reduce_mean_valid(action_kl)
        self.pi_loss = -reduce_mean_valid(surrogate_loss)

        # The baseline loss
        delta = values - value_targets
        self.value_targets = value_targets
        self.vf_loss = 0.5 * reduce_mean_valid(tf.square(delta))

        # The entropy loss
        self.entropy = reduce_mean_valid(actions_entropy)

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss
        if use_kl_loss:
            self.total_loss += cur_kl_coeff * self.mean_kl


class VTraceSurrogateLoss(object):
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
            valid_mask: A bool tensor of valid RNN input elements (#2992).
            vf_loss_coeff (float): Coefficient of the value function loss.
            entropy_coeff (float): Coefficient of the entropy regularizer.
            clip_param (float): Clip parameter.
            cur_kl_coeff (float): Coefficient for KL loss.
            use_kl_loss (bool): If true, use KL loss.
        """

        def reduce_mean_valid(t):
            return tf.reduce_mean(tf.boolean_mask(t, valid_mask))

        # Compute vtrace on the CPU for better perf.
        with tf.device("/cpu:0"):
            self.vtrace_returns = vtrace.multi_from_logits(
                behaviour_policy_logits=behaviour_logits,
                target_policy_logits=old_policy_behaviour_logits,
                actions=tf.unstack(actions, axis=2),
                discounts=tf.to_float(~dones) * discount,
                rewards=rewards,
                values=values,
                bootstrap_value=bootstrap_value,
                dist_class=dist_class,
                clip_rho_threshold=tf.cast(clip_rho_threshold, tf.float32),
                clip_pg_rho_threshold=tf.cast(clip_pg_rho_threshold,
                                              tf.float32))

        self.is_ratio = tf.clip_by_value(
            tf.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0)
        logp_ratio = self.is_ratio * tf.exp(actions_logp - prev_actions_logp)

        advantages = self.vtrace_returns.pg_advantages
        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))

        self.mean_kl = reduce_mean_valid(action_kl)
        self.pi_loss = -reduce_mean_valid(surrogate_loss)

        # The baseline loss
        delta = values - self.vtrace_returns.vs
        self.value_targets = self.vtrace_returns.vs
        self.vf_loss = 0.5 * reduce_mean_valid(tf.square(delta))

        # The entropy loss
        self.entropy = reduce_mean_valid(actions_entropy)

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss
        if use_kl_loss:
            self.total_loss += cur_kl_coeff * self.mean_kl


def build_appo_model(policy, obs_space, action_space, config):
    policy.model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        policy.logit_dim,
        config["model"],
        name=POLICY_SCOPE,
        framework="tf")

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        policy.logit_dim,
        config["model"],
        name=TARGET_POLICY_SCOPE,
        framework="tf")

    return policy.model


def build_appo_surrogate_loss(policy, batch_tensors):
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

    def make_time_major(*args, **kw):
        return _make_time_major(policy, *args, **kw)

    actions = batch_tensors[SampleBatch.ACTIONS]
    dones = batch_tensors[SampleBatch.DONES]
    rewards = batch_tensors[SampleBatch.REWARDS]

    behaviour_logits = batch_tensors[BEHAVIOUR_LOGITS]

    policy.target_model_out, _ = policy.target_model(
        policy.input_dict, policy.state_in, policy.seq_lens)
    old_policy_behaviour_logits = tf.stop_gradient(policy.target_model_out)

    unpacked_behaviour_logits = tf.split(
        behaviour_logits, output_hidden_shape, axis=1)
    unpacked_old_policy_behaviour_logits = tf.split(
        old_policy_behaviour_logits, output_hidden_shape, axis=1)
    unpacked_outputs = tf.split(policy.model_out, output_hidden_shape, axis=1)
    action_dist = policy.action_dist
    old_policy_action_dist = policy.dist_class(old_policy_behaviour_logits)
    prev_action_dist = policy.dist_class(behaviour_logits)
    values = policy.value_function

    policy.model_vars = policy.model.variables()
    policy.target_model_vars = policy.target_model.variables()

    if policy.state_in:
        max_seq_len = tf.reduce_max(policy.seq_lens) - 1
        mask = tf.sequence_mask(policy.seq_lens, max_seq_len)
        mask = tf.reshape(mask, [-1])
    else:
        mask = tf.ones_like(rewards)

    if policy.config["vtrace"]:
        logger.info("Using V-Trace surrogate loss (vtrace=True)")

        # Prepare actions for loss
        loss_actions = actions if is_multidiscrete else tf.expand_dims(
            actions, axis=1)

        # Prepare KL for Loss
        mean_kl = make_time_major(
            old_policy_action_dist.multi_kl(action_dist), drop_last=True)

        policy.loss = VTraceSurrogateLoss(
            actions=make_time_major(loss_actions, drop_last=True),
            prev_actions_logp=make_time_major(
                prev_action_dist.logp(actions), drop_last=True),
            actions_logp=make_time_major(
                action_dist.logp(actions), drop_last=True),
            old_policy_actions_logp=make_time_major(
                old_policy_action_dist.logp(actions), drop_last=True),
            action_kl=tf.reduce_mean(mean_kl, axis=0)
            if is_multidiscrete else mean_kl,
            actions_entropy=make_time_major(
                action_dist.multi_entropy(), drop_last=True),
            dones=make_time_major(dones, drop_last=True),
            behaviour_logits=make_time_major(
                unpacked_behaviour_logits, drop_last=True),
            old_policy_behaviour_logits=make_time_major(
                unpacked_old_policy_behaviour_logits, drop_last=True),
            target_logits=make_time_major(unpacked_outputs, drop_last=True),
            discount=policy.config["gamma"],
            rewards=make_time_major(rewards, drop_last=True),
            values=make_time_major(values, drop_last=True),
            bootstrap_value=make_time_major(values)[-1],
            dist_class=Categorical if is_multidiscrete else policy.dist_class,
            valid_mask=make_time_major(mask, drop_last=True),
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_rho_threshold=policy.config["vtrace_clip_rho_threshold"],
            clip_pg_rho_threshold=policy.config[
                "vtrace_clip_pg_rho_threshold"],
            clip_param=policy.config["clip_param"],
            cur_kl_coeff=policy.kl_coeff,
            use_kl_loss=policy.config["use_kl_loss"])
    else:
        logger.info("Using PPO surrogate loss (vtrace=False)")

        # Prepare KL for Loss
        mean_kl = make_time_major(prev_action_dist.multi_kl(action_dist))

        policy.loss = PPOSurrogateLoss(
            prev_actions_logp=make_time_major(prev_action_dist.logp(actions)),
            actions_logp=make_time_major(action_dist.logp(actions)),
            action_kl=tf.reduce_mean(mean_kl, axis=0)
            if is_multidiscrete else mean_kl,
            actions_entropy=make_time_major(action_dist.multi_entropy()),
            values=make_time_major(values),
            valid_mask=make_time_major(mask),
            advantages=make_time_major(
                batch_tensors[Postprocessing.ADVANTAGES]),
            value_targets=make_time_major(
                batch_tensors[Postprocessing.VALUE_TARGETS]),
            vf_loss_coeff=policy.config["vf_loss_coeff"],
            entropy_coeff=policy.config["entropy_coeff"],
            clip_param=policy.config["clip_param"],
            cur_kl_coeff=policy.kl_coeff,
            use_kl_loss=policy.config["use_kl_loss"])

    return policy.loss.total_loss


def stats(policy, batch_tensors):
    values_batched = _make_time_major(
        policy, policy.value_function, drop_last=policy.config["vtrace"])

    stats_dict = {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "policy_loss": policy.loss.pi_loss,
        "entropy": policy.loss.entropy,
        "var_gnorm": tf.global_norm(policy.var_list),
        "vf_loss": policy.loss.vf_loss,
        "vf_explained_var": explained_variance(
            tf.reshape(policy.loss.value_targets, [-1]),
            tf.reshape(values_batched, [-1])),
    }

    if policy.config["vtrace"]:
        is_stat_mean, is_stat_var = tf.nn.moments(policy.loss.is_ratio, [0, 1])
        stats_dict.update({"mean_IS": is_stat_mean})
        stats_dict.update({"var_IS": is_stat_var})

    if policy.config["use_kl_loss"]:
        stats_dict.update({"kl": policy.loss.mean_kl})
        stats_dict.update({"KL_Coeff": policy.kl_coeff})

    return stats_dict


def postprocess_trajectory(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    if not policy.config["vtrace"]:
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            next_state = []
            for i in range(len(policy.state_in)):
                next_state.append([sample_batch["state_out_{}".format(i)][-1]])
            last_r = policy.value(sample_batch["new_obs"][-1], *next_state)
        batch = compute_advantages(
            sample_batch,
            last_r,
            policy.config["gamma"],
            policy.config["lambda"],
            use_gae=policy.config["use_gae"])
    else:
        batch = sample_batch
    del batch.data["new_obs"]  # not used, so save some bandwidth
    return batch


def add_values_and_logits(policy):
    out = {BEHAVIOUR_LOGITS: policy.model_out}
    if not policy.config["vtrace"]:
        out[SampleBatch.VF_PREDS] = policy.value_function
    return out


class TargetNetworkMixin(object):
    def __init__(self, obs_space, action_space, config):
        """Target Network is updated by the master learner every
        trainer.update_target_frequency steps. All worker batches
        are importance sampled w.r. to the target network to ensure
        a more stable pi_old in PPO.
        """
        assign_ops = []
        assert len(self.model_vars) == len(self.target_model_vars)
        for var, var_target in zip(self.model_vars, self.target_model_vars):
            assign_ops.append(var_target.assign(var))
        self.update_target_network = tf.group(*assign_ops)

    def update_target(self):
        return self.get_session().run(self.update_target_network)


def setup_mixins(policy, obs_space, action_space, config):
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    KLCoeffMixin.__init__(policy, config)
    ValueNetworkMixin.__init__(policy)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


AsyncPPOTFPolicy = build_tf_policy(
    name="AsyncPPOTFPolicy",
    make_model=build_appo_model,
    loss_fn=build_appo_surrogate_loss,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    optimizer_fn=choose_optimizer,
    gradients_fn=clip_gradients,
    extra_action_fetches_fn=add_values_and_logits,
    before_init=validate_config,
    before_loss_init=setup_mixins,
    after_init=setup_late_mixins,
    mixins=[
        LearningRateSchedule, KLCoeffMixin, TargetNetworkMixin,
        ValueNetworkMixin
    ],
    get_batch_divisibility_req=lambda p: p.config["sample_batch_size"])
