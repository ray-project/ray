"""Adapted from VTraceTFPolicy to use the PPO surrogate loss.

Keep in sync with changes to VTraceTFPolicy."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import logging
import gym

import ray
from ray.rllib.agents.impala import vtrace
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.models.action_dist import Categorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_policy import LearningRateSchedule
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)

BEHAVIOUR_LOGITS = "behaviour_logits"

#Classic PPO Loss, no changes made to loss function
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

        logp_ratio = tf.exp(actions_logp - prev_actions_logp)

        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))

        self.mean_kl = tf.reduce_mean(action_kl)
        self.pi_loss = -tf.reduce_mean(surrogate_loss)

        # The baseline loss
        delta = tf.boolean_mask(values - value_targets, valid_mask)
        self.value_targets = value_targets
        self.vf_loss = 0.5 * tf.reduce_mean(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_mean(
            tf.boolean_mask(actions_entropy, valid_mask))

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss (helps with stability in continuous control environments)
        if use_kl_loss:
            self.total_loss+=cur_kl_coeff*self.mean_kl

#APPO Loss, with IS modifications and V-trace for Advantage Estimation
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
        """PPO surrogate loss with vtrace importance weighting.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Arguments:
            actions: An int|float32 tensor of shape [T, B, logit_dim].
            prev_actions_logp: A float32 tensor of shape [T, B].
            actions_logp: A float32 tensor of shape [T, B].
            action_kl: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behaviour_logits: A float32 tensor of shape [T, B, logit_dim].
            target_logits: A float32 tensor of shape [T, B, logit_dim].
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
            dist_class: action distribution class for logits.
            valid_mask: A bool tensor of valid RNN input elements (#2992).
        """

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

        self.is_ratio = tf.clip_by_value(tf.exp(prev_actions_logp-old_policy_actions_logp), 0.0, 2.0)
        logp_ratio = self.is_ratio*tf.exp(actions_logp - prev_actions_logp)

        advantages = self.vtrace_returns.pg_advantages
        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))

        self.mean_kl = tf.reduce_mean(action_kl)
        self.pi_loss = -tf.reduce_mean(surrogate_loss)

        # The baseline loss
        delta = tf.boolean_mask(values - self.vtrace_returns.vs, valid_mask)
        self.value_targets = self.vtrace_returns.vs
        self.vf_loss = 0.5 * tf.reduce_mean(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_mean(
            tf.boolean_mask(actions_entropy, valid_mask))

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)

        # Optional additional KL Loss (helps with stability in continuous control environments)
        if use_kl_loss:
            self.total_loss+=cur_kl_coeff*self.mean_kl


def _make_time_major(policy, tensor, drop_last=False):
    """Swaps batch and trajectory axis.

    Arguments:
        policy: Policy reference
        tensor: A tensor or list of tensors to reshape.
        drop_last: A bool indicating whether to drop the last
        trajectory item.

    Returns:
        res: A tensor with swapped axes or a list of tensors with
        swapped axes.
    """
    if isinstance(tensor, list):
        return [_make_time_major(policy, t, drop_last) for t in tensor]

    if policy.model.state_init:
        B = tf.shape(policy.model.seq_lens)[0]
        T = tf.shape(tensor)[0] // B
    else:
        # Important: chop the tensor into batches at known episode cut
        # boundaries. TODO(ekl) this is kind of a hack
        T = policy.config["sample_batch_size"]
        B = tf.shape(tensor)[0] // T
    rs = tf.reshape(tensor, tf.concat([[B, T], tf.shape(tensor)[1:]], axis=0))

    # swap B and T axes
    res = tf.transpose(
        rs, [1, 0] + list(range(2, 1 + int(tf.shape(tensor).shape[0]))))

    if drop_last:
        return res[:-1]
    return res


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
    old_policy_behaviour_logits = batch_tensors["old_policy_behaviour_logits"]
    
    unpacked_behaviour_logits = tf.split(
        behaviour_logits, output_hidden_shape, axis=1)
    unpacked_old_policy_behaviour_logits = tf.split(
            old_policy_behaviour_logits, output_hidden_shape, axis=1)
    unpacked_outputs = tf.split(
        policy.model.outputs, output_hidden_shape, axis=1)
    
    action_dist = policy.action_dist
    old_policy_action_dist = policy.dist_class(old_policy_behaviour_logits)
    prev_action_dist = policy.dist_class(behaviour_logits)
    
    values = policy.value_function
    if policy.model.state_in:
        max_seq_len = tf.reduce_max(policy.model.seq_lens) - 1
        mask = tf.sequence_mask(policy.model.seq_lens, max_seq_len)
        mask = tf.reshape(mask, [-1])
    else:
        mask = tf.ones_like(rewards)

    if policy.config["vtrace"]:
        logger.info("Using V-Trace surrogate loss (vtrace=True)")

        # Prepare actions for loss
        loss_actions = actions if is_multidiscrete else tf.expand_dims(
            actions, axis=1)

        policy.loss = VTraceSurrogateLoss(
            actions=make_time_major(loss_actions, drop_last=True),
            prev_actions_logp=make_time_major(
                prev_action_dist.logp(actions), drop_last=True),
            actions_logp=make_time_major(
                action_dist.logp(actions), drop_last=True),
            old_policy_actions_logp=make_time_major(
                old_policy_action_dist.logp(actions), drop_last=True),
            action_kl=old_policy_action_dist.kl(action_dist),
            actions_entropy=make_time_major(
                action_dist.entropy(), drop_last=True),
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
        policy.loss = PPOSurrogateLoss(
            prev_actions_logp=make_time_major(prev_action_dist.logp(actions)),
            actions_logp=make_time_major(action_dist.logp(actions)),
            action_kl=prev_action_dist.kl(action_dist),
            actions_entropy=make_time_major(action_dist.entropy()),
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
        is_stat_mean, is_stat_var = tf.nn.moments(policy.loss.is_ratio, [0,1])
        stats_dict.update({"mean_IS": is_stat_mean})
        stats_dict.update({"var_IS": is_stat_var})

    if policy.config["use_kl_loss"]:
        stats_dict.update({"KL": policy.loss.mean_kl})
        stats_dict.update({"KL_Coeff": policy.kl_coeff})

    return stats_dict


def grad_stats(policy, grads):
    return {
        "grad_gnorm": tf.global_norm(grads),
    }


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
            for i in range(len(policy.model.state_in)):
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
    out = {BEHAVIOUR_LOGITS: policy.model.outputs}
    if not policy.config["vtrace"]:
        out[SampleBatch.VF_PREDS] = policy.value_function
    return out


def validate_config(policy, obs_space, action_space, config):
    assert config["batch_mode"] == "truncate_episodes", \
        "Must use `truncate_episodes` batch mode with V-trace."


def choose_optimizer(policy, config):
    if policy.config["opt_type"] == "adam":
        return tf.train.AdamOptimizer(policy.cur_lr)
    else:
        return tf.train.RMSPropOptimizer(policy.cur_lr, config["decay"],
                                         config["momentum"], config["epsilon"])


def clip_gradients(policy, optimizer, loss):
    grads = tf.gradients(loss, policy.var_list)
    policy.grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
    clipped_grads = list(zip(policy.grads, policy.var_list))
    return clipped_grads

class KLCoeffMixin(object):
    def __init__(self, config):
        # KL Coefficient
        self.kl_coeff_val = config["kl_coeff"]
        self.kl_target = config["kl_target"]
        self.kl_coeff = tf.get_variable(
            initializer=tf.constant_initializer(self.kl_coeff_val),
            name="kl_coeff",
            shape=(),
            trainable=False,
            dtype=tf.float32)

    def update_kl(self, sampled_kl):
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff_val *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5
        self.kl_coeff.load(self.kl_coeff_val, session=self.get_session())
        return self.kl_coeff_val

class ValueNetworkMixin(object):
    def __init__(self):
        self.value_function = self.model.value_function()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

    def value(self, ob, *args):
        feed_dict = {
            self.get_placeholder(SampleBatch.CUR_OBS): [ob],
            self.model.seq_lens: [1]
        }
        assert len(args) == len(self.model.state_in), \
            (args, self.model.state_in)
        for k, v in zip(self.model.state_in, args):
            feed_dict[k] = v
        vf = self.get_session().run(self.value_function, feed_dict)
        return vf[0]


def setup_mixins(policy, obs_space, action_space, config):
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    if config["use_kl_loss"]:
        KLCoeffMixin.__init__(policy, config)
    ValueNetworkMixin.__init__(policy)


AsyncPPOTFPolicy = build_tf_policy(
    name="AsyncPPOTFPolicy",
    get_default_config=lambda: ray.rllib.agents.impala.impala.DEFAULT_CONFIG,
    loss_fn=build_appo_surrogate_loss,
    stats_fn=stats,
    grad_stats_fn=grad_stats,
    postprocess_fn=postprocess_trajectory,
    optimizer_fn=choose_optimizer,
    gradients_fn=clip_gradients,
    extra_action_fetches_fn=add_values_and_logits,
    before_init=validate_config,
    before_loss_init=setup_mixins,
    mixins=[LearningRateSchedule, KLCoeffMixin, ValueNetworkMixin],
    get_batch_divisibility_req=lambda p: p.config["sample_batch_size"])
