"""Adapted from VTracePolicyGraph to use the PPO surrogate loss.

Keep in sync with changes to VTracePolicyGraph."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import logging
import gym

import ray
from ray.rllib.agents.impala import vtrace
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph, \
    LearningRateSchedule
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.models.action_dist import MultiCategorical
from ray.rllib.evaluation.postprocessing import compute_advantages

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
                 clip_param=0.3):

        logp_ratio = tf.exp(actions_logp - prev_actions_logp)

        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))

        self.mean_kl = tf.reduce_mean(action_kl)
        self.pi_loss = -tf.reduce_sum(surrogate_loss)

        # The baseline loss
        delta = tf.boolean_mask(values - value_targets, valid_mask)
        self.value_targets = value_targets
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_sum(
            tf.boolean_mask(actions_entropy, valid_mask))

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)


class VTraceSurrogateLoss(object):
    def __init__(self,
                 actions,
                 prev_actions_logp,
                 actions_logp,
                 action_kl,
                 actions_entropy,
                 dones,
                 behaviour_logits,
                 target_logits,
                 discount,
                 rewards,
                 values,
                 bootstrap_value,
                 valid_mask,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01,
                 clip_rho_threshold=1.0,
                 clip_pg_rho_threshold=1.0,
                 clip_param=0.3):
        """PPO surrogate loss with vtrace importance weighting.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Arguments:
            actions: An int32 tensor of shape [T, B, NUM_ACTIONS].
            prev_actions_logp: A float32 tensor of shape [T, B].
            actions_logp: A float32 tensor of shape [T, B].
            action_kl: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behaviour_logits: A float32 tensor of shape [T, B, NUM_ACTIONS].
            target_logits: A float32 tensor of shape [T, B, NUM_ACTIONS].
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
            valid_mask: A bool tensor of valid RNN input elements (#2992).
        """

        # Compute vtrace on the CPU for better perf.
        with tf.device("/cpu:0"):
            self.vtrace_returns = vtrace.multi_from_logits(
                behaviour_policy_logits=behaviour_logits,
                target_policy_logits=target_logits,
                actions=tf.unstack(tf.cast(actions, tf.int32), axis=2),
                discounts=tf.to_float(~dones) * discount,
                rewards=rewards,
                values=values,
                bootstrap_value=bootstrap_value,
                clip_rho_threshold=tf.cast(clip_rho_threshold, tf.float32),
                clip_pg_rho_threshold=tf.cast(clip_pg_rho_threshold,
                                              tf.float32))

        logp_ratio = tf.exp(actions_logp - prev_actions_logp)

        advantages = self.vtrace_returns.pg_advantages
        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))

        self.mean_kl = tf.reduce_mean(action_kl)
        self.pi_loss = -tf.reduce_sum(surrogate_loss)

        # The baseline loss
        delta = tf.boolean_mask(values - self.vtrace_returns.vs, valid_mask)
        self.value_targets = self.vtrace_returns.vs
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_sum(
            tf.boolean_mask(actions_entropy, valid_mask))

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)


class AsyncPPOPolicyGraph(LearningRateSchedule, TFPolicyGraph):
    def __init__(self,
                 observation_space,
                 action_space,
                 config,
                 existing_inputs=None):
        config = dict(ray.rllib.agents.impala.impala.DEFAULT_CONFIG, **config)
        assert config["batch_mode"] == "truncate_episodes", \
            "Must use `truncate_episodes` batch mode with V-trace."
        self.config = config
        self.sess = tf.get_default_session()
        self.grads = None

        if isinstance(action_space, gym.spaces.Discrete):
            is_multidiscrete = False
            output_hidden_shape = [action_space.n]
        elif isinstance(action_space, gym.spaces.multi_discrete.MultiDiscrete):
            is_multidiscrete = True
            output_hidden_shape = action_space.nvec.astype(np.int32)
        elif self.config["vtrace"]:
            raise UnsupportedSpaceException(
                "Action space {} is not supported for APPO + VTrace.",
                format(action_space))
        else:
            is_multidiscrete = False
            output_hidden_shape = 1

        # Policy network model
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])

        # Create input placeholders
        if existing_inputs:
            if self.config["vtrace"]:
                actions, dones, behaviour_logits, rewards, observations, \
                    prev_actions, prev_rewards = existing_inputs[:7]
                existing_state_in = existing_inputs[7:-1]
                existing_seq_lens = existing_inputs[-1]
            else:
                actions, dones, behaviour_logits, rewards, observations, \
                    prev_actions, prev_rewards, adv_ph, value_targets = \
                    existing_inputs[:9]
                existing_state_in = existing_inputs[9:-1]
                existing_seq_lens = existing_inputs[-1]
        else:
            actions = ModelCatalog.get_action_placeholder(action_space)
            dones = tf.placeholder(tf.bool, [None], name="dones")
            rewards = tf.placeholder(tf.float32, [None], name="rewards")
            behaviour_logits = tf.placeholder(
                tf.float32, [None, logit_dim], name="behaviour_logits")
            observations = tf.placeholder(
                tf.float32, [None] + list(observation_space.shape))
            existing_state_in = None
            existing_seq_lens = None

            if not self.config["vtrace"]:
                adv_ph = tf.placeholder(
                    tf.float32, name="advantages", shape=(None, ))
                value_targets = tf.placeholder(
                    tf.float32, name="value_targets", shape=(None, ))
        self.observations = observations

        # Unpack behaviour logits
        unpacked_behaviour_logits = tf.split(
            behaviour_logits, output_hidden_shape, axis=1)

        # Setup the policy
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        prev_actions = ModelCatalog.get_action_placeholder(action_space)
        prev_rewards = tf.placeholder(tf.float32, [None], name="prev_reward")
        self.model = ModelCatalog.get_model(
            {
                "obs": observations,
                "prev_actions": prev_actions,
                "prev_rewards": prev_rewards,
                "is_training": self._get_is_training_placeholder(),
            },
            observation_space,
            action_space,
            logit_dim,
            self.config["model"],
            state_in=existing_state_in,
            seq_lens=existing_seq_lens)
        unpacked_outputs = tf.split(
            self.model.outputs, output_hidden_shape, axis=1)

        dist_inputs = unpacked_outputs if is_multidiscrete else \
            self.model.outputs
        prev_dist_inputs = unpacked_behaviour_logits if is_multidiscrete else \
            behaviour_logits

        action_dist = dist_class(dist_inputs)
        prev_action_dist = dist_class(prev_dist_inputs)

        values = self.model.value_function()
        self.value_function = values
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

        def make_time_major(tensor, drop_last=False):
            """Swaps batch and trajectory axis.
            Args:
                tensor: A tensor or list of tensors to reshape.
                drop_last: A bool indicating whether to drop the last
                trajectory item.
            Returns:
                res: A tensor with swapped axes or a list of tensors with
                swapped axes.
            """
            if isinstance(tensor, list):
                return [make_time_major(t, drop_last) for t in tensor]

            if self.model.state_init:
                B = tf.shape(self.model.seq_lens)[0]
                T = tf.shape(tensor)[0] // B
            else:
                # Important: chop the tensor into batches at known episode cut
                # boundaries. TODO(ekl) this is kind of a hack
                T = self.config["sample_batch_size"]
                B = tf.shape(tensor)[0] // T
            rs = tf.reshape(tensor,
                            tf.concat([[B, T], tf.shape(tensor)[1:]], axis=0))

            # swap B and T axes
            res = tf.transpose(
                rs,
                [1, 0] + list(range(2, 1 + int(tf.shape(tensor).shape[0]))))

            if drop_last:
                return res[:-1]
            return res

        if self.model.state_in:
            max_seq_len = tf.reduce_max(self.model.seq_lens) - 1
            mask = tf.sequence_mask(self.model.seq_lens, max_seq_len)
            mask = tf.reshape(mask, [-1])
        else:
            mask = tf.ones_like(rewards)

        # Inputs are reshaped from [B * T] => [T - 1, B] for V-trace calc.
        if self.config["vtrace"]:
            logger.info("Using V-Trace surrogate loss (vtrace=True)")

            # Prepare actions for loss
            loss_actions = actions if is_multidiscrete else tf.expand_dims(
                actions, axis=1)

            self.loss = VTraceSurrogateLoss(
                actions=make_time_major(loss_actions, drop_last=True),
                prev_actions_logp=make_time_major(
                    prev_action_dist.logp(actions), drop_last=True),
                actions_logp=make_time_major(
                    action_dist.logp(actions), drop_last=True),
                action_kl=prev_action_dist.kl(action_dist),
                actions_entropy=make_time_major(
                    action_dist.entropy(), drop_last=True),
                dones=make_time_major(dones, drop_last=True),
                behaviour_logits=make_time_major(
                    unpacked_behaviour_logits, drop_last=True),
                target_logits=make_time_major(
                    unpacked_outputs, drop_last=True),
                discount=config["gamma"],
                rewards=make_time_major(rewards, drop_last=True),
                values=make_time_major(values, drop_last=True),
                bootstrap_value=make_time_major(values)[-1],
                valid_mask=make_time_major(mask, drop_last=True),
                vf_loss_coeff=self.config["vf_loss_coeff"],
                entropy_coeff=self.config["entropy_coeff"],
                clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
                clip_pg_rho_threshold=self.config[
                    "vtrace_clip_pg_rho_threshold"],
                clip_param=self.config["clip_param"])
        else:
            logger.info("Using PPO surrogate loss (vtrace=False)")
            self.loss = PPOSurrogateLoss(
                prev_actions_logp=make_time_major(
                    prev_action_dist.logp(actions)),
                actions_logp=make_time_major(action_dist.logp(actions)),
                action_kl=prev_action_dist.kl(action_dist),
                actions_entropy=make_time_major(action_dist.entropy()),
                values=make_time_major(values),
                valid_mask=make_time_major(mask),
                advantages=make_time_major(adv_ph),
                value_targets=make_time_major(value_targets),
                vf_loss_coeff=self.config["vf_loss_coeff"],
                entropy_coeff=self.config["entropy_coeff"],
                clip_param=self.config["clip_param"])

        # KL divergence between worker and learner logits for debugging
        model_dist = MultiCategorical(unpacked_outputs)
        behaviour_dist = MultiCategorical(unpacked_behaviour_logits)

        kls = model_dist.kl(behaviour_dist)
        if len(kls) > 1:
            self.KL_stats = {}

            for i, kl in enumerate(kls):
                self.KL_stats.update({
                    "mean_KL_{}".format(i): tf.reduce_mean(kl),
                    "max_KL_{}".format(i): tf.reduce_max(kl),
                    "median_KL_{}".format(i): tf.contrib.distributions.
                    percentile(kl, 50.0),
                })
        else:
            self.KL_stats = {
                "mean_KL": tf.reduce_mean(kls[0]),
                "max_KL": tf.reduce_max(kls[0]),
                "median_KL": tf.contrib.distributions.percentile(kls[0], 50.0),
            }

        # Initialize TFPolicyGraph
        loss_in = [
            ("actions", actions),
            ("dones", dones),
            ("behaviour_logits", behaviour_logits),
            ("rewards", rewards),
            ("obs", observations),
            ("prev_actions", prev_actions),
            ("prev_rewards", prev_rewards),
        ]
        if not self.config["vtrace"]:
            loss_in.append(("advantages", adv_ph))
            loss_in.append(("value_targets", value_targets))
        LearningRateSchedule.__init__(self, self.config["lr"],
                                      self.config["lr_schedule"])
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=observations,
            action_sampler=action_dist.sample(),
            action_prob=action_dist.sampled_action_prob(),
            loss=self.loss.total_loss,
            model=self.model,
            loss_inputs=loss_in,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self.model.seq_lens,
            max_seq_len=self.config["model"]["max_seq_len"],
            batch_divisibility_req=self.config["sample_batch_size"])

        self.sess.run(tf.global_variables_initializer())

        values_batched = make_time_major(
            values, drop_last=self.config["vtrace"])
        self.stats_fetches = {
            "stats": dict({
                "cur_lr": tf.cast(self.cur_lr, tf.float64),
                "policy_loss": self.loss.pi_loss,
                "entropy": self.loss.entropy,
                "grad_gnorm": tf.global_norm(self._grads),
                "var_gnorm": tf.global_norm(self.var_list),
                "vf_loss": self.loss.vf_loss,
                "vf_explained_var": explained_variance(
                    tf.reshape(self.loss.value_targets, [-1]),
                    tf.reshape(values_batched, [-1])),
            }, **self.KL_stats),
        }

    def optimizer(self):
        if self.config["opt_type"] == "adam":
            return tf.train.AdamOptimizer(self.cur_lr)
        else:
            return tf.train.RMSPropOptimizer(self.cur_lr, self.config["decay"],
                                             self.config["momentum"],
                                             self.config["epsilon"])

    def gradients(self, optimizer, loss):
        grads = tf.gradients(loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    def extra_compute_action_fetches(self):
        out = {"behaviour_logits": self.model.outputs}
        if not self.config["vtrace"]:
            out["vf_preds"] = self.value_function
        return dict(TFPolicyGraph.extra_compute_action_fetches(self), **out)

    def extra_compute_grad_fetches(self):
        return self.stats_fetches

    def value(self, ob, *args):
        feed_dict = {self.observations: [ob], self.model.seq_lens: [1]}
        assert len(args) == len(self.model.state_in), \
            (args, self.model.state_in)
        for k, v in zip(self.model.state_in, args):
            feed_dict[k] = v
        vf = self.sess.run(self.value_function, feed_dict)
        return vf[0]

    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        if not self.config["vtrace"]:
            completed = sample_batch["dones"][-1]
            if completed:
                last_r = 0.0
            else:
                next_state = []
                for i in range(len(self.model.state_in)):
                    next_state.append(
                        [sample_batch["state_out_{}".format(i)][-1]])
                last_r = self.value(sample_batch["new_obs"][-1], *next_state)
            batch = compute_advantages(
                sample_batch,
                last_r,
                self.config["gamma"],
                self.config["lambda"],
                use_gae=self.config["use_gae"])
        else:
            batch = sample_batch
        del batch.data["new_obs"]  # not used, so save some bandwidth
        return batch

    def get_initial_state(self):
        return self.model.state_init

    def copy(self, existing_inputs):
        return AsyncPPOPolicyGraph(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=existing_inputs)
