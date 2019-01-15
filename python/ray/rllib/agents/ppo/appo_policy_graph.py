"""Adapted from VTracePolicyGraph to use the PPO surrogate loss.

Keep in sync with changes to VTracePolicyGraph."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
from ray.rllib.models.action_dist import Categorical
from ray.rllib.evaluation.postprocessing import compute_advantages

logger = logging.getLogger(__name__)


class PPOSurrogateLoss(object):
    """Loss used when V-trace is disabled.

    Arguments:
        prev_actions_logp: A float32 tensor of shape [T, B].
        actions_logp: A float32 tensor of shape [T, B].
        actions_kl: A float32 tensor of shape [T, B].
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
                 entropy_coeff=-0.01,
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
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff +
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
                 entropy_coeff=-0.01,
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
            actions_kl: A float32 tensor of shape [T, B].
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
            self.vtrace_returns = vtrace.from_logits(
                behaviour_policy_logits=behaviour_logits,
                target_policy_logits=target_logits,
                actions=tf.cast(actions, tf.int32),
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
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff +
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
            if (not isinstance(action_space, gym.spaces.Discrete)
                    and self.config["vtrace"]):
                raise UnsupportedSpaceException(
                    "Action space {} is not supported with vtrace.".format(
                        action_space))
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

        # Setup the policy
        prev_actions = ModelCatalog.get_action_placeholder(action_space)
        prev_rewards = tf.placeholder(tf.float32, [None], name="prev_reward")
        self.model = ModelCatalog.get_model(
            {
                "obs": observations,
                "prev_actions": prev_actions,
                "prev_rewards": prev_rewards,
            },
            observation_space,
            logit_dim,
            self.config["model"],
            state_in=existing_state_in,
            seq_lens=existing_seq_lens)

        action_dist = dist_class(self.model.outputs)
        prev_action_dist = dist_class(behaviour_logits)

        values = self.model.value_function()
        self.value_function = values
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

        def to_batches(tensor):
            if self.config["model"]["use_lstm"]:
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
            return tf.transpose(
                rs,
                [1, 0] + list(range(2, 1 + int(tf.shape(tensor).shape[0]))))

        if self.model.state_in:
            max_seq_len = tf.reduce_max(self.model.seq_lens) - 1
            mask = tf.sequence_mask(self.model.seq_lens, max_seq_len)
            mask = tf.reshape(mask, [-1])
        else:
            mask = tf.ones_like(rewards)

        # Inputs are reshaped from [B * T] => [T - 1, B] for V-trace calc.
        if self.config["vtrace"]:
            logger.info("Using V-Trace surrogate loss (vtrace=True)")
            self.loss = VTraceSurrogateLoss(
                actions=to_batches(actions)[:-1],
                prev_actions_logp=to_batches(
                    prev_action_dist.logp(actions))[:-1],
                actions_logp=to_batches(action_dist.logp(actions))[:-1],
                action_kl=prev_action_dist.kl(action_dist),
                actions_entropy=to_batches(action_dist.entropy())[:-1],
                dones=to_batches(dones)[:-1],
                behaviour_logits=to_batches(behaviour_logits)[:-1],
                target_logits=to_batches(self.model.outputs)[:-1],
                discount=config["gamma"],
                rewards=to_batches(rewards)[:-1],
                values=to_batches(values)[:-1],
                bootstrap_value=to_batches(values)[-1],
                valid_mask=to_batches(mask)[:-1],
                vf_loss_coeff=self.config["vf_loss_coeff"],
                entropy_coeff=self.config["entropy_coeff"],
                clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
                clip_pg_rho_threshold=self.config[
                    "vtrace_clip_pg_rho_threshold"],
                clip_param=self.config["clip_param"])
        else:
            logger.info("Using PPO surrogate loss (vtrace=False)")
            self.loss = PPOSurrogateLoss(
                prev_actions_logp=to_batches(prev_action_dist.logp(actions)),
                actions_logp=to_batches(action_dist.logp(actions)),
                action_kl=prev_action_dist.kl(action_dist),
                actions_entropy=to_batches(action_dist.entropy()),
                values=to_batches(values),
                valid_mask=to_batches(mask),
                advantages=to_batches(adv_ph),
                value_targets=to_batches(value_targets),
                vf_loss_coeff=self.config["vf_loss_coeff"],
                entropy_coeff=self.config["entropy_coeff"],
                clip_param=self.config["clip_param"])

        # KL divergence between worker and learner logits for debugging
        model_dist = Categorical(self.model.outputs)
        behaviour_dist = Categorical(behaviour_logits)
        self.KLs = model_dist.kl(behaviour_dist)
        self.mean_KL = tf.reduce_mean(self.KLs)
        self.max_KL = tf.reduce_max(self.KLs)
        self.median_KL = tf.contrib.distributions.percentile(self.KLs, 50.0)
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
            loss=self.model.loss() + self.loss.total_loss,
            loss_inputs=loss_in,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            prev_action_input=prev_actions,
            prev_reward_input=prev_rewards,
            seq_lens=self.model.seq_lens,
            max_seq_len=self.config["model"]["max_seq_len"],
            batch_divisibility_req=self.config["sample_batch_size"])

        self.sess.run(tf.global_variables_initializer())

        if self.config["vtrace"]:
            values_batched = to_batches(values)[:-1]
        else:
            values_batched = to_batches(values)
        self.stats_fetches = {
            "stats": {
                "model_loss": self.model.loss(),
                "cur_lr": tf.cast(self.cur_lr, tf.float64),
                "policy_loss": self.loss.pi_loss,
                "entropy": self.loss.entropy,
                "grad_gnorm": tf.global_norm(self._grads),
                "var_gnorm": tf.global_norm(self.var_list),
                "vf_loss": self.loss.vf_loss,
                "vf_explained_var": explained_variance(
                    tf.reshape(self.loss.value_targets, [-1]),
                    tf.reshape(values_batched, [-1])),
                "mean_KL": self.mean_KL,
                "max_KL": self.max_KL,
                "median_KL": self.median_KL,
            },
        }
        self.stats_fetches["kl"] = self.loss.mean_kl

    def optimizer(self):
        if self.config["opt_type"] == "adam":
            return tf.train.AdamOptimizer(self.cur_lr)
        else:
            return tf.train.RMSPropOptimizer(self.cur_lr, self.config["decay"],
                                             self.config["momentum"],
                                             self.config["epsilon"])

    def gradients(self, optimizer):
        grads = tf.gradients(self.loss.total_loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    def extra_compute_action_fetches(self):
        out = {"behaviour_logits": self.model.outputs}
        if not self.config["vtrace"]:
            out["vf_preds"] = self.value_function
        return out

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
