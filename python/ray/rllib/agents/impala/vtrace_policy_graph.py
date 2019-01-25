"""Adapted from A3CPolicyGraph to add V-trace.

Keep in sync with changes to A3CPolicyGraph and VtraceSurrogatePolicyGraph."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import gym

import ray
from ray.rllib.agents.impala import vtrace
from ray.rllib.agents.impala import multi_vtrace
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph, \
    LearningRateSchedule
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.models.action_dist import MultiCategorical


class VTraceLoss(object):
    def __init__(self,
                 actions,
                 actions_logp,
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
                 clip_pg_rho_threshold=1.0):
        """Policy gradient loss with vtrace importance weighting.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Args:
            actions: An int32 tensor of shape [T, B].
            actions_logp: A float32 tensor of shape [T, B].
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

        # The policy gradients loss
        self.pi_loss = -tf.reduce_sum(
            tf.boolean_mask(actions_logp * self.vtrace_returns.pg_advantages,
                            valid_mask))

        # The baseline loss
        delta = tf.boolean_mask(values - self.vtrace_returns.vs, valid_mask)
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_sum(
            tf.boolean_mask(actions_entropy, valid_mask))

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff +
                           self.entropy * entropy_coeff)


class MultiVTraceLoss(object):
    def __init__(self,
                 actions,
                 actions_logp,
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
                 clip_pg_rho_threshold=1.0):
        """Policy gradient loss with vtrace importance weighting.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Args:
            actions: An int32 tensor of shape [T, B, ACTION_SPACE].
            actions_logp: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behaviour_logits: A list with length of ACTION_SPACE of float32 tensors of shapes
                [T, B, OUTPUT_HIDDEN_SHAPE[0]],
                ...,
                [T, B, OUTPUT_HIDDEN_SHAPE[-1]]
            target_logits: A list with length of ACTION_SPACE of float32 tensors of shapes
                [T, B, OUTPUT_HIDDEN_SHAPE[0]],
                ...,
                [T, B, OUTPUT_HIDDEN_SHAPE[-1]]
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
            valid_mask: A bool tensor of valid RNN input elements (#2992).
        """
        # Compute vtrace on the CPU for better perf.
        with tf.device("/cpu:0"):
            self.vtrace_returns = multi_vtrace.get_vtrace(
                behaviour_policy=behaviour_logits,
                target_policy=target_logits,
                actions=tf.unstack(tf.cast(actions, tf.int32), axis=2),
                discounts=tf.to_float(~dones) * discount,
                rewards=rewards,
                values=values,
                bootstrap_value=bootstrap_value,
                clip_rho_threshold=tf.cast(clip_rho_threshold, tf.float32),
                clip_pg_rho_threshold=tf.cast(clip_pg_rho_threshold,
                                              tf.float32))

        # The policy gradients loss
        self.pi_loss = -tf.reduce_sum(
            tf.boolean_mask(actions_logp * self.vtrace_returns.pg_advantages,
                            valid_mask))

        # The baseline loss
        delta = tf.boolean_mask(values - self.vtrace_returns.vs, valid_mask)
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_sum(
            tf.boolean_mask(actions_entropy, valid_mask))

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff +
                           self.entropy * entropy_coeff)


class VTracePolicyGraph(LearningRateSchedule, TFPolicyGraph):
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

        # Create input placeholders
        if existing_inputs:
            actions, dones, behaviour_logits, rewards, observations, \
                prev_actions, prev_rewards = existing_inputs[:7]
            existing_state_in = existing_inputs[7:-1]
            existing_seq_lens = existing_inputs[-1]
        else:
            self.output_hidden_shape = \
                config["output_hidden_shape"] or [action_space.n]
            logit_dim = sum(self.output_hidden_shape)
            if isinstance(action_space, gym.spaces.Discrete):
                actions = tf.placeholder(tf.int64, [None], name="ac")
            elif isinstance(action_space, gym.spaces.Box):
                actions = tf.placeholder(
                    tf.int64, [None, *action_space.shape], name="ac")
            else:
                raise UnsupportedSpaceException(
                    "Action space {} is not supported for IMPALA.".format(
                        action_space))
            dones = tf.placeholder(tf.bool, [None], name="dones")
            rewards = tf.placeholder(tf.float32, [None], name="rewards")
            behaviour_logits = tf.placeholder(tf.float32,
                                              [None, logit_dim],
                                              name="behaviour_logits")

            unpacked_behaviour_logits = tf.split(
                behaviour_logits, self.output_hidden_shape, axis=1)
            observations = tf.placeholder(
                tf.float32, [None] + list(observation_space.shape))
            existing_state_in = None
            existing_seq_lens = None

        # Setup the policy
        dist_class, _ = ModelCatalog.get_action_dist(
            action_space, self.config["model"],
            dist_type=self.config["dist_type"])
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
            logit_dim,
            self.config["model"],
            state_in=existing_state_in,
            seq_lens=existing_seq_lens)
        unpacked_outputs = tf.split(
            self.model.outputs, self.output_hidden_shape, axis=1)
        action_dist = dist_class(unpacked_outputs)
        values = self.model.value_function()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

        def make_time_major(tensor, drop_last=False):
            if isinstance(tensor, list):
                return [make_time_major(t, drop_last) for t in tensor]

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
            mask = tf.ones_like(rewards, dtype=tf.bool)

        # Inputs are reshaped from [B * T] => [T - 1, B] for V-trace calc.
        self.loss = MultiVTraceLoss(
            actions=make_time_major(actions, True),
            actions_logp=make_time_major(action_dist.logp(
                tf.unstack(actions, axis=1)), True),
            actions_entropy=make_time_major(action_dist.entropy(), True),
            dones=make_time_major(dones, True),
            behaviour_logits=make_time_major(unpacked_behaviour_logits, True),
            target_logits=make_time_major(unpacked_outputs, True),
            discount=config["gamma"],
            rewards=make_time_major(rewards, True),
            values=make_time_major(values, True),
            bootstrap_value=make_time_major(values)[-1],
            valid_mask=make_time_major(mask, True),
            vf_loss_coeff=self.config["vf_loss_coeff"],
            entropy_coeff=self.config["entropy_coeff"],
            clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
            clip_pg_rho_threshold=self.config["vtrace_clip_pg_rho_threshold"])

        # KL divergence between worker and learner logits for debugging
        model_dist = MultiCategorical(unpacked_outputs)
        behaviour_dist = MultiCategorical(unpacked_behaviour_logits)

        KLs = model_dist.kl(behaviour_dist)
        if isinstance(KLs, list):
            self.KL_stats = {}

            for i, kl in enumerate(KLs):
                self.KL_stats.update({
                    f"mean_KL_{i}": tf.reduce_mean(kl),
                    f"max_KL_{i}": tf.reduce_max(kl),
                    f"median_KL_{i}": tf.contrib.distributions.percentile(
                        kl, 50.0),
                })
        else:
            self.KL_stats = {
                "mean_KL": tf.reduce_mean(kl),
                "max_KL": tf.reduce_max(kl),
                "median_KL": tf.contrib.distributions.percentile(
                    kl, 50.0),
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

        self.stats_fetches = {
            "stats": {
                "cur_lr": tf.cast(self.cur_lr, tf.float64),
                "policy_loss": self.loss.pi_loss,
                "entropy": self.loss.entropy,
                "grad_gnorm": tf.global_norm(self._grads),
                "var_gnorm": tf.global_norm(self.var_list),
                "vf_loss": self.loss.vf_loss,
                "vf_explained_var": explained_variance(
                    tf.reshape(self.loss.vtrace_returns.vs, [-1]),
                    tf.reshape(make_time_major(values, True), [-1])),
                **self.KL_stats,
            },
        }

    @override(TFPolicyGraph)
    def copy(self, existing_inputs):
        return VTracePolicyGraph(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=existing_inputs)

    @override(TFPolicyGraph)
    def optimizer(self):
        if self.config["opt_type"] == "adam":
            return tf.train.AdamOptimizer(self.cur_lr)
        else:
            return tf.train.RMSPropOptimizer(self.cur_lr, self.config["decay"],
                                             self.config["momentum"],
                                             self.config["epsilon"])

    @override(TFPolicyGraph)
    def gradients(self, optimizer):
        grads = tf.gradients(self._loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    @override(TFPolicyGraph)
    def extra_compute_action_fetches(self):
        return {"behaviour_logits": self.model.outputs}

    @override(TFPolicyGraph)
    def extra_compute_grad_fetches(self):
        return self.stats_fetches

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        del sample_batch.data["new_obs"]  # not used, so save some bandwidth
        return sample_batch

    @override(PolicyGraph)
    def get_initial_state(self):
        return self.model.state_init
