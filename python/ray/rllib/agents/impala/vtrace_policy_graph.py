"""Adapted from A3CPolicyGraph to add V-trace.

Keep in sync with changes to A3CPolicyGraph."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import gym

import ray
from ray.rllib.agents.impala import vtrace
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.utils.error import UnsupportedSpaceException


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
                 vf_loss_coeff=0.5,
                 entropy_coeff=-0.01,
                 clip_rho_threshold=1.0,
                 clip_pg_rho_threshold=1.0):
        """Policy gradient loss with vtrace importance weighting.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Args:
            actions: An int32 tensor of shape [T, B, NUM_ACTIONS].
            actions_logp: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behaviour_logits: A float32 tensor of shape [T, B, NUM_ACTIONS].
            target_logits: A float32 tensor of shape [T, B, NUM_ACTIONS].
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
        """

        # Compute vtrace on the CPU for better perf.
        with tf.device("/cpu:0"):
            vtrace_returns = vtrace.from_logits(
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
            actions_logp * vtrace_returns.pg_advantages)

        # The baseline loss
        delta = values - vtrace_returns.vs
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_sum(actions_entropy)

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff +
                           self.entropy * entropy_coeff)


class VTracePolicyGraph(TFPolicyGraph):
    def __init__(self,
                 observation_space,
                 action_space,
                 config,
                 existing_inputs=None):

        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        assert config["batch_mode"] == "truncate_episodes", \
            "Must use `truncate_episodes` batch mode with V-trace."
        self.config = config
        self.sess = tf.get_default_session()

        # Create input placeholders
        if existing_inputs:
            actions, dones, behaviour_logits, rewards, observations = \
                existing_inputs[:5]
            existing_state_in = existing_inputs[5:-1]
            existing_seq_lens = existing_inputs[-1]
        else:
            if isinstance(action_space, gym.spaces.Box):
                ac_size = action_space.shape[0]
                actions = tf.placeholder(
                    tf.float32, [None, ac_size], name="ac")
            elif isinstance(action_space, gym.spaces.Discrete):
                ac_size = action_space.n
                actions = tf.placeholder(tf.int64, [None], name="ac")
            else:
                raise UnsupportedSpaceException(
                    "Action space {} is not supported for IMPALA.".format(
                        action_space))
            dones = tf.placeholder(tf.bool, [None], name="dones")
            rewards = tf.placeholder(tf.float32, [None], name="rewards")
            behaviour_logits = tf.placeholder(
                tf.float32, [None, ac_size], name="behaviour_logits")
            observations = tf.placeholder(
                tf.float32, [None] + list(observation_space.shape))
            existing_state_in = None
            existing_seq_lens = None

        # Setup the policy
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_model(
            observations,
            logit_dim,
            self.config["model"],
            state_in=existing_state_in,
            seq_lens=existing_seq_lens)
        action_dist = dist_class(self.model.outputs)
        values = tf.reshape(
            linear(self.model.last_layer, 1, "value", normc_initializer(1.0)),
            [-1])
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

        def to_batches(tensor):
            if self.config["model"]["use_lstm"]:
                B = tf.shape(self.model.seq_lens)[0]
                T = tf.shape(tensor)[0] // B
            else:
                # Important: chop the tensor into batches at known episode cut
                # boundaries. TODO(ekl) this is kind of a hack
                T = (self.config["sample_batch_size"] //
                     self.config["num_envs_per_worker"])
                B = tf.shape(tensor)[0] // T
            rs = tf.reshape(tensor,
                            tf.concat([[B, T], tf.shape(tensor)[1:]], axis=0))
            # swap B and T axes
            return tf.transpose(
                rs,
                [1, 0] + list(range(2, 1 + int(tf.shape(tensor).shape[0]))))

        if self.config["clip_rewards"]:
            clipped_rewards = tf.clip_by_value(rewards, -1, 1)
        else:
            clipped_rewards = rewards

        # Inputs are reshaped from [B * T] => [T - 1, B] for V-trace calc.
        self.loss = VTraceLoss(
            actions=to_batches(actions)[:-1],
            actions_logp=to_batches(action_dist.logp(actions))[:-1],
            actions_entropy=to_batches(action_dist.entropy())[:-1],
            dones=to_batches(dones)[:-1],
            behaviour_logits=to_batches(behaviour_logits)[:-1],
            target_logits=to_batches(self.model.outputs)[:-1],
            discount=config["gamma"],
            rewards=to_batches(clipped_rewards)[:-1],
            values=to_batches(values)[:-1],
            bootstrap_value=to_batches(values)[-1],
            vf_loss_coeff=self.config["vf_loss_coeff"],
            entropy_coeff=self.config["entropy_coeff"],
            clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
            clip_pg_rho_threshold=self.config["vtrace_clip_pg_rho_threshold"])

        # Initialize TFPolicyGraph
        loss_in = [
            ("actions", actions),
            ("dones", dones),
            ("behaviour_logits", behaviour_logits),
            ("rewards", rewards),
            ("obs", observations),
        ]
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=observations,
            action_sampler=action_dist.sample(),
            loss=self.loss.total_loss,
            loss_inputs=loss_in,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            seq_lens=self.model.seq_lens,
            max_seq_len=self.config["model"]["max_seq_len"])

        self.sess.run(tf.global_variables_initializer())

    def optimizer(self):
        if self.config["opt_type"] == "adam":
            return tf.train.AdamOptimizer(self.config["lr"])
        else:
            return tf.train.RMSPropOptimizer(
                self.config["lr"], self.config["decay"],
                self.config["momentum"], self.config["epsilon"])

    def gradients(self, optimizer):
        grads = tf.gradients(self.loss.total_loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    def extra_compute_action_fetches(self):
        return {"behaviour_logits": self.model.outputs}

    def extra_compute_grad_fetches(self):
        if self.config.get("summarize"):
            return {
                "stats": {
                    "policy_loss": self.loss.pi_loss,
                    "value_loss": self.loss.vf_loss,
                    "entropy": self.loss.entropy,
                    "grad_gnorm": tf.global_norm(self._grads),
                    "var_gnorm": tf.global_norm(self.var_list),
                },
            }
        else:
            return {}

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        del sample_batch.data["new_obs"]  # not used, so save some bandwidth
        return sample_batch

    def get_initial_state(self):
        return self.model.state_init

    def copy(self, existing_inputs):
        return VTracePolicyGraph(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=existing_inputs)
