from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

"""This is an extension of A3CPolicyGraph that uses V-trace for loss calc.

Keep in sync with changes to A3CPolicyGraph."""

import tensorflow as tf
import gym

import ray
import ray.rllib.impala.vtrace
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.models.catalog import ModelCatalog


class VTraceLoss(object):
    def __init__(self,
                 action_dist,
                 actions,
                 dones,
                 behavior_logits,
                 target_logits,
                 discount,
                 rewards,
                 values,
                 vf_loss_coeff=0.5,
                 entropy_coeff=-0.01,
                 clip_rho_threshold=1.0,
                 clip_pg_threshold=1.0):
        """Loss using vtrace.

        Args:
            action_dist: ActionDistribution of the policy.
            actions: An int32 tensor of shape [T, NUM_ACTIONS].
            dones: A bool tensor of shape [T].
            behavior_logits: A float32 tensor of shape [T, NUM_ACTIONS].
            target_logits: A float32 tensor of shape [T, NUM_ACTIONS].
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T].
            values: A float32 tensor of shape [T].
        """

        # Use the last value prediction for bootstrapping only.
        bootstrap_value = values[-1]
        actions = actions[:-1]
        dones = dones[:-1]
        behavior_logits = behavior_logits[:-1]
        target_logits = target_logits[:-1]
        rewards = rewards[:-1]
        values = values[:-1]

        # Compute vtrace returns with B=1. This should be fine since we handle
        # LSTM sequencing elsewhere, so the T dim can span multiple episodes.
        with tf.device("/cpu:0"):
            vtrace_returns = vtrace.from_logits(
                behavior_policy_logits=tf.expand_dims(behavior_logits, 1),
                target_policy_logits=tf.expand_dims(target_logits, 1),
                actions=tf.expand_dims(actions, 1),
                discounts=tf.expand_dims(tf.to_float(~done) * discount, 1),
                rewards=tf.expand_dims(rewards, 1),
                values=tf.expand_dims(values, 1),
                bootstrap_value=tf.expand_dims(bootstrap_value, 0),
                clip_rho_threshold=clip_rho_threshold,
                clip_pg_threshold=clip_pg_threshold)

        # The policy gradients loss
        log_prob = action_dist.logp(actions)
        self.pi_loss = -tf.reduce_sum(log_prob * vtrace_returns.pg_advantages)

        # The baseline loss
        delta = vf - vtrace_returns.vs
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))

        # The entropy loss
        self.entropy = tf.reduce_sum(action_dist.entropy())

        # The summed weighted loss
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff +
                           self.entropy * entropy_coeff)


class VTracePolicyGraph(TFPolicyGraph):

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG, **config)
        self.config = config
        self.sess = tf.get_default_session()

        # Setup the policy
        self.observations = tf.placeholder(
            tf.float32, [None] + list(observation_space.shape))
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])
        self.model = ModelCatalog.get_model(self.observations, logit_dim,
                                            self.config["model"])
        action_dist = dist_class(self.model.outputs)
        values = tf.reshape(
            linear(self.model.last_layer, 1, "value", normc_initializer(1.0)),
            [-1])
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

        # Setup the policy loss
        if isinstance(action_space, gym.spaces.Box):
            ac_size = action_space.shape[0]
            actions = tf.placeholder(tf.float32, [None, ac_size], name="ac")
        elif isinstance(action_space, gym.spaces.Discrete):
            actions = tf.placeholder(tf.int64, [None], name="ac")
        else:
            raise UnsupportedSpaceException(
                "Action space {} is not supported for VTrace.".format(
                    action_space))
        dones = tf.placeholder(tf.bool, [None], name="dones")
        rewards = tf.placeholder(tf.float32, [None], name="rewards")
        v_target = tf.placeholder(tf.float32, [None], name="v_target")
        behavior_logits = tf.placeholder(
            tf.float32, [None, ac_size], name="behavior_logits")

        self.loss = VTraceLoss(
            action_dist=action_dist,
            actions=actions,
            dones=dones,
            behavior_logits=behavior_logits,
            target_logits=self.model.outputs,
            discount=config["gamma"],
            rewards=rewards,
            values=values,
            vf_loss_coeff=self.config["vf_loss_coeff"],
            entropy_coeff=self.config["entropy_coeff"])

        # Initialize TFPolicyGraph
        loss_in = [
            ("actions", actions),
            ("dones", dones),
            ("behavior_logits", behavior_logits),
            ("reward", rewards),
            ("obs", self.observations),
        ]
        self.state_in = self.model.state_in
        self.state_out = self.model.state_out
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=self.observations,
            action_sampler=action_dist.sample(),
            loss=self.loss.total_loss,
            loss_inputs=loss_in,
            state_inputs=self.state_in,
            state_outputs=self.state_out,
            seq_lens=self.model.seq_lens,
            max_seq_len=self.config["model"]["max_seq_len"])

        if self.config.get("summarize"):
            bs = tf.to_float(tf.shape(self.observations)[0])
            tf.summary.scalar("model/policy_graph", self.loss.pi_loss / bs)
            tf.summary.scalar("model/value_loss", self.loss.vf_loss / bs)
            tf.summary.scalar("model/entropy", self.loss.entropy / bs)
            tf.summary.scalar("model/grad_gnorm", tf.global_norm(self._grads))
            tf.summary.scalar("model/var_gnorm", tf.global_norm(self.var_list))
            self.summary_op = tf.summary.merge_all()

        self.sess.run(tf.global_variables_initializer())

    def optimizer(self):
        return tf.train.AdamOptimizer(self.config["lr"])

    def gradients(self, optimizer):
        grads = tf.gradients(self.loss.total_loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    def extra_compute_action_fetches(self):
        return {"behavior_logits": self.model.outputs}

    def extra_compute_grad_fetches(self):
        if self.config.get("summarize"):
            return {"summary": self.summary_op}
        else:
            return {}

    def get_initial_state(self):
        return self.model.state_init
