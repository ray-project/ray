from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import gym

from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph


class PPOTFPolicyGraph(TFPolicyGraph):
    """The TF policy base class."""

    def __init__(self, ob_space, action_space, registry, config):
        self.registry = registry
        self.config = config
        self._setup_graph(ob_space, action_space)
        assert all(hasattr(self, attr)
                   for attr in ["vf", "logits", "x", "var_list"])
        print("Setting up loss")
        self.setup_loss(action_space)
        self.is_training = tf.placeholder_with_default(True, ())
        self.sess = tf.get_default_session()

        TFPolicyGraph.__init__(
            self, self.sess, obs_input=self.x,
            action_sampler=self.action_dist.sample(), loss=self.loss,
            loss_inputs=self.loss_in, is_training=self.is_training,
            state_inputs=self.state_in, state_outputs=self.state_out)

        self.sess.run(tf.global_variables_initializer())

    def _setup_graph(self, ob_space, ac_space):
        self.observations = tf.placeholder(
            tf.float32, shape=(None,) + self.env.observation_space.shape)

        self.dist_class, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self.curr_logits = ModelCatalog.get_model(
            registry, observations, logit_dim, self.config["model"]).outputs
        self.logits = self._model.outputs
        self.action_dist = self.dist_class(self.logits)
        self.sampler = self.action_dist.sample()

        if self.config["use_gae"]:
            vf_config = self.config["model"].copy()
            # Do not split the last layer of the value function into
            # mean parameters and standard deviation parameters and
            # do not make the standard deviations free variables.
            vf_config["free_log_std"] = False
            with tf.variable_scope("value_function"):
                self.value_function = ModelCatalog.get_model(
                    registry, observations, 1, vf_config).outputs
            self.value_function = tf.reshape(self.value_function, [-1])


        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        self.global_step = tf.get_variable(
            "global_step", [], tf.int32,
            initializer=tf.constant_initializer(0, dtype=tf.int32),
            trainable=False)

    def setup_loss(self, action_space):
        # Defines the training inputs:
        # The coefficient of the KL penalty.
        self.kl_coeff = tf.placeholder(
            name="newkl", shape=(), dtype=tf.float32)

        self.actions = ModelCatalog.get_action_placeholder(action_space)
        # Targets of the value function.
        self.value_targets = tf.placeholder(tf.float32, shape=(None,))
        # Advantage values in the policy gradient estimator.
        self.advantages = tf.placeholder(tf.float32, shape=(None,))
        # Log probabilities from the policy before the policy update.
        self.prev_logits = tf.placeholder(
            tf.float32, shape=(None, self.logit_dim))
        self.prev_dist = self.dist_class(self.prev_logits)
        # Value function predictions before the policy update.
        self.prev_vf_preds = tf.placeholder(tf.float32, shape=(None,))

        # Make loss functions.
        self.ratio = tf.exp(self.curr_dist.logp(actions) -
                            self.prev_dist.logp(actions))
        self.kl = self.prev_dist.kl(self.curr_dist)
        self.mean_kl = tf.reduce_mean(self.kl)
        self.entropy = self.curr_dist.entropy()
        self.mean_entropy = tf.reduce_mean(self.entropy)
        self.surr1 = self.ratio * advantages
        self.surr2 = tf.clip_by_value(self.ratio, 1 - config["clip_param"],
                                      1 + config["clip_param"]) * advantages
        self.surr = tf.minimum(self.surr1, self.surr2)
        self.mean_policy_loss = tf.reduce_mean(-self.surr)

        if config["use_gae"]:
            # We use a huber loss here to be more robust against outliers,
            # which seem to occur when the rollouts get longer (the variance
            # scales superlinearly with the length of the rollout)
            self.vf_loss1 = tf.square(self.value_function - value_targets)
            vf_clipped = prev_vf_preds + tf.clip_by_value(
                self.value_function - prev_vf_preds,
                -config["clip_param"], config["clip_param"])
            self.vf_loss2 = tf.square(vf_clipped - value_targets)
            self.vf_loss = tf.minimum(self.vf_loss1, self.vf_loss2)
            self.mean_vf_loss = tf.reduce_mean(self.vf_loss)
            self.loss = tf.reduce_mean(
                -self.surr + kl_coeff * self.kl +
                config["vf_loss_coeff"] * self.vf_loss -
                config["entropy_coeff"] * self.entropy)
        else:
            self.mean_vf_loss = tf.constant(0.0)
            self.loss = tf.reduce_mean(
                -self.surr +
                kl_coeff * self.kl -
                config["entropy_coeff"] * self.entropy)

        self.loss_in = [
            ("obs", self.observations),
            ("value_targets", self.value_targets),
            ("advantages", self.advantages),
            ("actions", self.actions),
            ("logprobs", self.prev_logits),
            ("vf_preds", self.prev_vf_preds)
        ]

    def optimizer(self):
        return tf.train.AdamOptimizer(self.config["lr"])

    def gradients(self, optimizer):
        grads = tf.gradients(self.loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        clipped_grads = list(zip(self.grads, self.var_list))
        return clipped_grads

    def extra_compute_grad_fetches(self):
        if self.summarize:
            return {"summary": self.summary_op}
        else:
            return {}

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            next_state = []
            for i in range(len(self.state_in)):
                next_state.append([sample_batch["state_out_{}".format(i)][-1]])
            last_r = self.value(sample_batch["new_obs"][-1], *next_state)
        return compute_advantages(
            sample_batch, last_r, self.config["gamma"], self.config["lambda"])
