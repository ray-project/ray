from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import gym

from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph


class A3CTFPolicyGraph(TFPolicyGraph):
    """The TF policy base class."""

    def __init__(self, ob_space, action_space, registry, config):
        self.registry = registry
        self.local_steps = 0
        self.config = config
        self.summarize = config.get("summarize")

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

        if self.summarize:
            bs = tf.to_float(tf.shape(self.x)[0])
            tf.summary.scalar("model/policy_graph", self.pi_loss / bs)
            tf.summary.scalar("model/value_loss", self.vf_loss / bs)
            tf.summary.scalar("model/entropy", self.entropy / bs)
            tf.summary.scalar("model/grad_gnorm", tf.global_norm(self._grads))
            tf.summary.scalar("model/var_gnorm", tf.global_norm(self.var_list))
            self.summary_op = tf.summary.merge_all()

    def _setup_graph(self, ob_space, ac_space):
        raise NotImplementedError

    def setup_loss(self, action_space):
        if isinstance(action_space, gym.spaces.Box):
            ac_size = action_space.shape[0]
            self.ac = tf.placeholder(tf.float32, [None, ac_size], name="ac")
        elif isinstance(action_space, gym.spaces.Discrete):
            self.ac = tf.placeholder(tf.int64, [None], name="ac")
        else:
            raise UnsupportedSpaceException(
                "Action space {} is not supported for A3C.".format(
                    action_space))
        self.adv = tf.placeholder(tf.float32, [None], name="adv")
        self.r = tf.placeholder(tf.float32, [None], name="r")

        log_prob = self.action_dist.logp(self.ac)

        # The "policy gradients" loss: its derivative is precisely the policy
        # gradient. Notice that self.ac is a placeholder that is provided
        # externally. adv will contain the advantages, as calculated in
        # compute_advantages.
        self.pi_loss = - tf.reduce_sum(log_prob * self.adv)

        delta = self.vf - self.r
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))
        self.entropy = tf.reduce_sum(self.action_dist.entropy())
        self.loss = (self.pi_loss +
                     self.vf_loss * self.config["vf_loss_coeff"] +
                     self.entropy * self.config["entropy_coeff"])

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
