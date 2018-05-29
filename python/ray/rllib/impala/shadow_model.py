from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
from ray.rllib.models.misc import linear, normc_initializer
from ray.experimental.tfutils import TensorFlowVariables
from ray.rllib.impala.policy import Policy
from ray.rllib.models.lstm import LSTM


class ShadowModel(Policy):
    other_output = ["logprobs", "features"]
    is_recurrent = True

    def __init__(self, registry, ob_space, ac_space, config, **kwargs):
        super(SharedModelLSTM, self).__init__(
            registry, ob_space, ac_space, config, **kwargs)

    def _setup_graph(self, ob_space, ac_space):
        self.x = tf.placeholder(tf.float32, [None] + list(ob_space))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = LSTM(self.x, self.logit_dim, {})

        self.state_init = self._model.state_init
        self.state_in = self._model.state_in
        self.state_out = self._model.state_out

        self.logits = self._model.outputs
        self.curr_dist = dist_class(self.logits)
        # with tf.variable_scope("vf"):
        #     vf_model = ModelCatalog.get_model(self.x, 1)
        self.vf = tf.reshape(linear(self._model.last_layer, 1, "value",
                                    normc_initializer(1.0)), [-1])

        self.sample = self.curr_dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        self.global_step = tf.get_variable(
            "global_step", [], tf.int32,
            initializer=tf.constant_initializer(0, dtype=tf.int32),
            trainable=False)

    def _setup_gradients(self):
        """Setup critic and actor gradients."""
        self.critic_grads = tf.gradients(
                           self.model.critic_loss, self.model.critic_var_list)
        c_grads_and_vars = list(zip(
                          self.critic_grads, self.model.critic_var_list))
        c_opt = tf.train.AdamOptimizer(self.config["critic_lr"])
        self._apply_c_gradients = c_opt.apply_gradients(c_grads_and_vars)

        self.actor_grads = tf.gradients(
                          -self.model.cn_for_loss, self.model.actor_var_list)
        a_grads_and_vars = list(zip(
                          self.actor_grads, self.model.actor_var_list))
        a_opt = tf.train.AdamOptimizer(self.config["actor_lr"])
        self._apply_a_gradients = a_opt.apply_gradients(a_grads_and_vars)

    def compute_gradients(self, samples):
        """Computing the gradient is actually model-dependent.

        The LSTM needs its hidden states in order to compute the gradient
        accurately.
        """
        features = samples["features"][0]
        feed_dict = {
            self.x: samples["obs"],
            self.ac: samples["actions"],
            self.adv: samples["advantages"],
            self.r: samples["value_targets"],
            self.state_in[0]: features[0],
            self.state_in[1]: features[1]
        }
        info = {}
        self.local_steps += 1
        if self.summarize and self.local_steps % 10 == 0:
            grad, summ = self.sess.run([self.grads, self.summary_op],
                                       feed_dict=feed_dict)
            info['summary'] = summ
        else:
            grad = self.sess.run(self.grads, feed_dict=feed_dict)
        return grad, info

    def compute(self, ob, c, h):
        action, logprobs, c, h = self.sess.run(
            [self.sample, self.logits] + self.state_out,
            {self.x: [ob], self.state_in[0]: c, self.state_in[1]: h})
        return action[0], {"logprobs": logprobs[0], "features": (c, h)}

    def value(self, ob, c, h):
        vf = self.sess.run(self.vf, {self.x: [ob],
                                     self.state_in[0]: c,
                                     self.state_in[1]: h})
        return vf[0]

    def get_initial_features(self):
        return self.state_init
