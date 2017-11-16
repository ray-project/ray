from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.a3c.tfpolicy import TFPolicy
from ray.rllib.models.lstm import LSTM


class SharedModelLSTM(TFPolicy):

    def __init__(self, ob_space, ac_space, **kwargs):
        super(SharedModelLSTM, self).__init__(ob_space, ac_space, **kwargs)

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

    def compute_gradients(self, batch):
        """Computing the gradient is actually model-dependent.

        The LSTM needs its hidden states in order to compute the gradient
        accurately.
        """
        feed_dict = {
            self.x: batch.si,
            self.ac: batch.a,
            self.adv: batch.adv,
            self.r: batch.r,
            self.state_in[0]: batch.features[0],
            self.state_in[1]: batch.features[1]
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

    def compute_action(self, ob, c, h):
        action, vf, c, h = self.sess.run(
            [self.sample, self.vf] + self.state_out,
            {self.x: [ob], self.state_in[0]: c, self.state_in[1]: h})
        return action[0], vf[0], c, h

    def value(self, ob, c, h):
        # process_rollout is very non-intuitive due to value being a float
        vf = self.sess.run(self.vf, {self.x: [ob],
                                     self.state_in[0]: c,
                                     self.state_in[1]: h})
        return vf[0]

    def get_initial_features(self):
        return self.state_init
