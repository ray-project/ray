from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from ray.rllib.a3c.policy import (
    categorical_sample, linear,
    normalized_columns_initializer, Policy)

from ray.rllib.models.catalog import ModelCatalog


class SharedModel(Policy):
    def __init__(self, ob_space, ac_space, **kwargs):
        super(SharedModel, self).__init__(ob_space, ac_space, **kwargs)

    def setup_graph(self, ob_space, ac_space):
        num_actions = ac_space.n
        self.x = tf.placeholder(tf.float32, [None] + list(ob_space))
        dist_class, dist_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = ModelCatalog.ConvolutionalNetwork(self.x, dist_dim)
        self.logits = self._model.outputs
        self.vf = tf.reshape(linear(self._model.last_layer, 1, "value",
                                    normalized_columns_initializer(1.0)), [-1])

        self.sample = categorical_sample(self.logits, num_actions)[0, :]
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        self.global_step = tf.get_variable(
            "global_step", [], tf.int32,
            initializer=tf.constant_initializer(0, dtype=tf.int32),
            trainable=False)

    def get_gradients(self, batch):
        info = {}
        feed_dict = {
            self.x: batch.si,
            self.ac: batch.a,
            self.adv: batch.adv,
            self.r: batch.r,
        }

        self.local_steps += 1
        if self.summarize:
            grad, summ = self.sess.run([self.grads, self.summary_op],
                                       feed_dict=feed_dict)
            info['summary'] = summ
        else:
            grad = self.sess.run(self.grads, feed_dict=feed_dict)
        return grad, info

    def compute_actions(self, ob, *args):
        return self.sess.run([self.sample, self.vf],
                             {self.x: [ob]})

    def value(self, ob, *args):
        return self.sess.run(self.vf, {self.x: [ob]})[0]

    def get_initial_features(self):
        return []
