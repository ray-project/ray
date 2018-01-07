from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from ray.rllib.bc.tfpolicy import TFPolicy
from ray.rllib.models.catalog import ModelCatalog


class Policy(TFPolicy):
    def _setup_graph(self, ob_space, ac_space):
        self.x = tf.placeholder(tf.float32, [None] + list(ob_space))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        self._model = ModelCatalog.get_model(
            self.registry, self.x, self.logit_dim, self.config["model"])
        self.logits = self._model.outputs
        self.curr_dist = dist_class(self.logits)
        self.sample = self.curr_dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

    def setup_loss(self, action_space):
        self.ac = tf.placeholder(tf.int64, [None], name="ac")
        log_prob = self.curr_dist.logp(self.ac)
        self.pi_loss = - tf.reduce_sum(log_prob)
        self.loss = self.pi_loss

    def compute_gradients(self, samples):
        info = {}
        feed_dict = {
            self.x: samples["observations"],
            self.ac: samples["actions"]
        }
        self.grads = [g for g in self.grads if g is not None]
        self.local_steps += 1
        if self.summarize:
            loss, grad, summ = self.sess.run([self.loss, self.grads, self.summary_op], feed_dict=feed_dict)
            info["summary"] = summ
        else:
            loss, grad = self.sess.run([self.loss, self.grads], feed_dict=feed_dict)
        info["num_samples"] = len(samples)
        info["loss"] = loss
        return grad, info

    def compute(self, ob, *args):
        action = self.sess.run(self.sample, {self.x: [ob]})
        return action, None
