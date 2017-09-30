from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.a3c.policy import Policy
from ray.rllib.models.lstm import LSTM


class SharedModelLSTM(Policy):

    def __init__(self, ob_space, ac_space, config, **kwargs):
        super(SharedModelLSTM, self).__init__(ob_space, ac_space, config, **kwargs)

    def setup_graph(self, ob_space, ac_space):
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

    def get_gradients(self, batch):
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

    def run_sgd(self, batch_list, iterations, debug=False):
        """ Move this to LSTM """
        for i in range(iterations):
            self.local_steps += 1
            mini_batch = batch_list[i]
            feed_dict = {self.x: mini_batch['si'],
                         self.ac: mini_batch['a'],
                         self.adv: mini_batch['adv'],
                         self.r: mini_batch['r'],
                         self.state_in[0]: mini_batch["features"][0],
                         self.state_in[1]: mini_batch["features"][1]}
            _, summary = self.sess.run([self._apply_gradients,
                                        self.summary_op], feed_dict=feed_dict)
            if debug:
                piloss, vloss = self.sess.run([self.pi_loss,
                                               self.vf_loss], feed_dict=feed_dict)
                print("Piloss: %5.4f \t Vfloss: %5.4f" % (piloss, vloss))
        info = {"summary": summary}
        return info

    def compute_actions(self, ob, c, h):
        output = self.sess.run([self.sample, self.vf] + self.state_out,
                               {self.x: [ob],
                                self.state_in[0]: c,
                                self.state_in[1]: h})
        output = list(output)
        output[0] = output[0][0]
        return output

    def value(self, ob, c, h):
        # process_rollout is very non-intuitive due to value being a float
        return self.sess.run(self.vf, {self.x: [ob],
                                       self.state_in[0]: c,
                                       self.state_in[1]: h})[0]

    def get_initial_features(self):
        return self.state_init
