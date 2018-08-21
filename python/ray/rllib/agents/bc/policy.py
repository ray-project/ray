from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import gym

import ray
from ray.rllib.models.catalog import ModelCatalog


class BCPolicy(object):
    def __init__(self, obs_space, action_space, config):
        self.local_steps = 0
        self.config = config
        self.summarize = config.get("summarize")
        self._setup_graph(obs_space, action_space)
        self.setup_loss(action_space)
        self.setup_gradients()
        self.initialize()

    def _setup_graph(self, obs_space, ac_space):
        self.x = tf.placeholder(tf.float32, [None] + list(obs_space.shape))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(
            ac_space, self.config["model"])
        self._model = ModelCatalog.get_model(self.x, self.logit_dim,
                                             self.config["model"])
        self.logits = self._model.outputs
        self.curr_dist = dist_class(self.logits)
        self.sample = self.curr_dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

    def setup_loss(self, action_space):
        if isinstance(action_space, gym.spaces.Box):
            self.ac = tf.placeholder(
                tf.float32, [None] + list(action_space.shape), name="ac")
        elif isinstance(action_space, gym.spaces.Discrete):
            self.ac = tf.placeholder(tf.int64, [None], name="ac")
        else:
            raise NotImplementedError("action space" +
                                      str(type(action_space)) +
                                      "currently not supported")
        log_prob = self.curr_dist.logp(self.ac)
        self.pi_loss = -tf.reduce_sum(log_prob)
        self.loss = self.pi_loss

    def setup_gradients(self):
        grads = tf.gradients(self.loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
        grads_and_vars = list(zip(self.grads, self.var_list))
        opt = tf.train.AdamOptimizer(self.config["lr"])
        self._apply_gradients = opt.apply_gradients(grads_and_vars)

    def initialize(self):
        if self.summarize:
            bs = tf.to_float(tf.shape(self.x)[0])
            tf.summary.scalar("model/policy_loss", self.pi_loss / bs)
            tf.summary.scalar("model/grad_gnorm", tf.global_norm(self.grads))
            tf.summary.scalar("model/var_gnorm", tf.global_norm(self.var_list))
            self.summary_op = tf.summary.merge_all()

        # TODO(rliaw): Can consider exposing these parameters
        self.sess = tf.Session(
            graph=self.g,
            config=tf.ConfigProto(
                intra_op_parallelism_threads=1,
                inter_op_parallelism_threads=2,
                gpu_options=tf.GPUOptions(allow_growth=True)))
        self.variables = ray.experimental.TensorFlowVariables(
            self.loss, self.sess)
        self.sess.run(tf.global_variables_initializer())

    def compute_gradients(self, samples):
        info = {}
        feed_dict = {
            self.x: samples["observations"],
            self.ac: samples["actions"]
        }
        self.grads = [g for g in self.grads if g is not None]
        self.local_steps += 1
        if self.summarize:
            loss, grad, summ = self.sess.run(
                [self.loss, self.grads, self.summary_op], feed_dict=feed_dict)
            info["summary"] = summ
        else:
            loss, grad = self.sess.run(
                [self.loss, self.grads], feed_dict=feed_dict)
        info["num_samples"] = len(samples)
        info["loss"] = loss
        return grad, info

    def apply_gradients(self, grads):
        feed_dict = {self.grads[i]: grads[i] for i in range(len(grads))}
        self.sess.run(self._apply_gradients, feed_dict=feed_dict)

    def get_weights(self):
        weights = self.variables.get_weights()
        return weights

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def compute(self, ob, *args):
        action = self.sess.run(self.sample, {self.x: [ob]})
        return action, None
