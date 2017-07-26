from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import ray


class Policy(object):
    """The policy base class."""
    def __init__(self, ob_space, ac_space, name="local", summarize=True):
        self.local_steps = 0
        self.summarize = summarize
        worker_device = "/job:localhost/replica:0/task:0/cpu:0"
        self.g = tf.Graph()
        with self.g.as_default(), tf.device(worker_device):
            with tf.variable_scope(name):
                self.setup_graph(ob_space, ac_space)
                assert all([hasattr(self, attr)
                            for attr in ["vf", "logits", "x", "var_list"]])
            print("Setting up loss")
            self.setup_loss(ac_space)
            self.initialize()

    def setup_graph(self):
        raise NotImplementedError

    def setup_loss(self, ac_space):
        num_actions = ac_space.n
        self.ac = tf.placeholder(tf.float32, [None, num_actions], name="ac")
        self.adv = tf.placeholder(tf.float32, [None], name="adv")
        self.r = tf.placeholder(tf.float32, [None], name="r")

        log_prob_tf = tf.nn.log_softmax(self.logits)
        prob_tf = tf.nn.softmax(self.logits)

        # The "policy gradients" loss: its derivative is precisely the policy
        # gradient. Notice that self.ac is a placeholder that is provided
        # externally. adv will contain the advantages, as calculated in
        # process_rollout.
        pi_loss = - tf.reduce_sum(tf.reduce_sum(log_prob_tf * self.ac,
                                                [1]) * self.adv)

        # loss of value function
        vf_loss = 0.5 * tf.reduce_sum(tf.square(self.vf - self.r))
        entropy = - tf.reduce_sum(prob_tf * log_prob_tf)

        bs = tf.to_float(tf.shape(self.x)[0])
        self.loss = pi_loss + 0.5 * vf_loss - entropy * 0.01

        grads = tf.gradients(self.loss, self.var_list)
        self.grads, _ = tf.clip_by_global_norm(grads, 40.0)

        grads_and_vars = list(zip(self.grads, self.var_list))
        opt = tf.train.AdamOptimizer(1e-4)
        self._apply_gradients = opt.apply_gradients(grads_and_vars)

        if self.summarize:
            tf.summary.scalar("model/policy_loss", pi_loss / bs)
            tf.summary.scalar("model/value_loss", vf_loss / bs)
            tf.summary.scalar("model/entropy", entropy / bs)
            tf.summary.scalar("model/grad_global_norm", tf.global_norm(self.grads))
            tf.summary.scalar("model/var_global_norm", tf.global_norm(self.var_list))
            self.summary_op = tf.summary.merge_all()

    def initialize(self):
        self.sess = tf.Session(graph=self.g, config=tf.ConfigProto(
            intra_op_parallelism_threads=1, inter_op_parallelism_threads=2))
        self.variables = ray.experimental.TensorFlowVariables(self.loss,
                                                              self.sess)
        self.sess.run(tf.global_variables_initializer())

    def model_update(self, grads):
        feed_dict = {self.grads[i]: grads[i]
                     for i in range(len(grads))}
        self.sess.run(self._apply_gradients, feed_dict=feed_dict)

    def get_weights(self):
        weights = self.variables.get_weights()
        return weights

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def get_gradients(self, batch):
        raise NotImplementedError

    def get_vf_loss(self):
        raise NotImplementedError

    def compute_actions(self, observations):
        raise NotImplementedError

    def value(self, ob):
        raise NotImplementedError


def normalized_columns_initializer(std=1.0):
    def _initializer(shape, dtype=None, partition_info=None):
        out = np.random.randn(*shape).astype(np.float32)
        out *= std / np.sqrt(np.square(out).sum(axis=0, keepdims=True))
        return tf.constant(out)
    return _initializer


def flatten(x):
    return tf.reshape(x, [-1, np.prod(x.get_shape().as_list()[1:])])


def conv2d(x, num_filters, name, filter_size=(3, 3), stride=(1, 1), pad="SAME",
           dtype=tf.float32, collections=None):
    with tf.variable_scope(name):
        stride_shape = [1, stride[0], stride[1], 1]
        filter_shape = [filter_size[0], filter_size[1], int(x.get_shape()[3]),
                        num_filters]

        # There are "num input feature maps * filter height * filter width"
        # inputs to each hidden unit.
        fan_in = np.prod(filter_shape[:3])
        # Each unit in the lower layer receives a gradient from: "num output
        # feature maps * filter height * filter width" / pooling size.
        fan_out = np.prod(filter_shape[:2]) * num_filters
        # Initialize weights with random weights.
        w_bound = np.sqrt(6 / (fan_in + fan_out))

        w = tf.get_variable("W", filter_shape, dtype,
                            tf.random_uniform_initializer(-w_bound, w_bound),
                            collections=collections)
        b = tf.get_variable("b", [1, 1, 1, num_filters],
                            initializer=tf.constant_initializer(0.0),
                            collections=collections)
        return tf.nn.conv2d(x, w, stride_shape, pad) + b


def linear(x, size, name, initializer=None, bias_init=0):
    w = tf.get_variable(name + "/w", [x.get_shape()[1], size],
                        initializer=initializer)
    b = tf.get_variable(name + "/b", [size],
                        initializer=tf.constant_initializer(bias_init))
    return tf.matmul(x, w) + b


def categorical_sample(logits, d):
    value = tf.squeeze(tf.multinomial(logits - tf.reduce_max(logits, [1],
                                                             keep_dims=True),
                                      1), [1])
    return tf.one_hot(value, d)
