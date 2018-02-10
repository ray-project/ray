from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import ray
import gym
from ray.rllib.a3c.policy import Policy


class TFPolicy(Policy):
    """The policy base class."""
    def __init__(self, registry, ob_space, action_space, config,
                 name="local", summarize=True):
        self.registry = registry
        self.local_steps = 0
        self.config = config
        self.summarize = summarize
        worker_device = "/job:localhost/replica:0/task:0/cpu:0"
        self.g = tf.Graph()
        with self.g.as_default(), tf.device(worker_device):
            with tf.variable_scope(name):
                self._setup_graph(ob_space, action_space)
                assert all([hasattr(self, attr)
                            for attr in ["vf", "logits", "x", "var_list"]])
            print("Setting up loss")
            self.setup_loss(action_space)
            self.setup_gradients()
            self.initialize()

    def _setup_graph(self, ob_space, ac_space):
        raise NotImplementedError

    def setup_loss(self, action_space):
        if isinstance(action_space, gym.spaces.Box):
            ac_size = action_space.shape[0]
            self.ac = tf.placeholder(tf.float32, [None, ac_size], name="ac")
        elif isinstance(action_space, gym.spaces.Discrete):
            self.ac = tf.placeholder(tf.int64, [None], name="ac")
        else:
            raise NotImplemented(
                "action space" + str(type(action_space)) +
                "currently not supported")
        self.adv = tf.placeholder(tf.float32, [None], name="adv")
        self.r = tf.placeholder(tf.float32, [None], name="r")

        log_prob = self.curr_dist.logp(self.ac)

        # The "policy gradients" loss: its derivative is precisely the policy
        # gradient. Notice that self.ac is a placeholder that is provided
        # externally. adv will contain the advantages, as calculated in
        # process_rollout.
        self.pi_loss = - tf.reduce_sum(log_prob * self.adv)

        delta = self.vf - self.r
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))
        self.entropy = tf.reduce_sum(self.curr_dist.entropy())
        self.loss = (self.pi_loss +
                     self.vf_loss * self.config["vf_loss_coeff"] +
                     self.entropy * self.config["entropy_coeff"])

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
            tf.summary.scalar("model/value_loss", self.vf_loss / bs)
            tf.summary.scalar("model/entropy", self.entropy / bs)
            tf.summary.scalar("model/grad_gnorm", tf.global_norm(self.grads))
            tf.summary.scalar("model/var_gnorm", tf.global_norm(self.var_list))
            self.summary_op = tf.summary.merge_all()

        # TODO(rliaw): Can consider exposing these parameters
        self.sess = tf.Session(graph=self.g, config=tf.ConfigProto(
            intra_op_parallelism_threads=1, inter_op_parallelism_threads=2,
            gpu_options=tf.GPUOptions(allow_growth=True)))
        self.variables = ray.experimental.TensorFlowVariables(self.loss,
                                                              self.sess)
        self.sess.run(tf.global_variables_initializer())

    def apply_gradients(self, grads):
        feed_dict = {self.grads[i]: grads[i]
                     for i in range(len(grads))}
        self.sess.run(self._apply_gradients, feed_dict=feed_dict)

    def get_weights(self):
        weights = self.variables.get_weights()
        return weights

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def compute_gradients(self, samples):
        raise NotImplementedError

    def compute(self, observation):
        raise NotImplementedError

    def value(self, ob):
        raise NotImplementedError
