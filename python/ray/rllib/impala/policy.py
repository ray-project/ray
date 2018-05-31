from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import gym

import ray
from ray.experimental.tfutils import TensorFlowVariables
from ray.rllib.models import ModelCatalog
from ray.rllib.models.impalanet import ImpalaShadowNet
from ray.rllib.models.misc import (linear, normc_initializer)

class Policy(object):
    other_output = ["logprobs", "features"]
    is_recurrent = True

    def __init__(self, registry, ob_space, ac_space, config):
        self.registry = registry
        self.config = config
        self.g = tf.Graph()
        with self.g.as_default():
            self._setup_graph(ob_space, ac_space)
            assert all(hasattr(self, attr) for attr in ["vf", "logits", "x", "var_list"])
            print("Setting up loss")
            self.setup_loss(ac_space)
            self._setup_gradients()
            self.initialize()

    def _setup_graph(self, ob_space, ac_space):
        self.x = tf.placeholder(tf.float32, [None] + list(ob_space))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        
        model_options = {
            "num_trajs": self.config["optimizer"]["num_trajs"],
            "traj_length": self.config["num_local_steps"]}
        self._model = ImpalaShadowNet(self.x, self.logit_dim, model_options)

        self.state_init = self._model.state_init
        self.state_in = self._model.state_in
        self.state_out = self._model.state_out

        self.logits = self._model.outputs
        self.curr_dist = dist_class(self.logits)
        with tf.variable_scope("value_func"):
            self.vf = tf.reshape(
                linear(
                    self._model.last_layer, 1, "value",
                    normc_initializer(1.0)),
                [-1])

        self.sample = self.curr_dist.sample()
        self.logprobs = self.curr_dist.logp(self.sample)
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        self.global_step = tf.get_variable(
            "global_step", [], tf.int32,
            initializer=tf.constant_initializer(0, dtype=tf.int32),
            trainable=False)

    def setup_loss(self, action_space):
        num_trajs = self.config["optimizer"]["num_trajs"]
        traj_length = self.config["num_local_steps"]

        # initial states of LSTM
        self.state_ins = self._model.state_ins

        # input actions
        if isinstance(action_space, gym.spaces.Box):
            ac_size = action_space.shape[0]
            self.ac = tf.placeholder(tf.float32, [None, ac_size], name="ac")
        elif isinstance(action_space, gym.spaces.Discrete):
            self.ac = tf.placeholder(tf.int64, [None], name="ac")
        else:
            raise NotImplementedError(
                "action space" + str(type(action_space)) +
                "currently not supported")
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(action_space)
        # input rewards
        self.rwd = tf.placeholder(tf.float32, [None], name="rwd")
        # input \log\mu(a|s)
        self.actor_logprobs = tf.placeholder(tf.float32, [None], name="actor_logprobs")

        # calculate V_\pi(s), \log\pi(a|s)
        self.logitss = self._model.logits
        self.curr_dists = dist_class(self.logitss)
        with tf.variable_scope("value_func", reuse=True):
            self.vfs = tf.reshape(
                linear(
                    self._model.last_layers, 1, "value",
                    normc_initializer(1.0)),
                [-1])
        log_probs = self.curr_dists.logp(self.ac)

        # required values for calculating v-trace value
        log_diff = log_probs - self.actor_logprobs
        log_ruo_trunc = tf.clip_by_value(
            log_diff, -99999999, np.log(self.config["avg_ruo"]))
        log_c_trunc = tf.clip_by_value(
            log_diff, -99999999, np.log(self.config["avg_c"]))
        self.is_weights_ruo = tf.exp(log_ruo_trunc)
        self.is_weights_c = self.config["lambda"] * tf.exp(log_c_trunc)
        # TODO: should consider each trajectory seperately
        # this is invalid
        #delta_v = is_weights_ruo[:-1] * (self.rwd[:-1] + (self.config["gamma"] * self.vfs[1:]) - self.vfs[:-1])


        # input v-trace value
        self.vtrace = tf.placeholder(tf.float32, [num_trajs, traj_length], name="vtrace")
        # losses
        static_vfs = tf.reshape(tf.stop_gradient(self.vfs), [num_trajs, traj_length])
        static_ruo = tf.reshape(tf.stop_gradient(self.is_weights_ruo), [num_trajs, traj_length])
        shaped_rwd = tf.reshape(self.rwd, [num_trajs, traj_length])
        pi_loss = static_ruo[:,:-1] * tf.reshape(log_probs, [num_trajs, traj_length])[:,:-1] * (shaped_rwd[:,:-1]+self.config["gamma"]*self.vtrace[:,1:]-static_vfs[:,:-1])
        self.pi_loss = - tf.reduce_sum(pi_loss)
        delta = self.vfs - tf.reshape(self.vtrace, [-1])
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))
        self.entropy = tf.reduce_sum(self.curr_dists.entropy())
        self.loss = (self.pi_loss +
                     self.vf_loss * self.config["vf_loss_coeff"] +
                     self.entropy * self.config["entropy_coeff"])

    def _setup_gradients(self):
        grads = tf.gradients(self.loss, self.var_list)
        self.grads = [tf.clip_by_norm(grad, self.config["grad_norm_clipping"]) for grad in grads]
        grads_and_vars = list(zip(self.grads, self.var_list))
        opt = tf.train.RMSPropOptimizer(
            learning_rate=self.config["lr"],
            epsilon=self.config["epsilon"])
        self._apply_gradients = opt.apply_gradients(grads_and_vars)

    def initialize(self):
        self.sess = tf.Session(graph=self.g)
        self.variables = ray.experimental.TensorFlowVariables(self.loss,
                                                              self.sess)
        self.sess.run(tf.global_variables_initializer())

    def compute_gradients(self, samples):
        """Computing the gradient is actually model-dependent.

        The LSTM needs its hidden states in order to compute the gradient
        accurately.
        """
        features = [(np.squeeze(traj_fts[0][0]), np.squeeze(traj_fts[0][1])) for traj_fts in samples["features"]]
        feed_dict = {
            self.x: samples["obs"],
            self.ac: samples["actions"],
            self.rwd: samples["rewards"],
            self.state_ins[0]: features[0],
            self.state_ins[1]: features[1],
            self.actor_logprobs: samples["logprobs"]
        }
        sv, is_ruo, is_c = self.sess.run(
            [self.vfs, self.is_weights_ruo, self.is_weights_c],
            feed_dict=feed_dict)


        num_trajs = self.config["optimizer"]["num_trajs"]
        traj_length = self.config["num_local_steps"]
        sv = np.reshape(sv, [num_trajs, traj_length])
        is_ruo = np.reshape(is_ruo, [num_trajs, traj_length])
        is_c = np.reshape(is_c, [num_trajs, traj_length])
        rwd = np.reshape(samples["rewards"], [num_trajs, traj_length])

        delta_v = is_ruo[:,:-1] * (rwd[:,:-1]+self.config["gamma"]*sv[:,:-1]-sv[:,1:])

        def vtrace(v, dv, c):
            target_v = np.copy(v)
            cur = traj_length - self.config["n_step"] - 1
            for i in range(self.config["n_step"]):
                gamma = np.power(self.config["gamma"], i)
                cprod = 1.0
                for j in range(cur, cur+i):
                    cprod *= c[j]
                target_v[cur] += (gamma * cprod * dv[cur+i])
            en = range(cur)
            en.reverse()
            for i in en:
                target_v[cur] += (dv[i] + self.config["gamma"]*c[i]*(target_v[cur+1]-v[cur+1]))
            return target_v
        for idx in range(num_trajs):
            sv[idx] = vtrace(sv[idx], delta_v[idx], is_c[idx])
        feed_dict[self.vtrace] = sv

        info = {}
        grad, pi_loss, vf_loss, entropy_loss = self.sess.run([self.grads, self.pi_loss, self.vf_loss, self.entropy], feed_dict=feed_dict)
        print(pi_loss)
        print(vf_loss)
        print(entropy_loss)
        raw_input()

        return grad, info

    def apply_gradients(self, grads):
        feed_dict = {self.grads[i]: grads[i]
                     for i in range(len(grads))}
        self.sess.run(self._apply_gradients, feed_dict=feed_dict)

    def compute_and_apply_gradients(self, samples):
        grads, _ = self.compute_gradients(samples)
        self.apply_gradients(grads)

    def get_weights(self):
        weights = self.variables.get_weights()
        return weights

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def compute(self, ob, c, h):
        action, logprobs, c, h = self.sess.run(
            [self.sample, self.logprobs] + self.state_out,
            {self.x: [ob], self.state_in[0]: c, self.state_in[1]: h})
        return action[0], {"logprobs": logprobs[0], "features": (c, h)}

    def value(self, ob, c, h):
        vf = self.sess.run(self.vf, {self.x: [ob],
                                     self.state_in[0]: c,
                                     self.state_in[1]: h})
        return vf[0]

    def get_initial_features(self):
        return self.state_init
