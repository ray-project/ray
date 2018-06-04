from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf
import gym

import ray
from ray.experimental.tfutils import TensorFlowVariables
from ray.rllib.models import ModelCatalog
from ray.rllib.models.impalanet import ImpalaShallowNet
from ray.rllib.models.misc import (linear, normc_initializer)


class Policy(object):
    other_output = ["logprobs"]
    is_recurrent = False

    def __init__(self, registry, ob_space, ac_space, config):
        self.registry = registry
        self.config = config
        self.g = tf.Graph()
        with self.g.as_default():
            self._setup_graph(ob_space, ac_space)
            assert all(hasattr(self, attr) for attr in ["vf", "logits", "x", "var_list"])
            print("Setting up loss")
            self._setup_loss(ac_space)
            self._setup_gradients()
            self.initialize()

    def _setup_graph(self, ob_space, ac_space):
        self.x = tf.placeholder(tf.float32, [None] + list(ob_space))
        dist_class, self.logit_dim = ModelCatalog.get_action_dist(ac_space)
        
        model_options = {
            "num_trajs": self.config["optimizer"]["num_trajs"],
            "traj_length": self.config["num_local_steps"]}
        if self.config["use_lstm"]:
            self._model = ImpalaShadowNet(self.x, self.logit_dim, model_options)
            self.other_output.append("features")
            is_recurrent = True
            self.state_init = self._model.state_init
            self.state_in = self._model.state_in
            self.state_out = self._model.state_out

            def get_initial_features(self):
                return self.state_init
            self.get_initial_features = get_initial_features()

        else:
            self._model = ModelCatalog.get_model(
                self.registry,
                self.x,
                self.logit_dim,
                model_options)

        self.logits = self._model.outputs
        self.curr_dist = dist_class(self.logits)
        with tf.variable_scope("value_func"):
            self.vf = tf.reshape(
                linear(
                    self._model.last_layer, 1, "value",
                    normc_initializer(1.0)),
                [-1])

        self.sample = self.curr_dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)
        self.global_step = tf.get_variable(
            "global_step", [], tf.int32,
            initializer=tf.constant_initializer(0, dtype=tf.int32),
            trainable=False)

    def _setup_loss(self, action_space):
        num_trajs = self.config["optimizer"]["num_trajs"]
        traj_length = self.config["num_local_steps"]

        if self.is_recurrent:
            self.state_ins = self._model.state_ins

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
        self.rwd = tf.placeholder(tf.float32, [None], name="rwd")
        self.prev_logits = tf.placeholder(tf.float32, [None, self.logit_dim], name="actor_logprobs")

        self.prev_dists = dist_class(self.prev_logits)
        if self.is_recurrent:
            self.curr_dists = dist_class(self._model.logits)
            with tf.variable_scope("value_func", reuse=True):
                self.vfs = tf.reshape(
                    linear(
                        self._model.last_layers, 1, "value",
                        normc_initializer(1.0)),
                    [-1])
        else:
            self.curr_dists = dist_class(self.logits)
            self.vfs = self.vf

        log_probs = self.curr_dists.logp(self.ac)

        # required values for calculating v-trace value
        self.ratio = tf.exp(log_probs -
                            self.prev_dists.logp(self.ac))
        self.is_weights_ruo = tf.minimum(self.config["avg_ruo"], self.ratio)
        self.is_weights_c = self.config["lambda"] * tf.minimum(self.config["avg_c"], self.ratio)

        self.vtrace = tf.placeholder(tf.float32, [num_trajs, traj_length], name="vtrace")
        self.dones = tf.placeholder(tf.float32, [num_trajs, traj_length], name="dones")

        # losses
        static_vfs = tf.reshape(tf.stop_gradient(self.vfs), [num_trajs, traj_length])
        static_ruo = tf.reshape(tf.stop_gradient(self.is_weights_ruo), [num_trajs, traj_length])
        shaped_rwd = tf.reshape(self.rwd, [num_trajs, traj_length])
        pi_loss = (1.0-self.dones[:,:-1]) * static_ruo[:,:-1] * tf.reshape(log_probs, [num_trajs, traj_length])[:,:-1] * (shaped_rwd[:,:-1]+self.config["gamma"]*self.vtrace[:,1:]-static_vfs[:,:-1])
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

        feed_dict = {
            self.x: samples["obs"],
            self.ac: samples["actions"],
            self.rwd: samples["rewards"],
            self.prev_logits: samples["logprobs"]
        }
        if self.is_recurrent:
            features = ([np.squeeze(traj_fts[0][0]) for traj_fts in samples["features"]], [np.squeeze(traj_fts[0][1]) for traj_fts in samples["features"]])
            feed_dict[self.state_ins[0]] = features[0]
            feed_dict[self.state_ins[1]] = features[1]

        sv, is_ruo, is_c = self.sess.run(
            [self.vfs, self.is_weights_ruo, self.is_weights_c],
            feed_dict=feed_dict)


        num_trajs = self.config["optimizer"]["num_trajs"]
        traj_length = self.config["num_local_steps"]
        sv = np.reshape(sv, [num_trajs, traj_length])
        is_ruo = np.reshape(is_ruo, [num_trajs, traj_length])
        is_c = np.reshape(is_c, [num_trajs, traj_length])
        rwd = np.reshape(samples["rewards"], [num_trajs, traj_length])
        dones = np.reshape(samples["dones"], [num_trajs, traj_length]).astype("float32")

        #delta_v = is_ruo[:,:-1] * (rwd[:,:-1]+self.config["gamma"]*(1.0-dones[:,:-1])*sv[:,1:]-sv[:,:-1])
        delta_v = is_ruo*(rwd+self.config["gamma"]*(1.0-dones)*(np.concatenate((sv[:,1:],np.ones([1,num_trajs]).astype("float32").T), axis=1))-sv)

        def vtrace(v, dv, c, d):
            target_v = np.copy(v)
            i = 0
            while i < traj_length:
                if d[i] > 0.5:
                    break
                i += 1
            print("last_has_delta: %d" % i)
            if i == traj_length:
                cur = traj_length - 2
            else:
                cur = i
                #d[i] = 0.0
            en = range(cur+1)
            en.reverse()
            for i in en:
                if i + self.config["n_step"] > cur:
                    for j in range(self.config["n_step"]):
                        if i + j > cur:
                            break
                        gamma = np.power(self.config["gamma"], j)
                        cprod = 1.0
                        for k in range(i, i+j):
                            cprod *= c[k]
                        target_v[i] += (gamma * cprod * dv[i+j])
                else:
                    target_v[i] += (dv[i] + self.config["gamma"]*c[i]*(target_v[i+1]-v[i+1]))
            return target_v
        print(sv)
        print(rwd)
        for idx in range(num_trajs):
            sv[idx] = vtrace(sv[idx], delta_v[idx], is_c[idx], dones[idx])
        feed_dict[self.vtrace] = sv
        feed_dict[self.dones] = dones
        print(sv)
        #raw_input()

        info = {}
        grad, pi_loss, vf_loss, entropy_loss = self.sess.run([self.grads, self.pi_loss, self.vf_loss, self.entropy], feed_dict=feed_dict)
        print(pi_loss)
        print(vf_loss)
        print(entropy_loss)

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

    def compute(self, ob, c=None, h=None):
        if self.is_recurrent:
            action, logprobs, c, h = self.sess.run(
                [self.sample, self.logits] + self.state_out,
                {self.x: [ob], self.state_in[0]: c, self.state_in[1]: h})
            return action[0], {"logprobs": logprobs[0], "features": (c, h)}
        else:
            action, logprobs = self.sess.run(
                [self.sample, self.logits],
                {self.x: [ob]})
            return action[0], {"logprobs": logprobs[0]}

    """
    def value(self, ob, c, h):
        vf = self.sess.run(self.vf, {self.x: [ob],
                                     self.state_in[0]: c,
                                     self.state_in[1]: h})
        return vf[0]
    """
