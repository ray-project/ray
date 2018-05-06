from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from ray.rllib.models.catalog import ModelCatalog
import tensorflow.contrib.slim as slim


def _scope_vars(scope, trainable_only=False):
    """
    Get variables inside a scope
    The scope can be specified as a string

    Parameters
    ----------
    scope: str or VariableScope
      scope in which the variables reside.
    trainable_only: bool
      whether or not to return only the variables that were marked as
      trainable.

    Returns
    -------
    vars: [tf.Variable]
      list of variables in `scope`.
    """
    return tf.get_collection(
        tf.GraphKeys.TRAINABLE_VARIABLES
        if trainable_only else tf.GraphKeys.GLOBAL_VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name)


def _build_q_network(
          registry, inputs, state_space, ac_space, act_t, config):
    x = inputs
    x = slim.fully_connected(x, 64)
    x = tf.nn.relu(x)
    x = tf.concat([x, act_t], axis=-1)
    x = slim.fully_connected(x, 64)
    x = tf.nn.relu(x)
    frontend = ModelCatalog.get_model(registry, x, 1, config["model"])
    x = frontend.outputs
    return x


def _build_actor_network(registry, inputs, ac_space, config):
    # frontend = ModelCatalog.get_model(registry, inputs, 1, config["model"])
    # act = frontend.outputs
    # a_bound = ac_space.high
    # act = tf.multiply(act, a_bound, name='scaled_a')
    # return act
    x = inputs
    x = slim.fully_connected(x, 64)
    x = tf.nn.relu(x)
    x = slim.fully_connected(x, 64)
    x = tf.nn.relu(x)
    x = slim.fully_connected(x, ac_space.shape[-1])
    x = tf.nn.tanh(x)
    return x


class DDPGGraph(object):
    def __init__(self, registry, env, config):
        self.env = env
        state_space = env.observation_space
        ac_space = env.action_space
        # num_actions = env.action_space.shape[0]
        # num_states = env.observation_space.shape[0]
        actor_optimizer = tf.train.AdamOptimizer(learning_rate=config["actor_lr"])
        critic_optimizer = tf.train.AdamOptimizer(learning_rate=config["critic_lr"])
        self.config = config
        # Action inputs
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")
        self.param_noise_stddev = tf.placeholder(
            tf.float32, shape=(), name='param_noise_stddev')

        with tf.variable_scope("evaluate_func_a")as scope:
            self.a_t = _build_actor_network(
                registry, self.obs_t, ac_space, config)
            self.a_var_list = _scope_vars(scope.name)

        # critical network evaluation
        with tf.variable_scope("evaluate_func_c")as scope:
            self.q_t = _build_q_network(
                registry, self.obs_t, state_space, ac_space, self.a_t, config)
            self.c_var_list = _scope_vars(scope.name)

        with tf.variable_scope("target_func_a") as scope:
            # target actor network evalution
            self.a_tp1 = _build_actor_network(
                registry, self.obs_tp1, ac_space, config)
            self.at_var_list = _scope_vars(scope.name)

        with tf.variable_scope("target_func_c") as scope:
            # target critical network evalution
            self.q_tp1 = _build_q_network(
                registry, self.obs_tp1,
                state_space, ac_space, self.a_tp1, config)
            self.ct_var_list = _scope_vars(scope.name)

        y_i = self.rew_t + config["gamma"] * self.q_tp1

        # compute the  error

        self.td_error = tf.losses.mean_squared_error(
            labels=y_i, predictions=self.q_t)
        self.action_lost = - tf.reduce_mean(self.q_t)

        self.loss_inputs = [
            ("obs", self.obs_t),
            ("rewards", self.rew_t),
            ("new_obs", self.obs_tp1),
            ("dones", self.done_mask),
            ("weights", self.importance_weights),
        ]
        self.a_grads = tf.gradients(self.action_lost, self.a_var_list)
        self.a_grads_and_vars = list(zip(self.a_grads, self.a_var_list))
        # self.c_grads = tf.gradients(self.td_error, self.c_var_list)
        # self.c_grads_and_vars = list(zip(self.c_grads, self.c_var_list))

        self.c_grads = critic_optimizer.minimize(
            self.td_error, var_list=self.c_var_list)

        self.train_expr = actor_optimizer.apply_gradients(self.a_grads_and_vars)

        update_target_expr = []
        for ta, ea, tc, ec in zip(
                self.at_var_list, self.a_var_list,
                self.ct_var_list, self.c_var_list):
            update_target_expr.append(
                ta.assign(config["tau"] * ea + (1-config["tau"]) * ta))
            update_target_expr.append(
                tc.assign(config["tau"] * ec + (1 - config["tau"]) * tc))
        self.update_target_expr = tf.group(*update_target_expr)

    def update_target(self, sess):

        return sess.run(self.update_target_expr)

    def copy_target(self, sess):
        copy_target_expr = []
        for ta, ea, tc, ec in zip(self.at_var_list,
                                  self.a_var_list,
                                  self.ct_var_list, self.c_var_list):
            copy_target_expr.append(ta.assign(ea))
            copy_target_expr.append(tc.assign(ec))
        copy_target = tf.group(*copy_target_expr)
        return sess.run(copy_target)

    def act(self, sess, obs, eps):
        actor_tf = self.a_t
        return sess.run(
            actor_tf,
            feed_dict={
                self.obs_t: obs,
                self.eps: eps,
            })

    def compute_gradients(
            self, sess, obs_t, rew_t, obs_tp1, done_mask):

        self.a_grads = [g for g in self.a_grads if g is not None]
        grads, _ = sess.run(
            [self.a_grads, self.c_grads],

            feed_dict={
                self.obs_t: obs_t,
                self.rew_t: rew_t,
                self.obs_tp1: obs_tp1,
                self.done_mask: done_mask,
            })
        return grads

    def apply_gradients(self, sess, grads):
        assert len(grads) == len(self.a_grads_and_vars)
        feed_dict = dict(zip(self.a_grads, grads))

        sess.run(self.train_expr, feed_dict=feed_dict)


