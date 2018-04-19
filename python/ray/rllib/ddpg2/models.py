from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import tensorflow as tf
import tensorflow.contrib.layers as layers

from ray.rllib.models import ModelCatalog


def _build_p_network(registry, inputs, dim_actions, config):
    """
    map an observation (i.e., state) to an action where
    each entry takes value from (0, 1) due to the sigmoid function
    """
    frontend = ModelCatalog.get_model(registry, inputs, 1, config["model"])

    hiddens = config["actor_hiddens"]
    action_out = frontend.last_layer
    for hidden in hiddens:
        action_out = layers.fully_connected(
            action_out, num_outputs=hidden, activation_fn=tf.nn.relu)
    # Use sigmoid layer to bound values within (0, 1)
    # shape of action_scores is [batch_size, dim_actions]
    action_scores = layers.fully_connected(
        action_out, num_outputs=dim_actions, activation_fn=tf.nn.sigmoid)

    return action_scores


# As a stochastic policy for inference, but a deterministic policy for training
# thus ignore batch_size issue when constructing a stochastic action
def _build_action_network(p_values, low_action, high_action, stochastic, eps,
                          theta, sigma):
    # shape is [None, dim_action]
    deterministic_actions = (high_action - low_action) * p_values + low_action

    exploration_sample = tf.get_variable(
        name="ornstein_uhlenbeck",
        dtype=tf.float32,
        initializer=low_action.size * [.0],
        trainable=False)
    normal_sample = tf.random_normal(
        shape=[low_action.size], mean=0.0, stddev=1.0)
    exploration_value = tf.assign_add(
        exploration_sample,
        theta * (.0 - exploration_sample) + sigma * normal_sample)
    stochastic_actions = deterministic_actions + eps * (
        high_action - low_action) * exploration_value

    return tf.cond(stochastic, lambda: stochastic_actions,
                   lambda: deterministic_actions)


def _build_q_network(registry, inputs, action_inputs, config):
    frontend = ModelCatalog.get_model(registry, inputs, 1, config["model"])

    hiddens = config["critic_hiddens"]

    q_out = tf.concat([frontend.last_layer, action_inputs], axis=1)
    for hidden in hiddens:
        q_out = layers.fully_connected(
            q_out, num_outputs=hidden, activation_fn=tf.nn.relu)
    q_scores = layers.fully_connected(q_out, num_outputs=1, activation_fn=None)

    return q_scores


def _huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return tf.where(
        tf.abs(x) < delta,
        tf.square(x) * 0.5, delta * (tf.abs(x) - 0.5 * delta))


def _minimize_and_clip(optimizer, objective, var_list, clip_val=10):
    """Minimized `objective` using `optimizer` w.r.t. variables in
    `var_list` while ensure the norm of the gradients for each
    variable is clipped to `clip_val`
    """
    gradients = optimizer.compute_gradients(objective, var_list=var_list)
    for i, (grad, var) in enumerate(gradients):
        if grad is not None:
            gradients[i] = (tf.clip_by_norm(grad, clip_val), var)
    return gradients


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
        if trainable_only else tf.GraphKeys.VARIABLES,
        scope=scope if isinstance(scope, str) else scope.name)


class ModelAndLoss(object):
    """Holds the model and loss function.

    Both graphs are necessary in order for the multi-gpu SGD implementation
    to create towers on each device.
    """

    def __init__(self, registry, dim_actions, low_action, high_action, config,
                 obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
        # p network evaluation
        with tf.variable_scope("p_func", reuse=True) as scope:
            self.p_t = _build_p_network(registry, obs_t, dim_actions, config)

        # target p network evaluation
        with tf.variable_scope("target_p_func") as scope:
            self.p_tp1 = _build_p_network(registry, obs_tp1, dim_actions,
                                          config)
            self.target_p_func_vars = _scope_vars(scope.name)

        # Action outputs
        with tf.variable_scope("a_func", reuse=True):
            deterministic_flag = tf.constant(value=False, dtype=tf.bool)
            zero_eps = tf.constant(value=.0, dtype=tf.float32)
            output_actions = _build_action_network(
                self.p_t, low_action, high_action, deterministic_flag,
                zero_eps, config["exploration_theta"],
                config["exploration_sigma"])

            output_actions_estimated = _build_action_network(
                self.p_tp1, low_action, high_action, deterministic_flag,
                zero_eps, config["exploration_theta"],
                config["exploration_sigma"])

        # q network evaluation
        with tf.variable_scope("q_func") as scope:
            self.q_t = _build_q_network(registry, obs_t, act_t, config)
            self.q_func_vars = _scope_vars(scope.name)
        with tf.variable_scope("q_func", reuse=True):
            self.q_tp0 = _build_q_network(registry, obs_t, output_actions,
                                          config)

        # target q network evalution
        with tf.variable_scope("target_q_func") as scope:
            self.q_tp1 = _build_q_network(registry, obs_tp1,
                                          output_actions_estimated, config)
            self.target_q_func_vars = _scope_vars(scope.name)

        q_t_selected = tf.squeeze(self.q_t, axis=len(self.q_t.shape) - 1)

        q_tp1_best = tf.squeeze(
            input=self.q_tp1, axis=len(self.q_tp1.shape) - 1)
        q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = (
            rew_t + config["gamma"]**config["n_step"] * q_tp1_best_masked)

        # compute the error (potentially clipped)
        self.td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
        if config.get("use_huber"):
            errors = _huber_loss(self.td_error, config.get("huber_threshold"))
        else:
            errors = 0.5 * tf.square(self.td_error)

        weighted_error = tf.reduce_mean(importance_weights * errors)

        self.loss = weighted_error

        # for policy gradient
        self.actor_loss = -1.0 * tf.reduce_mean(self.q_tp0)


class DDPGGraph(object):
    def __init__(self, registry, env, config, logdir):
        self.env = env
        dim_actions = env.action_space.shape[0]
        low_action = env.action_space.low
        high_action = env.action_space.high
        actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["actor_lr"])
        critic_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["critic_lr"])

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None, ) + env.observation_space.shape)

        # Actor: P (policy) network
        p_scope_name = "p_func"
        with tf.variable_scope(p_scope_name) as scope:
            p_values = _build_p_network(registry, self.cur_observations,
                                        dim_actions, config)
            p_func_vars = _scope_vars(scope.name)

        # Action outputs
        a_scope_name = "a_func"
        with tf.variable_scope(a_scope_name):
            self.output_actions = _build_action_network(
                p_values, low_action, high_action, self.stochastic, self.eps,
                config["exploration_theta"], config["exploration_sigma"])

        with tf.variable_scope(a_scope_name, reuse=True):
            exploration_sample = tf.get_variable(name="ornstein_uhlenbeck")
            self.reset_noise_op = tf.assign(exploration_sample,
                                            dim_actions * [.0])

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32,
            shape=(None, ) + env.observation_space.shape,
            name="observation")
        self.act_t = tf.placeholder(
            tf.float32, shape=(None, ) + env.action_space.shape, name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None, ) + env.observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")

        def build_loss(obs_t, act_t, rew_t, obs_tp1, done_mask,
                       importance_weights):
            return ModelAndLoss(registry, dim_actions, low_action, high_action,
                                config, obs_t, act_t, rew_t, obs_tp1,
                                done_mask, importance_weights)

        self.loss_inputs = [
            ("obs", self.obs_t),
            ("actions", self.act_t),
            ("rewards", self.rew_t),
            ("new_obs", self.obs_tp1),
            ("dones", self.done_mask),
            ("weights", self.importance_weights),
        ]

        loss_obj = build_loss(self.obs_t, self.act_t, self.rew_t, self.obs_tp1,
                              self.done_mask, self.importance_weights)

        self.build_loss = build_loss

        actor_loss = loss_obj.actor_loss
        weighted_error = loss_obj.loss
        q_func_vars = loss_obj.q_func_vars
        target_p_func_vars = loss_obj.target_p_func_vars
        target_q_func_vars = loss_obj.target_q_func_vars
        self.p_t = loss_obj.p_t
        self.q_t = loss_obj.q_t
        self.q_tp0 = loss_obj.q_tp0
        self.q_tp1 = loss_obj.q_tp1
        self.td_error = loss_obj.td_error

        if config["l2_reg"] is not None:
            for var in p_func_vars:
                if "bias" not in var.name:
                    actor_loss += config["l2_reg"] * 0.5 * tf.nn.l2_loss(var)
            for var in q_func_vars:
                if "bias" not in var.name:
                    weighted_error += config["l2_reg"] * 0.5 * tf.nn.l2_loss(
                        var)

        # compute optimization op (potentially with gradient clipping)
        if config["grad_norm_clipping"] is not None:
            self.actor_grads_and_vars = _minimize_and_clip(
                actor_optimizer,
                actor_loss,
                var_list=p_func_vars,
                clip_val=config["grad_norm_clipping"])
            self.critic_grads_and_vars = _minimize_and_clip(
                critic_optimizer,
                weighted_error,
                var_list=q_func_vars,
                clip_val=config["grad_norm_clipping"])
        else:
            self.actor_grads_and_vars = actor_optimizer.compute_gradients(
                actor_loss, var_list=p_func_vars)
            self.critic_grads_and_vars = critic_optimizer.compute_gradients(
                weighted_error, var_list=q_func_vars)
        self.actor_grads_and_vars = [(g, v)
                                     for (g, v) in self.actor_grads_and_vars
                                     if g is not None]
        self.critic_grads_and_vars = [(g, v)
                                      for (g, v) in self.critic_grads_and_vars
                                      if g is not None]
        self.grads_and_vars = (
            self.actor_grads_and_vars + self.critic_grads_and_vars)
        self.grads = [g for (g, v) in self.grads_and_vars]
        self.actor_train_expr = actor_optimizer.apply_gradients(
            self.actor_grads_and_vars)
        self.critic_train_expr = critic_optimizer.apply_gradients(
            self.critic_grads_and_vars)

        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        self.tau_value = config.get("tau")
        self.tau = tf.placeholder(tf.float32, (), name="tau")
        update_target_expr = []
        for var, var_target in zip(
                sorted(q_func_vars, key=lambda v: v.name),
                sorted(target_q_func_vars, key=lambda v: v.name)):
            update_target_expr.append(
                var_target.assign(self.tau * var +
                                  (1.0 - self.tau) * var_target))
        for var, var_target in zip(
                sorted(p_func_vars, key=lambda v: v.name),
                sorted(target_p_func_vars, key=lambda v: v.name)):
            update_target_expr.append(
                var_target.assign(self.tau * var +
                                  (1.0 - self.tau) * var_target))
        self.update_target_expr = tf.group(*update_target_expr)

    # support both hard and soft sync
    def update_target(self, sess, tau=None):
        return sess.run(
            self.update_target_expr,
            feed_dict={self.tau: tau or self.tau_value})

    def act(self, sess, obs, eps, stochastic=True):
        return sess.run(
            self.output_actions,
            feed_dict={
                self.cur_observations: obs,
                self.stochastic: stochastic,
                self.eps: eps
            })

    def compute_gradients(self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
                          importance_weights):
        td_err, grads = sess.run(
            [self.td_error, self.grads],
            feed_dict={
                self.obs_t: obs_t,
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: obs_tp1,
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return td_err, grads

    def compute_td_error(self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
                         importance_weights):
        td_err = sess.run(
            self.td_error,
            feed_dict={
                self.obs_t: [np.array(ob) for ob in obs_t],
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: [np.array(ob) for ob in obs_tp1],
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return td_err

    def apply_gradients(self, sess, grads):
        assert len(grads) == len(self.grads_and_vars)
        feed_dict = {ph: g for (g, ph) in zip(grads, self.grads)}
        sess.run(
            [self.critic_train_expr, self.actor_train_expr],
            feed_dict=feed_dict)

    def compute_apply(self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
                      importance_weights):
        td_err, _, _ = sess.run(
            [self.td_error, self.critic_train_expr, self.actor_train_expr],
            feed_dict={
                self.obs_t: obs_t,
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: obs_tp1,
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return td_err

    def reset_noise(self, sess):
        sess.run(self.reset_noise_op)
