from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.layers as layers

from ray.rllib.models import ModelCatalog
from ray.rllib.parallel import LocalSyncParallelOptimizer, TOWER_SCOPE_NAME


def _build_q_network(inputs, num_actions, config):
    dueling = config["dueling"]
    hiddens = config["hiddens"]
    frontend = ModelCatalog.get_model(inputs, 1, config["model"])
    frontend_out = frontend.last_layer

    with tf.variable_scope("action_value"):
        action_out = frontend_out
        for hidden in hiddens:
            action_out = layers.fully_connected(
                action_out, num_outputs=hidden, activation_fn=tf.nn.relu)
        action_scores = layers.fully_connected(
            action_out, num_outputs=num_actions, activation_fn=None)

    if dueling:
        with tf.variable_scope("state_value"):
            state_out = frontend_out
            for hidden in hiddens:
                state_out = layers.fully_connected(
                    state_out, num_outputs=hidden, activation_fn=tf.nn.relu)
            state_score = layers.fully_connected(
                state_out, num_outputs=1, activation_fn=None)
        action_scores_mean = tf.reduce_mean(action_scores, 1)
        action_scores_centered = action_scores - tf.expand_dims(
            action_scores_mean, 1)
        return state_score + action_scores_centered
    else:
        return action_scores


def _build_action_network(
        q_values, observations, num_actions, stochastic, eps):
    deterministic_actions = tf.argmax(q_values, axis=1)
    batch_size = tf.shape(observations)[0]
    random_actions = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=num_actions, dtype=tf.int64)
    chose_random = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=1, dtype=tf.float32) < eps
    stochastic_actions = tf.where(
        chose_random, random_actions, deterministic_actions)
    return tf.cond(
        stochastic, lambda: stochastic_actions,
        lambda: deterministic_actions)


def _huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return tf.where(
        tf.abs(x) < delta,
        tf.square(x) * 0.5,
        delta * (tf.abs(x) - 0.5 * delta))


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

    def __init__(
            self, num_actions, config,
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
        # q network evaluation
        with tf.variable_scope("q_func", reuse=True):
            self.q_t = _build_q_network(obs_t, num_actions, config)

        # target q network evalution
        with tf.variable_scope("target_q_func") as scope:
            self.q_tp1 = _build_q_network(obs_tp1, num_actions, config)
            self.target_q_func_vars = _scope_vars(scope.name)

        # q scores for actions which we know were selected in the given state.
        q_t_selected = tf.reduce_sum(
            self.q_t * tf.one_hot(act_t, num_actions), 1)

        # compute estimate of best possible value starting from state at t + 1
        if config["double_q"]:
            with tf.variable_scope("q_func", reuse=True):
                q_tp1_using_online_net = _build_q_network(
                    obs_tp1, num_actions, config)
            q_tp1_best_using_online_net = tf.argmax(q_tp1_using_online_net, 1)
            q_tp1_best = tf.reduce_sum(
                self.q_tp1 * tf.one_hot(
                    q_tp1_best_using_online_net, num_actions), 1)
        else:
            q_tp1_best = tf.reduce_max(self.q_tp1, 1)
        q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = rew_t + config["gamma"] * q_tp1_best_masked

        # compute the error (potentially clipped)
        self.td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
        errors = _huber_loss(self.td_error)

        weighted_error = tf.reduce_mean(importance_weights * errors)

        self.loss = weighted_error


class DQNGraph(object):
    def __init__(self, env, config, logdir):
        self.env = env
        num_actions = env.action_space.n
        optimizer = tf.train.AdamOptimizer(learning_rate=config["lr"])

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)

        # Action Q network
        if config["multi_gpu_optimize"]:
            q_scope_name = TOWER_SCOPE_NAME + "/q_func"
        else:
            q_scope_name = "q_func"
        with tf.variable_scope(q_scope_name) as scope:
            q_values = _build_q_network(
                self.cur_observations, num_actions, config)
            q_func_vars = _scope_vars(scope.name)

        # Action outputs
        self.output_actions = _build_action_network(
            q_values,
            self.cur_observations,
            num_actions,
            self.stochastic,
            self.eps)

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)
        self.act_t = tf.placeholder(tf.int32, [None], name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")

        def build_loss(
                obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
            return ModelAndLoss(
                num_actions, config,
                obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights)

        if config["multi_gpu_optimize"]:
            self.multi_gpu_optimizer = LocalSyncParallelOptimizer(
                optimizer,
                config["devices"],
                [self.obs_t, self.act_t, self.rew_t, self.obs_tp1,
                 self.done_mask, self.importance_weights],
                int(config["sgd_batch_size"] / len(config["devices"])),
                build_loss,
                logdir,
                grad_norm_clipping=config["grad_norm_clipping"])
            loss_obj = self.multi_gpu_optimizer.get_common_loss()
        else:
            loss_obj = build_loss(
                self.obs_t, self.act_t, self.rew_t, self.obs_tp1,
                self.done_mask, self.importance_weights)

        weighted_error = loss_obj.loss
        target_q_func_vars = loss_obj.target_q_func_vars
        self.q_t = loss_obj.q_t
        self.q_tp1 = loss_obj.q_tp1
        self.td_error = loss_obj.td_error

        # compute optimization op (potentially with gradient clipping)
        if config["grad_norm_clipping"] is not None:
            self.grads_and_vars = _minimize_and_clip(
                optimizer, weighted_error, var_list=q_func_vars,
                clip_val=config["grad_norm_clipping"])
        else:
            self.grads_and_vars = optimizer.compute_gradients(
                weighted_error, var_list=q_func_vars)
        self.grads_and_vars = [
            (g, v) for (g, v) in self.grads_and_vars if g is not None]
        self.grads = [g for (g, v) in self.grads_and_vars]
        self.train_expr = optimizer.apply_gradients(self.grads_and_vars)

        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        update_target_expr = []
        for var, var_target in zip(
            sorted(q_func_vars, key=lambda v: v.name),
                sorted(target_q_func_vars, key=lambda v: v.name)):
            update_target_expr.append(var_target.assign(var))
        self.update_target_expr = tf.group(*update_target_expr)

    def update_target(self, sess):
        return sess.run(self.update_target_expr)

    def act(self, sess, obs, eps, stochastic=True):
        return sess.run(
            self.output_actions,
            feed_dict={
                self.cur_observations: obs,
                self.stochastic: stochastic,
                self.eps: eps,
            })

    def compute_gradients(
            self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
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

    def compute_td_error(
            self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
            importance_weights):
        td_err = sess.run(
            self.td_error,
            feed_dict={
                self.obs_t: obs_t,
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: obs_tp1,
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return td_err

    def apply_gradients(self, sess, grads):
        assert len(grads) == len(self.grads_and_vars)
        feed_dict = {ph: g for (g, ph) in zip(grads, self.grads)}
        sess.run(self.train_expr, feed_dict=feed_dict)
