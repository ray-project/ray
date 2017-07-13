from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.layers as layers

from ray.rllib.models import ModelCatalog


def _build_q_network(inputs, num_actions, hiddens=[256], dueling=True):
  frontend = ModelCatalog.get_model(inputs, 1)
  frontend_out = frontend.last_layer()

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
    q_values, observations, num_actions, eps, stochastic):
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


class DQNGraph(object):
  def __init__(self, env, config):
    self.env = env
    num_actions = env.action_space.n

    # Action inputs
    self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
    self.eps = tf.placeholder(tf.float32, (), name="eps")
    self.cur_observations = tf.placeholder(
      tf.float32, shape=(None,) + env.observation_shape.shape)

    # Action Q network
    with tf.variable_scope("q_func") as scope:
      q_values = _build_q_network(self.cur_observations, num_actions)
      q_func_vars = U.scope_vars(scope.name)

    # Action outputs
    self.output_actions = _build_action_network(
        q_values,
        self.cur_observations,
        num_actions,
        self.stochastic,
        self.eps)

    # Replay inputs
    self.obs_t_ph = tf.placeholder(
      tf.float32, shape=(None,) + env.observation_shape.shape)
    self.act_t_ph = tf.placeholder(tf.int32, [None], name="action")
    self.rew_t_ph = tf.placeholder(tf.float32, [None], name="reward")
    self.obs_tp1_ph = tf.placeholder(
      tf.float32, shape=(None,) + env.observation_shape.shape)
    self.done_mask_ph = tf.placeholder(tf.float32, [None], name="done")
    self.importance_weights_ph = tf.placeholder(
        tf.float32, [None], name="weight")

    # q network evaluation
    with tf.variable_scope("q_func", reuse=True):
      self.q_t = q_func(self.obs_t_ph, num_actions)

    # target q network evalution
    with tf.variable_scope("target_q_func") as scope:
      target_q_values = _build_q_network(self.obs_tp1_ph, num_actions)
      target_q_func_vars = U.scope_vars(scope.name)

    # q scores for actions which we know were selected in the given state.
    q_t_selected = tf.reduce_sum(
        self.q_t * tf.one_hot(self.act_t_ph, num_actions), 1)

    # compute estimate of best possible value starting from state at t + 1
    if double_q:
      with tf.variable_scope("q_func", reuse=True):
        q_tp1_using_online_net = _build_q_network(self.obs_tp1_ph, num_actions)
      q_tp1_best_using_online_net = tf.arg_max(q_tp1_using_online_net, 1)
      q_tp1_best = tf.reduce_sum(
          q_tp1 * tf.one_hot(q_tp1_best_using_online_net, num_actions), 1)
    else:
      q_tp1_best = tf.reduce_max(q_tp1, 1)
    q_tp1_best_masked = (1.0 - done_mask_ph) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = rew_t_ph + config["gamma"] * q_tp1_best_masked

    # compute the error (potentially clipped)
    td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
    errors = U.huber_loss(td_error)
    weighted_error = tf.reduce_mean(importance_weights_ph * errors)
    # compute optimization op (potentially with gradient clipping)
    if config["grad_norm_clipping"] is not None:
      self.optimize_expr = U.minimize_and_clip(
          optimizer, weighted_error, var_list=q_func_vars,
          clip_val=grad_norm_clipping)
    else:
      self.optimize_expr = optimizer.minimize(
          weighted_error, var_list=q_func_vars)

    # update_target_fn will be called periodically to copy Q network to
    # target Q network
    update_target_expr = []
    for var, var_target in zip(
        sorted(q_func_vars, key=lambda v: v.name),
            sorted(target_q_func_vars, key=lambda v: v.name)):
      update_target_expr.append(var_target.assign(var))
    self.update_target_expr = tf.group(*update_target_expr)

  def act(self, obs, eps, stochastic=True):
    return sess.run(
        self.output_actions,
        feed_dict={
          self.cur_observations: obs,
          self.stochastic_ph: stochastic,
          self.eps: eps,
        })

  def train(
      self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
    td_err, _ = sess.run(
        [td_error, optimize_expr],
        feed_dict={
            self.obs_t_ph: obs_t,
            self.act_t_ph: act_t,
            self.rew_t_ph: rew_t,
            self.done_mask_ph: done_mask,
            self.importance_weights_ph: importance_weights
        })
    return td_err

  def q_values(self, sess, obs):
    return sess.run(self.q_t, feed_dict={self.obs_t_ph: obs})
