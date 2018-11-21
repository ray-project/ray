from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Box
import numpy as np
import tensorflow as tf
import tensorflow.contrib.layers as layers

import ray
from ray.rllib.agents.dqn.dqn_policy_graph import _huber_loss, \
    _minimize_and_clip, _scope_vars, _postprocess_dqn
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph

A_SCOPE = "a_func"
P_SCOPE = "p_func"
P_TARGET_SCOPE = "target_p_func"
Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"


class PNetwork(object):
    """Maps an observations (i.e., state) to an action where each entry takes
    value from (0, 1) due to the sigmoid function."""

    def __init__(self, model, dim_actions, hiddens=[64, 64],
                 activation="relu"):
        action_out = model.last_layer
        activation = tf.nn.__dict__[activation]
        for hidden in hiddens:
            action_out = layers.fully_connected(
                action_out, num_outputs=hidden, activation_fn=activation)
        # Use sigmoid layer to bound values within (0, 1)
        # shape of action_scores is [batch_size, dim_actions]
        self.action_scores = layers.fully_connected(
            action_out, num_outputs=dim_actions, activation_fn=tf.nn.sigmoid)


class ActionNetwork(object):
    """Acts as a stochastic policy for inference, but a deterministic policy
    for training, thus ignoring the batch_size issue when constructing a
    stochastic action."""

    def __init__(self,
                 p_values,
                 low_action,
                 high_action,
                 stochastic,
                 eps,
                 theta=0.15,
                 sigma=0.2,
                 use_gaussian_noise=False,
                 act_noise=0.1,
                 is_target=False,
                 target_noise=0.2,
                 noise_clip=0.5):

        # shape is [None, dim_action]
        deterministic_actions = (
            (high_action - low_action) * p_values + low_action)

        if use_gaussian_noise:
            if is_target:
                normal_sample = tf.random_normal(
                    tf.shape(deterministic_actions), stddev=target_noise)
                normal_sample = tf.clip_by_value(normal_sample, -noise_clip,
                                                 noise_clip)
                stochastic_actions = tf.clip_by_value(
                    deterministic_actions + normal_sample, low_action,
                    high_action)
            else:
                normal_sample = tf.random_normal(
                    tf.shape(deterministic_actions), stddev=act_noise)
                stochastic_actions = tf.clip_by_value(
                    deterministic_actions + normal_sample, low_action,
                    high_action)
        else:
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

        self.actions = tf.cond(stochastic, lambda: stochastic_actions,
                               lambda: deterministic_actions)


class QNetwork(object):
    def __init__(self,
                 model,
                 action_inputs,
                 hiddens=[64, 64],
                 activation="relu"):
        q_out = tf.concat([model.last_layer, action_inputs], axis=1)
        activation = tf.nn.__dict__[activation]
        for hidden in hiddens:
            q_out = layers.fully_connected(
                q_out, num_outputs=hidden, activation_fn=activation)
        self.value = layers.fully_connected(
            q_out, num_outputs=1, activation_fn=None)
        self.model = model


class ActorCriticLoss(object):
    def __init__(self,
                 q_t,
                 q_tp1,
                 q_tp0,
                 importance_weights,
                 rewards,
                 done_mask,
                 twin_q_t,
                 twin_q_tp1,
                 global_step,
                 gamma=0.99,
                 n_step=1,
                 use_huber=False,
                 huber_threshold=1.0,
                 twin_q=False,
                 policy_delay=1):

        q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
        if twin_q:
            twin_q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
            q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

        q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
        q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = rewards + gamma**n_step * q_tp1_best_masked

        # compute the error (potentially clipped)
        if twin_q:
            td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
            twin_td_error = twin_q_t_selected - tf.stop_gradient(
                q_t_selected_target)
            self.td_error = td_error + twin_td_error
            if use_huber:
                errors = _huber_loss(td_error, huber_threshold) + _huber_loss(
                    twin_td_error, huber_threshold)
            else:
                errors = 0.5 * tf.square(td_error) + 0.5 * tf.square(
                    twin_td_error)
        else:
            self.td_error = q_t_selected - tf.stop_gradient(
                q_t_selected_target)
            if use_huber:
                errors = _huber_loss(self.td_error, huber_threshold)
            else:
                errors = 0.5 * tf.square(self.td_error)

        self.critic_loss = tf.reduce_mean(importance_weights * errors)

        # for policy gradient
        self.actor_loss = -1.0 * tf.reduce_mean(q_tp0)

        # update policy net one time v.s. update critic net `policy_delay` time(s)
        actor_loss_coeff = tf.to_float(
            tf.equal(tf.mod(global_step, policy_delay), 0))
        self.total_loss = actor_loss_coeff * self.actor_loss + self.critic_loss


class DDPGPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.ddpg.ddpg.DEFAULT_CONFIG, **config)
        if not isinstance(action_space, Box):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DDPG.".format(
                    action_space))

        self.config = config
        self.cur_epsilon = 1.0
        self.dim_actions = action_space.shape[0]
        self.low_action = action_space.low
        self.high_action = action_space.high
        self.actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["actor_lr"])
        self.critic_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["critic_lr"])

        # create global step for counting the number of update operations
        self.global_step = tf.train.get_or_create_global_step()

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)

        # Actor: P (policy) network
        with tf.variable_scope(P_SCOPE) as scope:
            p_values = self._build_p_network(self.cur_observations,
                                             observation_space)
            self.p_func_vars = _scope_vars(scope.name)

        # Action outputs
        with tf.variable_scope(A_SCOPE):
            self.output_actions = self._build_action_network(
                p_values, self.stochastic, self.eps)

        if not self.config["smooth_target_policy"]:
            with tf.variable_scope(A_SCOPE, reuse=True):
                exploration_sample = tf.get_variable(name="ornstein_uhlenbeck")
                self.reset_noise_op = tf.assign(exploration_sample,
                                                self.dim_actions * [.0])

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32,
            shape=(None, ) + observation_space.shape,
            name="observation")
        self.act_t = tf.placeholder(
            tf.float32, shape=(None, ) + action_space.shape, name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")

        # p network evaluation
        with tf.variable_scope(P_SCOPE, reuse=True) as scope:
            self.p_t = self._build_p_network(self.obs_t, observation_space)

        # target p network evaluation
        with tf.variable_scope(P_TARGET_SCOPE) as scope:
            p_tp1 = self._build_p_network(self.obs_tp1, observation_space)
            target_p_func_vars = _scope_vars(scope.name)

        # Action outputs
        with tf.variable_scope(A_SCOPE, reuse=True):
            deterministic_flag = tf.constant(value=True, dtype=tf.bool)
            zero_eps = tf.constant(value=.0, dtype=tf.float32)
            output_actions = self._build_action_network(
                self.p_t, deterministic_flag, zero_eps)
            output_actions_estimated = self._build_action_network(
                p_tp1,
                tf.constant(value=False, dtype=tf.bool)
                if self.config["smooth_target_policy"] else deterministic_flag,
                zero_eps,
                is_target=True)

        # q network evaluation
        with tf.variable_scope(Q_SCOPE) as scope:
            q_t, model = self._build_q_network(self.obs_t, observation_space,
                                               self.act_t)
            self.q_func_vars = _scope_vars(scope.name)
        with tf.variable_scope(Q_SCOPE, reuse=True):
            q_tp0, _ = self._build_q_network(self.obs_t, observation_space,
                                             output_actions)
        if self.config["twin_q"]:
            with tf.variable_scope("twin_" + Q_SCOPE) as scope:
                twin_q_t, twin_model = self._build_q_network(
                    self.obs_t, observation_space, self.act_t)
                self.twin_q_func_vars = _scope_vars(scope.name)

        # target q network evalution
        with tf.variable_scope(Q_TARGET_SCOPE) as scope:
            q_tp1, _ = self._build_q_network(self.obs_tp1, observation_space,
                                             output_actions_estimated)
            target_q_func_vars = _scope_vars(scope.name)
        if self.config["twin_q"]:
            with tf.variable_scope("twin_" + Q_TARGET_SCOPE) as scope:
                twin_q_tp1, _ = self._build_q_network(
                    self.obs_tp1, observation_space, output_actions_estimated)
                twin_target_q_func_vars = _scope_vars(scope.name)

        if self.config["twin_q"]:
            self.loss = self._build_actor_critic_loss(
                q_t, q_tp1, q_tp0, twin_q_t=twin_q_t, twin_q_tp1=twin_q_tp1)
        else:
            self.loss = self._build_actor_critic_loss(q_t, q_tp1, q_tp0)

        if config["l2_reg"] is not None:
            for var in self.p_func_vars:
                if "bias" not in var.name:
                    self.loss.actor_loss += (
                        config["l2_reg"] * 0.5 * tf.nn.l2_loss(var))
            for var in self.q_func_vars:
                if "bias" not in var.name:
                    self.loss.critic_loss += (
                        config["l2_reg"] * 0.5 * tf.nn.l2_loss(var))
            if self.config["twin_q"]:
                for var in self.twin_q_func_vars:
                    if "bias" not in var.name:
                        self.loss.critic_loss += (
                            config["l2_reg"] * 0.5 * tf.nn.l2_loss(var))

        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        self.tau_value = config.get("tau")
        self.tau = tf.placeholder(tf.float32, (), name="tau")
        update_target_expr = []
        for var, var_target in zip(
                sorted(self.q_func_vars, key=lambda v: v.name),
                sorted(target_q_func_vars, key=lambda v: v.name)):
            update_target_expr.append(
                var_target.assign(self.tau * var +
                                  (1.0 - self.tau) * var_target))
        if self.config["twin_q"]:
            for var, var_target in zip(
                    sorted(self.twin_q_func_vars, key=lambda v: v.name),
                    sorted(twin_target_q_func_vars, key=lambda v: v.name)):
                update_target_expr.append(
                    var_target.assign(self.tau * var +
                                      (1.0 - self.tau) * var_target))
        for var, var_target in zip(
                sorted(self.p_func_vars, key=lambda v: v.name),
                sorted(target_p_func_vars, key=lambda v: v.name)):
            update_target_expr.append(
                var_target.assign(self.tau * var +
                                  (1.0 - self.tau) * var_target))
        self.update_target_expr = tf.group(*update_target_expr)

        self.sess = tf.get_default_session()
        self.loss_inputs = [
            ("obs", self.obs_t),
            ("actions", self.act_t),
            ("rewards", self.rew_t),
            ("new_obs", self.obs_tp1),
            ("dones", self.done_mask),
            ("weights", self.importance_weights),
        ]
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=self.cur_observations,
            action_sampler=self.output_actions,
            loss=model.loss() + self.loss.total_loss,
            loss_inputs=self.loss_inputs)
        self.sess.run(tf.global_variables_initializer())

        # Note that this encompasses both the policy and Q-value networks and
        # their corresponding target networks
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(q_tp0, q_tp1), self.sess)

        # Hard initial update
        self.update_target(tau=1.0)

    def _build_q_network(self, obs, obs_space, actions):
        q_net = QNetwork(
            ModelCatalog.get_model({
                "obs": obs
            }, obs_space, 1, self.config["model"]), actions,
            self.config["critic_hiddens"],
            self.config["critic_hidden_activation"])
        return q_net.value, q_net.model

    def _build_p_network(self, obs, obs_space):
        return PNetwork(
            ModelCatalog.get_model({
                "obs": obs
            }, obs_space, 1, self.config["model"]), self.dim_actions,
            self.config["actor_hiddens"],
            self.config["actor_hidden_activation"]).action_scores

    def _build_action_network(self, p_values, stochastic, eps,
                              is_target=False):
        return ActionNetwork(
            p_values, self.low_action, self.high_action, stochastic, eps,
            self.config["exploration_theta"], self.config["exploration_sigma"],
            self.config["smooth_target_policy"], self.config["act_noise"],
            is_target, self.config["target_noise"],
            self.config["noise_clip"]).actions

    def _build_actor_critic_loss(self,
                                 q_t,
                                 q_tp1,
                                 q_tp0,
                                 twin_q_t=None,
                                 twin_q_tp1=None):
        return ActorCriticLoss(
            q_t, q_tp1, q_tp0, self.importance_weights, self.rew_t,
            self.done_mask, twin_q_t, twin_q_tp1, self.global_step,
            self.config["gamma"], self.config["n_step"],
            self.config["use_huber"], self.config["huber_threshold"],
            self.config["twin_q"])

    def gradients(self, optimizer):
        if self.config["grad_norm_clipping"] is not None:
            actor_grads_and_vars = _minimize_and_clip(
                self.actor_optimizer,
                self.loss.actor_loss,
                var_list=self.p_func_vars,
                clip_val=self.config["grad_norm_clipping"])
            critic_grads_and_vars = _minimize_and_clip(
                self.critic_optimizer,
                self.loss.critic_loss,
                var_list=self.q_func_vars + self.twin_q_func_vars
                if self.config["twin_q"] else self.q_func_vars,
                clip_val=self.config["grad_norm_clipping"])
        else:
            actor_grads_and_vars = self.actor_optimizer.compute_gradients(
                self.loss.actor_loss, var_list=self.p_func_vars)
            critic_grads_and_vars = self.critic_optimizer.compute_gradients(
                self.loss.critic_loss,
                var_list=self.q_func_vars + self.twin_q_func_vars
                if self.config["twin_q"] else self.q_func_vars)
        actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                if g is not None]
        critic_grads_and_vars = [(g, v) for (g, v) in critic_grads_and_vars
                                 if g is not None]
        return critic_grads_and_vars + actor_grads_and_vars

    def extra_compute_action_feed_dict(self):
        return {
            self.stochastic: True,
            self.eps: self.cur_epsilon,
        }

    def extra_compute_grad_fetches(self):
        return {
            "td_error": self.loss.td_error,
        }

    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        return _postprocess_dqn(self, sample_batch)

    def compute_td_error(self, obs_t, act_t, rew_t, obs_tp1, done_mask,
                         importance_weights):
        td_err = self.sess.run(
            self.loss.td_error,
            feed_dict={
                self.obs_t: [np.array(ob) for ob in obs_t],
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: [np.array(ob) for ob in obs_tp1],
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return td_err

    def reset_noise(self, sess):
        if not self.config["use_gaussian_noise"]:
            sess.run(self.reset_noise_op)

    # support both hard and soft sync
    def update_target(self, tau=None):
        return self.sess.run(
            self.update_target_expr,
            feed_dict={self.tau: tau or self.tau_value})

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def get_state(self):
        return [TFPolicyGraph.get_state(self), self.cur_epsilon]

    def set_state(self, state):
        TFPolicyGraph.set_state(self, state[0])
        self.set_epsilon(state[1])
