from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.layers as layers

from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers.multi_gpu_impl import TOWER_SCOPE_NAME

def _build_p_network(registry, inputs, num_actions, config):
    hiddens = config["actor_hiddens"]

    with tf.variable_scope("action_value"):
        action_out = inputs
        for hidden in hiddens:
            action_out = layers.fully_connected(
                action_out, num_outputs=hidden, activation_fn=tf.nn.relu)
        # The final output layer of the actor was a tanh layer, to bound the actions
        # (batch_size, num_actions)
        action_scores = layers.fully_connected(
            action_out, num_outputs=num_actions, activation_fn=tf.nn.tanh)

    return action_scores

# As a stochastic policy for inference
# but a deterministic policy for training
# thus ignore batch_size issue when constructing a stochastic action
def _build_action_network(
        p_values, low_action, high_action, stochastic, noise_scale, theta, sigma):
    deterministic_actions = high_action * p_values

    #batch_size = tf.shape(p_values)[0]
    exploration_sample = tf.get_variable(name="ornstein_uhlenbeck", dtype=tf.float32, \
                         initializer=low_action.size*[.0], trainable=False)
    normal_sample = tf.random_normal(shape=[low_action.size], mean=0.0, stddev=1.0)
    exploration_value = tf.assign_add(exploration_sample, \
                         theta * (.0-exploration_sample) + sigma * normal_sample)
    stochastic_actions = deterministic_actions + noise_scale * exploration_value

    return tf.cond(
        stochastic, lambda: stochastic_actions,
        lambda: deterministic_actions)

def _build_q_network(registry, state_inputs, action_inputs, config):
    hiddens = config["critic_hiddens"]

    with tf.variable_scope("action_value"):
        q_out = tf.concat([state_inputs, action_inputs], axis=1)
        for hidden in hiddens:
            q_out = layers.fully_connected(
                q_out, num_outputs=hidden, activation_fn=tf.nn.relu)
        q_scores = layers.fully_connected(
            q_out, num_outputs=1, activation_fn=None)

    return q_scores

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
            self, registry, num_actions, low_action, high_action, config,
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
        # p network evaluation
        with tf.variable_scope("p_func", reuse=True) as scope:
            self.p_t = _build_p_network(registry, obs_t, num_actions, config)
            p_func_vars = _scope_vars(scope.name)
            print("###### ensure identical to p_func scope ######")
            for var in p_func_vars:
                print(var.name)
            raw_input()


        # target p network evaluation
        with tf.variable_scope("target_p_func") as scope:
            self.p_tp1 = _build_p_network(registry, obs_tp1, num_actions, config)
            self.target_p_func_vars = _scope_vars(scope.name)
            for var in self.target_p_func_vars:
                print(var.name)
            raw_input()

        # Action outputs
        with tf.variable_scope("a_func", reuse=True):
            output_actions = _build_action_network(
                self.p_t,
                low_action,
                high_action,
                tf.constant(value=False, dtype=tf.bool),
                tf.constant(value=.0, dtype=tf.float32),
                config["exploration_theta"],
                config["exploration_sigma"])

            output_actions_estimated = _build_action_network(
                self.p_tp1,
                low_action,
                high_action,
                tf.constant(value=False, dtype=tf.bool),
                tf.constant(value=.0, dtype=tf.float32),
                config["exploration_theta"],
                config["exploration_sigma"])

        # q network evaluation
        with tf.variable_scope("q_func", reuse=True) as scope:
            self.q_t = _build_q_network(registry, obs_t, act_t, config)
            self.q_tp0 = _build_q_network(registry, obs_t, output_actions, config)
            q_func_vars = _scope_vars(scope.name)
            print("###### ensure identical to q_func scope ######")
            for var in q_func_vars:
                print(var.name)
            raw_input()


        # target q network evalution
        with tf.variable_scope("target_q_func") as scope:
            self.q_tp1 = _build_q_network(registry, obs_tp1, output_actions_estimated, config)
            self.target_q_func_vars = _scope_vars(scope.name)
            for var in self.target_q_func_vars:
                print(var.name)
            raw_input()


        # q scores for actions which we know were selected in the given state.
        #q_t_selected = tf.reduce_sum(
        #    self.q_t * tf.one_hot(act_t, num_actions), 1)
        q_t_selected = self.q_t

        '''
        # compute estimate of best possible value starting from state at t + 1
        if config["double_q"]:
            with tf.variable_scope("q_func", reuse=True):
                q_tp1_using_online_net = _build_q_network(
                    registry, obs_tp1, num_actions, config)
            q_tp1_best_using_online_net = tf.argmax(q_tp1_using_online_net, 1)
            q_tp1_best = tf.reduce_sum(
                self.q_tp1 * tf.one_hot(
                    q_tp1_best_using_online_net, num_actions), 1)
        else:
            q_tp1_best = tf.reduce_max(self.q_tp1, 1)
        '''
        # deterministic policy
        q_tp1_best = self.q_tp1
        q_tp1_best_masked = (1.0 - tf.expand_dims(done_mask, 1)) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = (
            tf.expand_dims(rew_t, 1) + config["gamma"] ** config["n_step"] * q_tp1_best_masked)

        # compute the error (potentially clipped)
        self.td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
        #errors = _huber_loss(self.td_error)
        errors = 0.5 * tf.square(self.td_error)

        weighted_error = tf.reduce_mean(importance_weights * errors)

        self.loss = weighted_error

        # for policy gradient
        self.actor_loss = -1.0 * tf.reduce_mean(self.q_tp0)


class DDPGGraph(object):
    def __init__(self, registry, env, config, logdir):
        self.env = env
        num_actions = env.action_space.shape[0]
        low_action = env.action_space.low
        high_action = env.action_space.high
        actor_optimizer = tf.train.AdamOptimizer(learning_rate=config["actor_lr"])
        critic_optimizer = tf.train.AdamOptimizer(learning_rate=config["critic_lr"])

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.noise_scale = tf.placeholder(tf.float32, (), name="noise_scale")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape, name="observation_for_infer")

        # Action P (policy) network
        p_scope_name = TOWER_SCOPE_NAME + "/p_func"
        with tf.variable_scope(p_scope_name) as scope:
            p_values = _build_p_network(
                registry, self.cur_observations, num_actions, config)
            p_func_vars = _scope_vars(scope.name)
            for var in p_func_vars:
                print(var.name)
            raw_input()


        # Action outputs
        a_scope_name = TOWER_SCOPE_NAME + "/a_func"
        with tf.variable_scope(a_scope_name):
            self.output_actions = _build_action_network(
                p_values,
                low_action,
                high_action,
                self.stochastic,
                self.noise_scale,
                config["exploration_theta"],
                config["exploration_sigma"])

        with tf.variable_scope(a_scope_name, reuse=True):
            exploration_sample = tf.get_variable(name="ornstein_uhlenbeck")
            self.reset_noise_op = tf.assign(exploration_sample, num_actions*[.0])


        # Action Q network
        q_scope_name = TOWER_SCOPE_NAME + "/q_func"
        with tf.variable_scope(q_scope_name) as scope:
            q_values = _build_q_network(registry, self.cur_observations, self.output_actions, config)
            q_func_vars = _scope_vars(scope.name)
            for var in q_func_vars:
                print(var.name)
            raw_input()

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape, name="observation")
        self.act_t = tf.placeholder(tf.float32, shape=(None,) + env.action_space.shape, name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")

        def build_loss(
                obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
            return ModelAndLoss(
                registry,
                num_actions, low_action, high_action, config,
                obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights)

        self.loss_inputs = [
            ("obs", self.obs_t),
            ("actions", self.act_t),
            ("rewards", self.rew_t),
            ("new_obs", self.obs_tp1),
            ("dones", self.done_mask),
            ("weights", self.importance_weights),
        ]

        with tf.variable_scope(TOWER_SCOPE_NAME):
            loss_obj = build_loss(
                self.obs_t, self.act_t, self.rew_t, self.obs_tp1,
                self.done_mask, self.importance_weights)

        self.build_loss = build_loss

        self.critic_loss = loss_obj.loss
        weighted_error = loss_obj.loss
        target_p_func_vars = loss_obj.target_p_func_vars
        target_q_func_vars = loss_obj.target_q_func_vars
        self.p_t = loss_obj.p_t
        self.q_t = loss_obj.q_t
        self.q_tp0 = loss_obj.q_tp0
        self.q_tp1 = loss_obj.q_tp1
        self.td_error = loss_obj.td_error

        actor_loss = loss_obj.actor_loss
        if config["actor_l2_reg"] is not None:
            for var in p_func_vars:
                if not "bias" in var.name:
                    actor_loss += config["actor_l2_reg"] * 0.5 * tf.nn.l2_loss(var)
        if config["critic_l2_reg"] is not None:
            for var in q_func_vars:
                if not "bias" in var.name:
                    weighted_error += config["critic_l2_reg"] * 0.5 * tf.nn.l2_loss(var)

        # compute optimization op (potentially with gradient clipping)
        if config["grad_norm_clipping"] is not None:
            self.actor_grads_and_vars = _minimize_and_clip(
                actor_optimizer, actor_loss, var_list=p_func_vars,
                clip_val=config["grad_norm_clipping"])
            self.critic_grads_and_vars = _minimize_and_clip(
                critic_optimizer, weighted_error, var_list=q_func_vars,
                clip_val=config["grad_norm_clipping"])
        else:
            self.actor_grads_and_vars = actor_optimizer.compute_gradients(
                actor_loss, var_list=p_func_vars)
            self.critic_grads_and_vars = critic_optimizer.compute_gradients(
                weighted_error, var_list=q_func_vars)
        self.actor_grads_and_vars = [
            (g, v) for (g, v) in self.actor_grads_and_vars if g is not None]
        self.critic_grads_and_vars = [
            (g, v) for (g, v) in self.critic_grads_and_vars if g is not None]
        self.actor_grads = [g for (g, v) in self.actor_grads_and_vars]
        self.critic_grads = [g for (g, v) in self.critic_grads_and_vars]
        self.actor_train_expr = actor_optimizer.apply_gradients(self.actor_grads_and_vars)
        self.critic_train_expr = critic_optimizer.apply_gradients(self.critic_grads_and_vars)

        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        tau = config.get("tau", 1.0)
        update_target_expr = []
        for var, var_target in zip(
            sorted(q_func_vars, key=lambda v: v.name),
                sorted(target_q_func_vars, key=lambda v: v.name)):
            update_target_expr.append(var_target.assign(tau*var+(1-tau)*var_target))
        for var, var_target in zip(
            sorted(p_func_vars, key=lambda v: v.name),
                sorted(target_p_func_vars, key=lambda v: v.name)):
            update_target_expr.append(var_target.assign(tau*var+(1-tau)*var_target))
        self.update_target_expr = tf.group(*update_target_expr)

        update_target_hard_expr = []
        for var, var_target in zip(
            sorted(q_func_vars, key=lambda v: v.name),
                sorted(target_q_func_vars, key=lambda v: v.name)):
            update_target_hard_expr.append(var_target.assign(var))
        for var, var_target in zip(
            sorted(p_func_vars, key=lambda v: v.name),
                sorted(target_p_func_vars, key=lambda v: v.name)):
            update_target_hard_expr.append(var_target.assign(var))
        self.update_target_hard_expr = tf.group(*update_target_hard_expr)


    def update_target(self, sess):
        return sess.run(self.update_target_expr)

    def update_target_hard(self, sess):
        return sess.run(self.update_target_hard_expr)

    def act(self, sess, obs, stochastic=True, noise_scale=0.1):
        return sess.run(
            self.output_actions,
            feed_dict={
                self.cur_observations: obs,
                self.stochastic: stochastic,
                self.noise_scale: noise_scale
            })

    def compute_critic_gradients(
            self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
            importance_weights):
        td_err, grads = sess.run(
            [self.td_error, self.critic_grads],
            feed_dict={
                self.obs_t: obs_t,
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: obs_tp1,
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return td_err, grads

    def compute_actor_gradients(self, sess, obs_t):
        return sess.run(
            self.actor_grads,
            feed_dict={
                self.obs_t: obs_t,
            })

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

    def apply_critic_gradients(self, sess, grads):
        assert len(grads) == len(self.critic_grads_and_vars)
        feed_dict = {ph: g for (g, ph) in zip(grads, self.critic_grads)}
        sess.run(self.critic_train_expr, feed_dict=feed_dict)

    def apply_actor_gradients(self, sess, grads):
        assert len(grads) == len(self.actor_grads_and_vars)
        feed_dict = {ph: g for (g, ph) in zip(grads, self.actor_grads)}
        sess.run(self.actor_train_expr, feed_dict=feed_dict)

    def reset_noise(self, sess):
        sess.run(self.reset_noise_op)
