from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.layers as layers

from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers.multi_gpu_impl import TOWER_SCOPE_NAME

def _build_p_network(registry, inputs, low_action, high_action, config):
    #dueling = config["dueling"]
    hiddens = config["actor_hiddens"]
    frontend = ModelCatalog.get_model(registry, inputs, 1, config["model"])
    frontend_out = frontend.last_layer

    with tf.variable_scope("action_value"):
        action_out = frontend_out
        for hidden in hiddens:
            action_out = layers.fully_connected(
                action_out, num_outputs=hidden, activation_fn=tf.nn.relu)#,
                #weights_regularizer=layers.l2_regularizer(0.01))
        # The final output layer of the actor was a tanh layer, to bound the actions
        action_scores = layers.fully_connected(
            action_out, num_outputs=low_action.size, activation_fn=tf.nn.tanh)#,
            #weights_regularizer=layers.l2_regularizer(0.01))

    '''
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
    '''

    return action_scores

def _build_action_network(
        p_values, observations, low_action, high_action, stochastic, theta, sigma):
    deterministic_actions = p_values
    batch_size = tf.shape(observations)[0]
    '''
    random_actions = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=num_actions, dtype=tf.int64)
    chose_random = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=1, dtype=tf.float32) < eps
    stochastic_actions = tf.where(
        chose_random, random_actions, deterministic_actions)
    '''
    exploration_sample = tf.get_variable(name="ornstein_uhlenbeck", dtype=tf.float32, \
                         initializer=[.0], trainable=False)
    normal_samples = tf.random_normal(shape=[low_action.size], mean=0.0, stddev=1.0)
    exploration_values = tf.assign_add(exploration_sample, \
                         theta * (0.0-exploration_sample) + sigma * normal_samples)
    stochastic_actions = p_values + exploration_values

    taken_actions = tf.cond(
        stochastic, lambda: stochastic_actions,
        lambda: deterministic_actions)

    # Scaling the output of policy network to [low, high]
    scaled_actions = high_action * tf.clip_by_value(taken_actions, -1.0, 1.0)
    return scaled_actions

def _build_q_network(registry, inputs, act_inputs, low_action, high_action, config):
    #dueling = config["dueling"]
    hiddens = config["critic_hiddens"]
    frontend = ModelCatalog.get_model(registry, inputs, 1, config["model"])
    frontend_out = frontend.last_layer

    with tf.variable_scope("action_value"):
        state_out = frontend_out
        action_out = act_inputs
        for i in xrange(len(hiddens)):
            if i == len(hiddens)-1:
                action_out = tf.concat([state_out, action_out], axis=1)
                action_out = layers.fully_connected(
                    action_out, num_outputs=hiddens[i], activation_fn=tf.nn.relu)#,
                    #weights_regularizer=layers.l2_regularizer(0.01))
                break
            state_out = layers.fully_connected(
                state_out, num_outputs=hiddens[i], activation_fn=tf.nn.relu)#,
                #weights_regularizer=layers.l2_regularizer(0.01))
            action_out = layers.fully_connected(
                action_out, num_outputs=hiddens[i], activation_fn=tf.nn.relu)#,
                #weights_regularizer=layers.l2_regularizer(0.01))

        action_scores = layers.fully_connected(
            action_out, num_outputs=1, activation_fn=None)#,
            #weights_regularizer=layers.l2_regularizer(0.01))

    '''
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
    '''
    return action_scores

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
            self, registry, low_action, high_action, config,
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
        # p network evaluation
        with tf.variable_scope("p_func", reuse=True):
            self.p_t = _build_p_network(registry, obs_t, low_action, high_action, config)

        # Action outputs
        with tf.variable_scope("a_func", reuse=True):
            self.output_actions = _build_action_network(
                self.p_t,
                obs_t,
                low_action,
                high_action,
                tf.constant(value=False, dtype=tf.bool),
                config["exploration_theta"],
                config["exploration_sigma"])

        # target p network evaluation
        with tf.variable_scope("target_p_func") as scope:
            self.p_tp1 = _build_p_network(registry, obs_tp1, low_action, high_action, config)
            self.target_p_func_vars = _scope_vars(scope.name)

        # q network evaluation
        with tf.variable_scope("q_func", reuse=True):
            self.q_t = _build_q_network(registry, obs_t, act_t, low_action, high_action, config)
            self.q_tp0 = _build_q_network(registry, obs_t, self.output_actions, low_action, high_action, config)

        # target q network evalution
        with tf.variable_scope("target_q_func") as scope:
            self.q_tp1 = _build_q_network(
                registry, obs_tp1, self.p_tp1, low_action, high_action, config)
            self.target_q_func_vars = _scope_vars(scope.name)

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
        q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = (
            rew_t + config["gamma"] ** config["n_step"] * q_tp1_best_masked)

        # compute the error (potentially clipped)
        self.td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
        errors = _huber_loss(self.td_error)

        weighted_error = tf.reduce_mean(importance_weights * errors)

        self.loss = weighted_error

        # for policy gradient
        self.actor_loss = tf.reduce_mean(-self.q_tp0)


class DDPGGraph(object):
    def __init__(self, registry, env, config, logdir):
        self.env = env
        low_action = env.action_space.low
        high_action = env.action_space.high
        assert low_action.size == high_action.size, "the size of action_space.low does NOT equal to the size of action_space.high"
        actor_optimizer = tf.train.AdamOptimizer(learning_rate=config["actor_lr"])
        critic_optimizer = tf.train.AdamOptimizer(learning_rate=config["critic_lr"])

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        #self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)

        # Action P (policy) network
        p_scope_name = TOWER_SCOPE_NAME + "/p_func"
        with tf.variable_scope(p_scope_name) as scope:
            p_values = _build_p_network(
                registry, self.cur_observations, low_action, high_action, config)
            p_func_vars = _scope_vars(scope.name)

        # Action outputs
        a_scope_name = TOWER_SCOPE_NAME + "/a_func"
        with tf.variable_scope(a_scope_name):
            self.output_actions = _build_action_network(
                p_values,
                self.cur_observations,
                low_action,
                high_action,
                self.stochastic,
                config["exploration_theta"],
                config["exploration_sigma"])

        with tf.variable_scope(a_scope_name, reuse=True):
            exploration_sample = tf.get_variable(name="ornstein_uhlenbeck")
            self.reset_noise_op = tf.assign(exploration_sample, [.0])


        # Action Q network
        q_scope_name = TOWER_SCOPE_NAME + "/q_func"
        with tf.variable_scope(q_scope_name) as scope:
            q_values = _build_q_network(
                registry, self.cur_observations, self.output_actions, low_action, high_action, config)
            q_func_vars = _scope_vars(scope.name)

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None,) + env.observation_space.shape)
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
                low_action, high_action, config,
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

        weighted_error = loss_obj.loss
        target_p_func_vars = loss_obj.target_p_func_vars
        target_q_func_vars = loss_obj.target_q_func_vars
        self.q_t = loss_obj.q_t
        self.q_tp1 = loss_obj.q_tp1
        self.td_error = loss_obj.td_error

        # compute optimization op (potentially with gradient clipping)
        if config["grad_norm_clipping"] is not None:
            self.critic_grads_and_vars = _minimize_and_clip(
                critic_optimizer, weighted_error, var_list=q_func_vars,
                clip_val=config["grad_norm_clipping"])
            self.actor_grads_and_vars = _minimize_and_clip(
                actor_optimizer, loss_obj.actor_loss, var_list=p_func_vars,
                clip_val=config["grad_norm_clipping"])
        else:
            self.critic_grads_and_vars = critic_optimizer.compute_gradients(
                weighted_error, var_list=q_func_vars)
            self.actor_grads_and_vars = actor_optimizer.compute_gradients(
                loss_obj.actor_loss, var_list=p_func_vars)
        self.critic_grads_and_vars = [
            (g, v) for (g, v) in self.critic_grads_and_vars if g is not None]
        self.actor_grads_and_vars = [
            (g, v) for (g, v) in self.actor_grads_and_vars if g is not None]
        self.critic_grads = [g for (g, v) in self.critic_grads_and_vars]
        self.actor_grads = [g for (g, v) in self.actor_grads_and_vars]
        self.critic_train_expr = critic_optimizer.apply_gradients(self.critic_grads_and_vars)
        self.actor_train_expr = actor_optimizer.apply_gradients(self.actor_grads_and_vars)


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

    def update_target(self, sess):
        return sess.run(self.update_target_expr)

    def act(self, sess, obs, stochastic=True):
        return sess.run(
            self.output_actions,
            feed_dict={
                self.cur_observations: obs,
                self.stochastic: stochastic,
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

    def compute_actor_gradients(
            self, sess, obs_t, act_t, rew_t, obs_tp1, done_mask,
            importance_weights):
        grads = sess.run(
            self.actor_grads,
            feed_dict={
                self.obs_t: obs_t,
                self.act_t: act_t,
                self.rew_t: rew_t,
                self.obs_tp1: obs_tp1,
                self.done_mask: done_mask,
                self.importance_weights: importance_weights
            })
        return grads

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
