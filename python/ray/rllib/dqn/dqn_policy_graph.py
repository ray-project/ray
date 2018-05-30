from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import numpy as np
import tensorflow as tf
import tensorflow.contrib.layers as layers

from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.tf_policy_graph import TFPolicyGraph


Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"


def adjust_nstep(n_step, gamma, obs, actions, rewards, new_obs, dones):
    """Rewrites the given trajectory fragments to encode n-step rewards.

    reward[i] = (
        reward[i] * gamma**0 +
        reward[i+1] * gamma**1 +
        ... +
        reward[i+n_step-1] * gamma**(n_step-1))

    The ith new_obs is also adjusted to point to the (i+n_step-1)'th new obs.

    If the episode finishes, the reward will be truncated. After this rewrite,
    all the arrays will be shortened by (n_step - 1).
    """
    for i in range(len(rewards) - n_step + 1):
        if dones[i]:
            continue  # episode end
        for j in range(1, n_step):
            new_obs[i] = new_obs[i + j]
            rewards[i] += gamma ** j * rewards[i + j]
            if dones[i + j]:
                break  # episode end
    # truncate ends of the trajectory
    new_len = len(obs) - n_step + 1
    for arr in [obs, actions, rewards, new_obs, dones]:
        del arr[new_len:]


class DQNPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, registry, config):
        if not isinstance(action_space, Discrete):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DQN.".format(
                    action_space))

        self.config = config
        self.cur_epsilon = 1.0
        num_actions = action_space.n

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None,) + observation_space.shape)

        # Action Q network
        with tf.variable_scope(Q_SCOPE) as scope:
            q_values = _build_q_network(
                registry, self.cur_observations, num_actions, config)
            self.q_func_vars = _scope_vars(scope.name)

        # Action outputs
        self.output_actions = _build_action_network(
            q_values,
            self.cur_observations,
            num_actions,
            self.stochastic,
            self.eps)

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None,) + observation_space.shape)
        self.act_t = tf.placeholder(tf.int32, [None], name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None,) + observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")

        # q network evaluation
        with tf.variable_scope(Q_SCOPE, reuse=True):
            q_t = _build_q_network(
                registry, self.obs_t, num_actions, config)

        # target q network evalution
        with tf.variable_scope(Q_TARGET_SCOPE) as scope:
            q_tp1 = _build_q_network(
                registry, self.obs_tp1, num_actions, config)
            self.target_q_func_vars = _scope_vars(scope.name)

        # q scores for actions which we know were selected in the given state.
        q_t_selected = tf.reduce_sum(
            q_t * tf.one_hot(self.act_t, num_actions), 1)

        # compute estimate of best possible value starting from state at t + 1
        if config["double_q"]:
            with tf.variable_scope(Q_SCOPE, reuse=True):
                q_tp1_using_online_net = _build_q_network(
                    registry, self.obs_tp1, num_actions, config)
            q_tp1_best_using_online_net = tf.argmax(q_tp1_using_online_net, 1)
            q_tp1_best = tf.reduce_sum(
                q_tp1 * tf.one_hot(
                    q_tp1_best_using_online_net, num_actions), 1)
        else:
            q_tp1_best = tf.reduce_max(q_tp1, 1)
        q_tp1_best_masked = (1.0 - self.done_mask) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = (
            self.rew_t +
            config["gamma"] ** config["n_step"] * q_tp1_best_masked)

        # compute the error (potentially clipped)
        self.td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
        self.loss = tf.reduce_mean(
            self.importance_weights * _huber_loss(self.td_error))

        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        update_target_expr = []
        for var, var_target in zip(
            sorted(self.q_func_vars, key=lambda v: v.name),
                sorted(self.target_q_func_vars, key=lambda v: v.name)):
            update_target_expr.append(var_target.assign(var))
        self.update_target_expr = tf.group(*update_target_expr)

        # initialize TFPolicyGraph
        self.sess = tf.get_default_session()
        self.loss_inputs = [
            ("obs", self.obs_t),
            ("actions", self.act_t),
            ("rewards", self.rew_t),
            ("new_obs", self.obs_tp1),
            ("dones", self.done_mask),
            ("weights", self.importance_weights),
        ]
        self.is_training = tf.placeholder_with_default(True, ())
        TFPolicyGraph.__init__(
            self, self.sess, obs_input=self.cur_observations,
            action_sampler=self.output_actions, loss=self.loss,
            loss_inputs=self.loss_inputs, is_training=self.is_training)
        self.sess.run(tf.global_variables_initializer())

    def optimizer(self):
        return tf.train.AdamOptimizer(learning_rate=self.config["lr"])

    def gradients(self, optimizer):
        if self.config["grad_norm_clipping"] is not None:
            grads_and_vars = _minimize_and_clip(
                optimizer, self.loss, var_list=self.q_func_vars,
                clip_val=self.config["grad_norm_clipping"])
        else:
            grads_and_vars = optimizer.compute_gradients(
                self.loss, var_list=self.q_func_vars)
        grads_and_vars = [
            (g, v) for (g, v) in grads_and_vars if g is not None]
        return grads_and_vars

    def extra_compute_action_feed_dict(self):
        return {
            self.stochastic: True,
            self.eps: self.cur_epsilon,
        }

    def extra_compute_grad_fetches(self):
        return {
            "td_error": self.td_error,
        }

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        return _postprocess_dqn(self, sample_batch)

    def compute_td_error(
            self, obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights):
        td_err = self.sess.run(
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

    def update_target(self):
        return self.sess.run(self.update_target_expr)

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon

    def get_state(self):
        return [TFPolicyGraph.get_state(self), self.cur_epsilon]

    def set_state(self, state):
        TFPolicyGraph.set_state(self, state[0])
        self.set_epsilon(state[1])


def _postprocess_dqn(policy_graph, sample_batch):
    obs, actions, rewards, new_obs, dones = [
        list(x) for x in sample_batch.columns(
            ["obs", "actions", "rewards", "new_obs", "dones"])]

    # N-step Q adjustments
    if policy_graph.config["n_step"] > 1:
        adjust_nstep(
            policy_graph.config["n_step"], policy_graph.config["gamma"],
            obs, actions, rewards, new_obs, dones)

    batch = SampleBatch({
        "obs": obs, "actions": actions, "rewards": rewards,
        "new_obs": new_obs, "dones": dones,
        "weights": np.ones_like(rewards)})
    assert batch.count == policy_graph.config["sample_batch_size"], \
        (batch.count, policy_graph.config["sample_batch_size"])

    # Prioritize on the worker side
    if policy_graph.config["worker_side_prioritization"]:
        td_errors = policy_graph.compute_td_error(
            batch["obs"], batch["actions"], batch["rewards"],
            batch["new_obs"], batch["dones"], batch["weights"])
        new_priorities = (
            np.abs(td_errors) + policy_graph.config["prioritized_replay_eps"])
        batch.data["weights"] = new_priorities

    return batch


def _build_q_network(registry, inputs, num_actions, config):
    dueling = config["dueling"]
    hiddens = config["hiddens"]
    frontend = ModelCatalog.get_model(registry, inputs, 1, config["model"])
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
