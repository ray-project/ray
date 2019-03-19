from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from numbers import Number

import numpy as np
import tensorflow as tf

from gym.spaces import Box

import ray
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.annotations import override

from .models import GaussianLatentSpacePolicy, feedforward_model


class SACPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        if not isinstance(action_space, Box):
            # TODO(hartikainen): Should we support discrete action spaces?
            # I've seen several people requesting support for it.
            raise UnsupportedSpaceException(
                "Action space {} is not supported for SAC.".format(
                    action_space))

        config = dict(ray.rllib.agents.sac.sac.DEFAULT_CONFIG, **config)

        self.config = config

        self.action_space = action_space
        self.observation_space = observation_space

        self.session = self.sess = tf.get_default_session()
        tf.keras.backend.set_session(self.session)

        # create global step for counting the number of update operations
        # self.global_step = tf.train.get_or_create_global_step()

        self._init_placeholders(observation_space, action_space)
        self._init_models(observation_space, action_space)
        self._init_losses()

        self.loss_inputs = (
            ("obs", self._observations_ph),
            ("new_obs", self._next_observations_ph),
            ("actions", self._actions_ph),
            ("rewards", self._rewards_ph),
            ("dones", self._terminals_ph),
        )

        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.session,
            obs_input=self._observations_ph,
            action_sampler=self.policy.actions([self._observations_ph]),
            loss=self.total_loss,
            loss_inputs=self.loss_inputs,
            # TODO(hartikainen): what is this for?
            update_ops=None)

        self.session.run(tf.global_variables_initializer())

        Q_mean, Q_var = tf.nn.moments(self.Q_values, axes=[0, 1])
        Q_std = tf.sqrt(Q_var)
        actions_mean, actions_var = tf.nn.moments(self._actions_ph, axes=[0, 1])
        actions_std = tf.sqrt(actions_var)
        actions_min = tf.reduce_min(self._actions_ph)
        actions_max = tf.reduce_max(self._actions_ph)
        self.diagnostics = {
            'stats': {
                'actions-avg': actions_mean,
                'actions-std': actions_std,
                'actions-min': actions_min,
                'actions-max': actions_max,
                'Q-avg': Q_mean,
                'Q-std': Q_std,
                'Q_loss': self.Q_loss,
                'alpha': self.alpha,
                'log_pis': tf.reduce_mean(self.log_pis),
                'policy_loss': self.policy_loss,
                'entropy_loss': self.entropy_loss,
            }
        }

        self.update_target(tau=1.0)

    @override(TFPolicyGraph)
    def extra_compute_grad_fetches(self):
        return self.diagnostics

    def _init_placeholders(self, observation_space, action_space):
        observation_shape = observation_space.shape
        action_shape = action_space.shape

        self._iteration_ph = tf.placeholder(tf.int64, (), name="iteration")

        self._observations_ph = tf.placeholder(
            tf.float32, (None, *observation_shape), name="observations")

        self._next_observations_ph = tf.placeholder(
            tf.float32, (None, *observation_shape), name="next_observations")

        self._actions_ph = tf.placeholder(
            tf.float32, (None, *action_shape), name="actions")

        self._rewards_ph = tf.placeholder(
            tf.float32, (None, ), name="rewards")

        self._terminals_ph = tf.placeholder(
            tf.bool, (None, ), name="terminals")

    def _init_models(self, observation_space, action_space):
        """Initialize models for value-functions and policy."""
        policy_type = self.config['policy']['type']
        assert policy_type == 'GaussianLatentSpacePolicy', policy_type
        policy_kwargs = self.config['policy']['kwargs']

        self.policy = GaussianLatentSpacePolicy(
            input_shapes=(observation_space.shape, ),
            output_shape=action_space.shape,
            **policy_kwargs)

        self.log_alpha = tf.get_variable(
            'log_alpha',
            dtype=tf.float32,
            initializer=0.0)
        self.alpha = tf.exp(self.log_alpha)

        Q_type = self.config['Q']['type']
        # TODO(hartikainen): implement twin q
        assert Q_type == 'FeedforwardQ', Q_type
        Q_kwargs = self.config['Q']['kwargs']
        self.Q = feedforward_model(
            input_shapes=(observation_space.shape, action_space.shape),
            output_size=1,
            **Q_kwargs)
        self.Q_target = tf.keras.models.clone_model(self.Q)

    def _init_actor_loss(self):
        actions = self.policy.actions([self._observations_ph])
        log_pis = self.policy.log_pis([self._observations_ph], actions)

        assert log_pis.shape.as_list() == [None, 1]

        Q_log_targets = self.Q([self._observations_ph, actions])

        policy_kl_losses = self.alpha * log_pis - Q_log_targets

        assert policy_kl_losses.shape.as_list() == [None, 1]

        policy_loss_weight = self.config['optimization']['policy_loss_weight']
        self.policy_loss = policy_loss_weight * tf.reduce_mean(policy_kl_losses)

    def _get_Q_target(self):
        next_actions = self.policy.actions([self._next_observations_ph])
        next_log_pis = self.policy.log_pis(
            [self._next_observations_ph], next_actions)

        next_Q_values = self.Q_target([self._next_observations_ph, next_actions])

        next_values = next_Q_values - self.alpha * next_log_pis
        discount = self.config['gamma']

        # td target
        Q_targets = (
            self._rewards_ph[:, None]
            + discount
            * (1.0 - tf.to_float(self._terminals_ph[:, None])) * next_values)

        return Q_targets

    def _init_critic_loss(self):
        Q_targets = tf.stop_gradient(self._get_Q_target())

        assert Q_targets.shape.as_list() == [None, 1]

        Q_values = self.Q_values = self.Q([self._observations_ph, self._actions_ph])
        Q_loss_weight = self.config['optimization']['Q_loss_weight']
        self.Q_loss = Q_loss_weight * tf.losses.mean_squared_error(
            labels=Q_targets, predictions=Q_values, weights=0.5)

    def _init_entropy_loss(self):
        target_entropy = (
            -np.prod(self.action_space.shape)
            if self.config['target_entropy'] == 'auto'
            else self.config['target_entropy'])

        assert isinstance(target_entropy, Number)

        actions = self.policy.actions([self._observations_ph])
        log_pis = self.policy.log_pis([self._observations_ph], actions)

        self.log_pis = log_pis
        entropy_loss_weight = self.config['optimization']['entropy_loss_weight']
        self.entropy_loss = -1.0 * entropy_loss_weight * tf.reduce_mean(
            self.log_alpha * tf.stop_gradient(log_pis + target_entropy))

    def _init_losses(self):
        self._init_actor_loss()
        self._init_critic_loss()
        self._init_entropy_loss()

        self.total_loss = (
            self.policy_loss
            + self.Q_loss
            + self.entropy_loss)

    @override(TFPolicyGraph)
    def optimizer(self):
        optimizer = tf.train.AdamOptimizer(
            learning_rate=self.config['optimization']["learning_rate"])
        return optimizer

    @override(TFPolicyGraph)
    def gradients(self, optimizer):
        policy_grads_and_vars = optimizer.compute_gradients(
            self.policy_loss, var_list=self.policy.trainable_variables)
        Q_grads_and_vars = optimizer.compute_gradients(
            self.Q_loss, var_list=self.Q.trainable_variables)
        entropy_grads_and_vars = optimizer.compute_gradients(
            self.entropy_loss, var_list=self.log_alpha)

        grads_and_vars = (
            policy_grads_and_vars
            + Q_grads_and_vars
            + entropy_grads_and_vars)

        grads_and_vars = tuple(
            grad_and_var for grad_and_var in grads_and_vars
            if grad_and_var is not None)

        return grads_and_vars

    def set_epsilon(self, epsilon):
        return

    def update_target(self, tau=None):
        tau = tau or self.config["tau"]

        source_params = self.Q.get_weights()
        target_params = self.Q_target.get_weights()
        self.Q_target.set_weights([
            tau * source + (1.0 - tau) * target
            for source, target in zip(source_params, target_params)
        ])
