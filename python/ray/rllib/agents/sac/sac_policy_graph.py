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

        # create global step for counting the number of update operations
        # self.session = self.sess = tf.keras.backend.get_session()
        self.session = self.sess = tf.get_default_session()
        self.session._graph = tf.get_default_graph()
        tf.keras.backend.set_session(self.session)

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

        self.update_target(tau=1.0)

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
            tf.float32, (None), name="rewards")

        self._terminals_ph = tf.placeholder(
            tf.bool, (None), name="terminals")

    def _init_models(self, observation_space, action_space):
        """Initialize models for value-functions and policy."""
        policy_type = self.config['policy']['type']
        assert policy_type == 'GaussianLatentSpacePolicy', policy_type
        policy_kwargs = self.config['policy']['kwargs']

        self.policy = GaussianLatentSpacePolicy(
            input_shape=observation_space.shape,
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
            input_shape=observation_space.shape,
            output_size=1,
            **Q_kwargs)
        self.Q_target = tf.keras.models.clone_model(self.Q)

    def _init_actor_loss(self):
        actions = self.policy.actions([self._observations_ph])
        log_pis = self.policy.log_pis([self._observations_ph], actions)

#        assert log_pis.shape.as_list() == [None, 1]

        Q_log_targets = self.Q([self._observations_ph, actions])

        # assert self._reparameterize

        policy_kl_losses = self.alpha * log_pis - Q_log_targets

#        assert policy_kl_losses.shape.as_list() == [None, 1]

        self.policy_loss = tf.reduce_mean(policy_kl_losses)

    def _init_critic_loss(self):
        next_actions = self.policy.actions([self._next_observations_ph])
        next_log_pis = self.policy.log_pis(
            [self._next_observations_ph], next_actions)

        next_Q_values = self.Q([self._next_observations_ph, next_actions])
        next_values = next_Q_values - self.alpha * next_log_pis

        discount = self.config['gamma']

        Q_targets = tf.stop_gradient(
            self._rewards_ph
            + discount
            * (1.0 - tf.to_float(self._terminals_ph)) * next_values)

#        assert Q_targets.shape.as_list() == [None, 1]

        Q_values = self.Q([self._observations_ph, self._actions_ph])
        self.Q_loss = tf.losses.mean_squared_error(
            labels=Q_targets, predictions=Q_values, weights=0.5)

    def _init_entropy_loss(self):
        target_entropy = (
            -np.prod(self.action_space.shape)
            if self.config['target_entropy'] == 'auto'
            else self.config['target_entropy'])

        assert isinstance(target_entropy, Number)

        actions = self.policy.actions([self._observations_ph])
        log_pis = self.policy.log_pis([self._observations_ph], actions)

        self.entropy_loss = -tf.reduce_mean(
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
        config = self.config['optimization']

        entropy_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["entropy_lr"])
        policy_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["policy_lr"])
        Q_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["Q_lr"])

        optimizers = {
            'entropy': entropy_optimizer,
            'policy': policy_optimizer,
            'Q': Q_optimizer,
        }

        return optimizers

    @override(TFPolicyGraph)
    def gradients(self, optimizers):
        entropy_optimizer = optimizers['entropy']
        policy_optimizer = optimizers['policy']
        Q_optimizer = optimizers['Q']

        policy_grads_and_vars = policy_optimizer.compute_gradients(
            self.policy_loss, var_list=self.policy.trainable_variables)
        Q_grads_and_vars = Q_optimizer.compute_gradients(
            self.Q_loss, var_list=self.Q.trainable_variables)
        entropy_grads_and_vars = entropy_optimizer.compute_gradients(
            self.entropy_loss, var_list=self.log_alpha)

        grads_and_vars = {
            'entropy': entropy_grads_and_vars,
            'policy': policy_grads_and_vars,
            'Q': Q_grads_and_vars,
        }

        return grads_and_vars

    def update_target(self, tau=None):
        tau = tau or self.config["tau"]

        source_params = self.Q.get_weights()
        target_params = self.Q_target.get_weights()
        self.Q_target.set_weights([
            tau * source + (1.0 - tau) * target
            for source, target in zip(source_params, target_params)
        ])
