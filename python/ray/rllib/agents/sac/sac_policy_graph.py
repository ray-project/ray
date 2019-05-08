from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from numbers import Number

import numpy as np
import tensorflow as tf

from gym.spaces import Box

import ray
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation import SampleBatch
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.utils.annotations import override
from ray.rllib.agents.dqn.dqn_policy_graph import _adjust_nstep

from .models import GaussianLatentSpacePolicy, q_network_model

PRIO_WEIGHTS = "weights"


class SACPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        if not isinstance(action_space, Box):
            # TODO(hartikainen): Should we support discrete action spaces?
            # I"ve seen several people requesting support for it.
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
        self.global_step = tf.train.get_or_create_global_step()

        self._init_placeholders(observation_space, action_space)
        self._init_models(observation_space, action_space)
        self._init_losses()
        self._init_target_update()

        self.loss_inputs = (
            (SampleBatch.CUR_OBS, self._observations_ph),
            (SampleBatch.NEXT_OBS, self._next_observations_ph),
            (SampleBatch.ACTIONS, self._actions_ph),
            (SampleBatch.REWARDS, self._rewards_ph),
            (SampleBatch.DONES, self._terminals_ph),
        )

        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.session,
            obs_input=self._observations_ph,
            action_sampler=self.policy.actions(self._observations_ph),
            loss=self.loss,
            loss_inputs=self.loss_inputs,
            # TODO(hartikainen): what is this for?
            update_ops=None)

        self.session.run(self._initializers())

        Q_mean, Q_var = tf.nn.moments(self.Q_values, axes=[0, 1, 2])
        Q_std = tf.sqrt(Q_var)
        actions_mean, actions_var = tf.nn.moments(
            self._actions_ph, axes=[0, 1])
        actions_std = tf.sqrt(actions_var)
        actions_min = tf.reduce_min(self._actions_ph)
        actions_max = tf.reduce_max(self._actions_ph)
        self.diagnostics = {
            LEARNER_STATS_KEY: {
                "actions-avg": actions_mean,
                "actions-std": actions_std,
                "actions-min": actions_min,
                "actions-max": actions_max,
                "Q-avg": Q_mean,
                "Q-std": Q_std,
                "Q_loss": self.Q_loss,
                "alpha": self.alpha,
                "log_pis": tf.reduce_mean(self.log_pis),
                "policy_loss": self.policy_loss,
                "entropy_loss": self.entropy_loss,
            }
        }

        self.update_target(tau=1.0)

    @override(TFPolicyGraph)
    def extra_compute_grad_fetches(self):
        fetches = self.diagnostics.copy()
        fetches["td_error"] = self.td_error
        return fetches

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

        self._rewards_ph = tf.placeholder(tf.float32, (None, ), name="rewards")

        self._terminals_ph = tf.placeholder(
            tf.bool, (None, ), name="terminals")

        self._tau_ph = tf.placeholder_with_default(
            self.config["tau"], (), name="tau")

    def _init_models(self, observation_space, action_space):
        """Initialize models for value-functions and policy."""
        assert self.config[
            "policy"] == "GaussianLatentSpacePolicy", self.config["policy"]

        self.policy = GaussianLatentSpacePolicy(
            observation_space, action_space, self.config["policy_model"])

        self.log_alpha = tf.get_variable(
            "log_alpha", dtype=tf.float32, initializer=0.0)
        self.alpha = tf.exp(self.log_alpha)

        self.Qs = [
            q_network_model(observation_space, action_space,
                            self.config["Q_model"]) for _ in range(2)
        ]
        self.Q_targets = [
            tf.keras.models.clone_model(self.Qs[i])
            for i in range(len(self.Qs))
        ]

    def _init_actor_loss(self):
        actions = self.policy.actions(self._observations_ph)
        log_pis = self.policy.log_pis(self._observations_ph, actions)

        assert log_pis.shape.as_list() == [None, 1]

        Qs = tf.stack([Q([self._observations_ph, actions]) for Q in self.Qs])
        Q_min = tf.reduce_min(Qs, axis=0)
        assert Q_min.shape.as_list() == [None, 1]

        policy_kl_losses = self.alpha * log_pis - Q_min

        assert policy_kl_losses.shape.as_list() == [None, 1]

        policy_loss_weight = self.config["optimization"]["policy_loss_weight"]
        self.policy_loss = (
            policy_loss_weight * tf.reduce_mean(policy_kl_losses))

    def _get_Q_targets(self):
        next_actions = self.policy.actions(self._next_observations_ph)
        next_log_pis = self.policy.log_pis(self._next_observations_ph,
                                           next_actions)

        next_Q_values = tf.stack([
            Q_target([self._next_observations_ph, next_actions])
            for Q_target in self.Q_targets
        ])

        next_values = next_Q_values - self.alpha * next_log_pis[None, :, :]
        assert next_values.shape.as_list() == [2, None, 1]

        discount = self.config["gamma"]**self.config["n_step"]
        # td target
        return (self._rewards_ph[None, :, None] + discount *
                (1.0 - tf.to_float(self._terminals_ph[None, :, None])) *
                next_values)

    def _init_critic_loss(self):
        Q_targets = tf.stop_gradient(self._get_Q_targets())

        assert Q_targets.shape.as_list() == [2, None, 1]

        Q_values = self.Q_values = tf.stack(
            [Q([self._observations_ph, self._actions_ph]) for Q in self.Qs])

        assert Q_values.shape.as_list() == [2, None, 1]

        Q_loss_weight = self.config["optimization"]["Q_loss_weight"]
        self.td_error = tf.reduce_mean(Q_targets - Q_values, axis=0)

        self.Q_loss = Q_loss_weight * tf.losses.mean_squared_error(
            labels=Q_targets, predictions=Q_values, weights=0.5)

    def _init_entropy_loss(self):
        target_entropy = (-np.prod(self.action_space.shape)
                          if self.config["target_entropy"] == "auto" else
                          self.config["target_entropy"])

        assert isinstance(target_entropy, Number)

        actions = self.policy.actions(self._observations_ph)
        log_pis = self.policy.log_pis(self._observations_ph, actions)

        self.log_pis = log_pis
        entropy_loss_weight = self.config["optimization"][
            "entropy_loss_weight"]
        self.entropy_loss = -1.0 * entropy_loss_weight * tf.reduce_mean(
            self.log_alpha * tf.stop_gradient(log_pis + target_entropy))

    def _init_losses(self):
        self._init_actor_loss()
        self._init_critic_loss()
        self._init_entropy_loss()

        self.loss = (self.policy_loss + self.Q_loss + self.entropy_loss)

    @override(TFPolicyGraph)
    def _build_learn_on_batch(self, builder, postprocessed_batch):
        fetch = super(SACPolicyGraph, self)._build_learn_on_batch(
            builder, postprocessed_batch)
        builder.add_fetches([self._update_target_op])
        return fetch

    @override(TFPolicyGraph)
    def learn_on_batch(self, samples):
        return_value = super(SACPolicyGraph, self).learn_on_batch(samples)
        self.update_target()
        return return_value

    @override(TFPolicyGraph)
    def optimizer(self):
        return tf.train.AdamOptimizer(
            learning_rate=self.config["optimization"]["learning_rate"])

    @override(TFPolicyGraph)
    def gradients(self, optimizer, loss):
        Q_vars = sum((Q.trainable_variables for Q in self.Qs), [])

        policy_grads_and_vars = optimizer.compute_gradients(
            loss, var_list=self.policy.trainable_variables)
        Q_grads_and_vars = optimizer.compute_gradients(loss, var_list=Q_vars)
        entropy_grads_and_vars = optimizer.compute_gradients(
            loss, var_list=self.log_alpha)

        grads_and_vars = (
            policy_grads_and_vars + Q_grads_and_vars + entropy_grads_and_vars)

        grads_and_vars = tuple(grad_and_var for grad_and_var in grads_and_vars
                               if grad_and_var is not None)

        return grads_and_vars

    def set_epsilon(self, epsilon):
        return

    def _init_target_update(self):
        ops = []
        tau = self._tau_ph
        for Q, Q_target in zip(self.Qs, self.Q_targets):
            for source, target in zip(Q.variables, Q_target.variables):
                op = target.assign(tau * source + (1.0 - tau) * target)
                ops.append(op)
        self._update_target_op = tf.group(ops)

    def update_target(self, tau=None):
        self.session.run(self._update_target_op,
                         {self._tau_ph: tau or self.config["tau"]})

    def compute_td_error(self, obs_t, act_t, rew_t, obs_tp1, done):
        return self.sess.run(
            self.td_error,
            feed_dict={
                self._observations_ph: [np.array(ob) for ob in obs_t],
                self._actions_ph: act_t,
                self._rewards_ph: rew_t,
                self._next_observations_ph: [np.array(ob) for ob in obs_tp1],
                self._terminals_ph: done
            })

    def _initializers(self):
        initializers = [
            self.log_alpha.initializer, self.global_step.initializer
        ]
        initializers += [v.initializer for Q in self.Qs for v in Q.variables]
        initializers += [
            v.initializer for Q in self.Q_targets for v in Q.variables
        ]
        initializers += [v.initializer for v in self.policy.variables]
        initializers += [v.initializer for v in self._optimizer.variables()]
        return initializers

    @override(TFPolicyGraph)
    def postprocess_trajectory(self, batch, *args, **kwargs):
        # N-step Q adjustments
        if self.config["n_step"] > 1:
            _adjust_nstep(
                self.config["n_step"], self.config["gamma"],
                batch[SampleBatch.CUR_OBS], batch[SampleBatch.ACTIONS],
                batch[SampleBatch.REWARDS], batch[SampleBatch.NEXT_OBS],
                batch[SampleBatch.DONES])

        if self.config["worker_side_prioritization"]:
            td_errors = self.compute_td_error(
                batch[SampleBatch.CUR_OBS], batch[SampleBatch.ACTIONS],
                batch[SampleBatch.REWARDS], batch[SampleBatch.NEXT_OBS],
                batch[SampleBatch.DONES])
            batch[PRIO_WEIGHTS] = td_errors
        return batch
