from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import numpy as np
import tensorflow as tf
import tensorflow.contrib.layers as layers

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.agents.dqn.dqn_policy_graph import _scope_vars

P_SCOPE = "p_func"
V_SCOPE = "v_func"


class PNetwork(object):
    def __init__(self,
                 model,
                 num_actions,
                 hiddens=[256]):
        self.model = model
        with tf.variable_scope("action_activation"):
            action_out = model.last_layer
            for i in range(len(hiddens)):
                action_out = layers.fully_connected(
                    action_out,
                    num_outputs=hiddens[i],
                    activation_fn=tf.nn.relu)
            self.logits = layers.fully_connected(
                action_out,
                num_outputs=num_actions,
                activation_fn=None)


class VNetwork(object):
    def __init__(self,
                 model,
                 hiddens=[256]):
        self.model = model
        with tf.variable_scope("state_value"):
            if hiddens:
                state_out = model.last_layer
                for i in range(len(hiddens)):
                    state_out = layers.fully_connected(
                        state_out,
                        num_outputs=hiddens[i],
                        activation_fn=tf.nn.relu)
                self.values = layers.fully_connected(
                    state_out,
                    num_outputs=1,
                    activation_fn=None)
            else:
                self.values = model.outputs


class VLoss(object):
    def __init__(self,
                 state_values,
                 cummulative_rewards):
        self.loss = 0.5 * tf.reduce_mean(
            tf.square(state_values - cummulative_rewards))


class ReweightedImitationLoss(object):
    def __init__(self,
                 state_values,
                 cummulative_rewards,
                 logits,
                 actions,
                 action_space,
                 beta):

        ma_adv_norm = tf.get_variable(
            name="moving_average_of_advantage_norm",
            dtype=tf.float32,
            initializer=100.0,
            trainable=False)
        # advantage estimation
        adv = cummulative_rewards - state_values
        # update averaged advantage norm
        update_adv_norm = tf.assign_add(
            ref=ma_adv_norm,
            value=1e-6*(tf.reduce_mean(tf.square(adv)) - ma_adv_norm))

        # exponentially weighted advantages
        with tf.control_dependencies([update_adv_norm]):
            exp_advs = tf.exp(beta * tf.divide(adv, 1e-8+tf.sqrt(ma_adv_norm)))

        # log\pi_\theta(a|s)
        dist_cls, _ = ModelCatalog.get_action_dist(action_space, {})
        action_dist = dist_cls(logits)
        logprobs = action_dist.logp(actions)

        self.loss = -1.0 * tf.reduce_mean(tf.stop_gradient(adv) * logprobs)


class MARWILPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG, **config)
        # TO DO: support hybrid action
        if not isinstance(action_space, Discrete):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DQN.".format(
                    action_space))

        self.config = config
        self.cur_epsilon = 1.0
        self.num_actions = action_space.n

        # Action inputs
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)

        with tf.variable_scope(P_SCOPE) as scope:
            logits = self._build_p_network(
                self.cur_observations, observation_space)
            self.p_func_vars = _scope_vars(scope.name)

        # Action outputs
        dist_cls, _ = ModelCatalog.get_action_dist(action_space, {})
        action_dist = dist_cls(logits)
        self.output_actions = action_dist.sample()

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)
        self.act_t = tf.placeholder(tf.int32, [None], name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")

        # v network evaluation
        with tf.variable_scope(V_SCOPE) as scope:
            state_values = self._build_v_network(
                self.obs_t, observation_space)
            self.v_func_vars = _scope_vars(scope.name)
        self.v_loss = self._build_v_loss(state_values, self.rew_t)

        # p network evaluation
        with tf.variable_scope(P_SCOPE, reuse=True) as scope:
            logits = self._build_p_network(
                self.obs_t, observation_space)
        self.p_loss = self._build_p_loss(
            state_values, self.rew_t, logits, self.act_t, action_space)

        # initialize TFPolicyGraph
        self.sess = tf.get_default_session()
        self.loss_inputs = [
            ("obs", self.obs_t),
            ("actions", self.act_t),
            ("rewards", self.rew_t),
        ]
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=self.cur_observations,
            action_sampler=self.output_actions,
            loss=self.p_loss.loss+self.config["c"]*self.v_loss.loss,
            loss_inputs=self.loss_inputs,
            update_ops=[])
        self.sess.run(tf.global_variables_initializer())

    def _build_p_network(self, obs, obs_space):
        return PNetwork(
            ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, 1, self.config["model"]),
            self.num_actions,
            self.config["actor_hiddens"]).logits

    def _build_v_network(self, obs, obs_space):
        return VNetwork(
            ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, 1, self.config["model"]),
            self.config["critic_hiddens"]).values

    def _build_v_loss(self, state_values, cum_rwds):
        return VLoss(state_values, cum_rwds)

    def _build_p_loss(self, state_values, cum_rwds,
                      logits, actions, action_space):
        return ReweightedImitationLoss(
            state_values, cum_rwds,
             logits, actions, action_space, self.config["beta"])
