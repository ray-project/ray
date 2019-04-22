from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Box
import numpy as np
import tensorflow as tf
import tensorflow.contrib.layers as layers

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.dqn.dqn_policy_graph import (
    _huber_loss, _minimize_and_clip, _scope_vars, _postprocess_dqn)
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph

ACTION_SCOPE = "a_func"
POLICY_SCOPE = "policy"
POLICY_TARGET_SCOPE = "target_policy"
Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"
TWIN_Q_SCOPE = "twin_q_func"
TWIN_Q_TARGET_SCOPE = "twin_target_q_func"

# Importance sampling weights for prioritized replay
PRIO_WEIGHTS = "weights"


class ActorCriticLoss(object):
    def __init__(self,
                 q_t,
                 q_tp1,
                 q_t_det_policy,
                 importance_weights,
                 rewards,
                 done_mask,
                 twin_q_t,
                 twin_q_tp1,
                 gamma=0.99,
                 n_step=1,
                 use_huber=False,
                 huber_threshold=1.0,
                 twin_q=False,
                 policy_delay=1):

        q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
        if twin_q:
            twin_q_t_selected = tf.squeeze(
                twin_q_t, axis=len(q_t.shape) - 1)
            q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

        q_tp1_best = tf.squeeze(
            input=q_tp1, axis=len(q_tp1.shape) - 1)
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
            self.td_error = (
                q_t_selected - tf.stop_gradient(q_t_selected_target))
            if use_huber:
                errors = _huber_loss(self.td_error, huber_threshold)
            else:
                errors = 0.5 * tf.square(self.td_error)

        self.critic_loss = tf.reduce_mean(importance_weights * errors)
        self.actor_loss = -tf.reduce_mean(q_t_det_policy)


class DDPGPostprocessing(object):
    """Implements n-step learning and param noise adjustments."""

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        if self.config["parameter_noise"]:
            # adjust the sigma of parameter space noise
            states, noisy_actions = [
                list(x) for x in sample_batch.columns(
                    [SampleBatch.CUR_OBS, SampleBatch.ACTIONS])
            ]
            self.sess.run(self.remove_noise_op)
            clean_actions = self.sess.run(
                self.output_actions,
                feed_dict={
                    self.cur_observations: states,
                    self.stochastic: False,
                    self.eps: .0
                })
            distance_in_action_space = np.sqrt(
                np.mean(np.square(clean_actions - noisy_actions)))
            self.pi_distance = distance_in_action_space
            if distance_in_action_space < self.config["exploration_ou_sigma"]:
                self.parameter_noise_sigma_val *= 1.01
            else:
                self.parameter_noise_sigma_val /= 1.01
            self.parameter_noise_sigma.load(
                self.parameter_noise_sigma_val, session=self.sess)

        return _postprocess_dqn(self, sample_batch)


class PolicyNetwork(object):
    """Maps an observations (i.e., state) to an action where each entry takes
    value from (0, 1) due to the sigmoid function."""

    def __init__(self,
                 model,
                 dim_actions,
                 high_action,
                 low_action,
                 hiddens=[64, 64],
                 activation="relu",
                 parameter_noise=False):
        action_out = model.last_layer
        activation = tf.nn.__dict__[activation]
        for hidden in hiddens:
            action_out = layers.fully_connected(
                action_out,
                num_outputs=hidden,
                activation_fn=activation,
                normalizer_fn=layers.layer_norm if parameter_noise else None)
        # Use sigmoid layer to bound values within (0, 1)
        # shape of sigmoid_out is [batch_size, dim_actions]
        sigmoid_out = layers.fully_connected(
            action_out, num_outputs=dim_actions, activation_fn=tf.nn.sigmoid)
        # Rescale to actual env policy scale
        self.actions = (high_action - low_action) * sigmoid_out + low_action
        self.model = model


class ActionNetwork(object):
    """Acts as a stochastic policy for inference, but a deterministic policy
    for training, thus ignoring the batch_size issue when constructing a
    stochastic action."""

    def __init__(self,
                 # shape of deterministic_actions is [None, dim_action]
                 deterministic_actions,
                 low_action,
                 high_action,
                 stochastic,
                 eps,
                 is_target,
                 exploration_ou_theta,
                 exploration_ou_sigma,
                 exploration_noise_type,
                 smooth_target_policy,
                 exploration_gaussian_sigma,
                 target_noise,
                 target_noise_clip,
                 parameter_noise):

        # FIXME: this is stupid; should remove it
        use_gaussian_noise = smooth_target_policy

        if use_gaussian_noise:
            if is_target:
                # add noise to target policy to smooth it, TD3-style
                normal_sample = tf.random_normal(
                    tf.shape(deterministic_actions), stddev=target_noise)
                normal_sample = tf.clip_by_value(
                    normal_sample, -target_noise_clip, target_noise_clip)
                stochastic_actions = tf.clip_by_value(
                    deterministic_actions + normal_sample, low_action,
                    high_action)
            else:
                # add IID Gaussian noise for exploration, TD3-style
                normal_sample = tf.random_normal(
                    tf.shape(deterministic_actions),
                    stddev=exploration_gaussian_sigma)
                stochastic_actions = tf.clip_by_value(
                    deterministic_actions + normal_sample, low_action,
                    high_action)
        else:
            # add OU noise for exploration, DDPG-style
            exploration_sample = tf.get_variable(
                name="ornstein_uhlenbeck",
                dtype=tf.float32,
                initializer=low_action.size * [.0],
                trainable=False)
            normal_sample = tf.random_normal(
                shape=[low_action.size], mean=0.0, stddev=1.0)
            exploration_value = tf.assign_add(
                exploration_sample,
                exploration_ou_theta * (.0 - exploration_sample) +
                exploration_ou_sigma * normal_sample)
            stochastic_actions = tf.clip_by_value(
                deterministic_actions +
                eps * (high_action - low_action) * exploration_value,
                low_action, high_action)

        self.actions = tf.cond(
            tf.logical_and(stochastic, not parameter_noise),
            lambda: stochastic_actions, lambda: deterministic_actions)


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


class DDPGPolicyGraph(DDPGPostprocessing, TFPolicyGraph):
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

        # create global step for counting the number of update operations
        self.global_step = tf.train.get_or_create_global_step()

        # use separate optimizers for actor & critic
        self._actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=self.config["actor_lr"])
        self._critic_optimizer = tf.train.AdamOptimizer(
            learning_rate=self.config["critic_lr"])

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32,
            shape=(None, ) + observation_space.shape,
            name="cur_obs")

        # Actor (policy network). policy_values is scaled to (0,1) via a
        # sigmoid activation; to get action values, you need to rescale using
        # actual action bounds for the environment.
        with tf.variable_scope(POLICY_SCOPE) as scope:
            # FIXME: this is super bizarre. Why don't we just scale things
            # *correctly* in the policy network thing?
            policy_out, self.policy_model = self._build_policy_network(
                self.cur_observations, observation_space, action_space)
            self.policy_vars = _scope_vars(scope.name)

        # Noise vars for P network except for layer normalization vars
        if self.config["parameter_noise"]:
            self._build_parameter_noise([
                var for var in self.policy_vars if "LayerNorm" not in
                var.name
            ])

        # Action outputs
        with tf.variable_scope(ACTION_SCOPE):
            # FIXME: remove ActionNetwork entirely
            self.output_actions = self._build_action_network(
                policy_out, self.stochastic, self.eps)

        if self.config["smooth_target_policy"]:
            self.reset_noise_op = tf.no_op()
        else:
            with tf.variable_scope(ACTION_SCOPE, reuse=True):
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
        with tf.variable_scope(POLICY_SCOPE, reuse=True) as scope:
            prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
            self.policy_t, _ = self._build_policy_network(
                self.obs_t, observation_space, action_space)
            policy_batchnorm_update_ops = list(
                set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) -
                prev_update_ops)

        # target p network evaluation
        with tf.variable_scope(POLICY_TARGET_SCOPE) as scope:
            policy_tp1, _ = self._build_policy_network(
                self.obs_tp1, observation_space, action_space)
            target_policy_vars = _scope_vars(scope.name)

        # Action outputs
        with tf.variable_scope(ACTION_SCOPE, reuse=True):
            output_actions = self._build_action_network(
                self.policy_t,
                stochastic=tf.constant(value=False, dtype=tf.bool),
                eps=.0)
            output_actions_estimated = self._build_action_network(
                policy_tp1,
                stochastic=tf.constant(
                    value=self.config["smooth_target_policy"], dtype=tf.bool),
                eps=.0,
                is_target=True)

        # q network evaluation
        prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
        with tf.variable_scope(Q_SCOPE) as scope:
            # Q-values for given actions & observations in given current
            q_t, self.q_model = self._build_q_network(
                self.obs_t, observation_space, action_space, self.act_t)
            self.q_func_vars = _scope_vars(scope.name)
        self.stats = {
            "mean_q": tf.reduce_mean(q_t),
            "max_q": tf.reduce_max(q_t),
            "min_q": tf.reduce_min(q_t),
        }
        with tf.variable_scope(Q_SCOPE, reuse=True):
            # Q-values for current policy (no noise) in given current state
            q_t_det_policy, _ = self._build_q_network(
                self.obs_t, observation_space, action_space, output_actions)
        if self.config["twin_q"]:
            with tf.variable_scope(TWIN_Q_SCOPE) as scope:
                twin_q_t, self.twin_q_model = self._build_q_network(
                    self.obs_t, observation_space, action_space, self.act_t)
                self.twin_q_func_vars = _scope_vars(scope.name)
        q_batchnorm_update_ops = list(
            set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)

        # target q network evaluation
        with tf.variable_scope(Q_TARGET_SCOPE) as scope:
            q_tp1, _ = self._build_q_network(
                self.obs_tp1, observation_space, action_space,
                output_actions_estimated)
            target_q_func_vars = _scope_vars(scope.name)
        if self.config["twin_q"]:
            with tf.variable_scope(TWIN_Q_TARGET_SCOPE) as scope:
                twin_q_tp1, _ = self._build_q_network(
                    self.obs_tp1, observation_space, action_space,
                    output_actions_estimated)
                twin_target_q_func_vars = _scope_vars(scope.name)

        if self.config["twin_q"]:
            self.loss = self._build_actor_critic_loss(
                q_t, q_tp1, q_t_det_policy,
                twin_q_t=twin_q_t, twin_q_tp1=twin_q_tp1)
        else:
            self.loss = self._build_actor_critic_loss(
                q_t, q_tp1, q_t_det_policy)

        if config["l2_reg"] is not None:
            for var in self.policy_vars:
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
                sorted(self.policy_vars, key=lambda v: v.name),
                sorted(target_policy_vars, key=lambda v: v.name)):
            update_target_expr.append(
                var_target.assign(self.tau * var +
                                  (1.0 - self.tau) * var_target))
        self.update_target_expr = tf.group(*update_target_expr)

        self.sess = tf.get_default_session()
        self.loss_inputs = [
            (SampleBatch.CUR_OBS, self.obs_t),
            (SampleBatch.ACTIONS, self.act_t),
            (SampleBatch.REWARDS, self.rew_t),
            (SampleBatch.NEXT_OBS, self.obs_tp1),
            (SampleBatch.DONES, self.done_mask),
            (PRIO_WEIGHTS, self.importance_weights),
        ]
        input_dict = dict(self.loss_inputs)

        # Model self-supervised losses
        self.loss.actor_loss = self.policy_model.custom_loss(
            self.loss.actor_loss, input_dict)
        self.loss.critic_loss = self.q_model.custom_loss(
            self.loss.critic_loss, input_dict)
        if self.config["twin_q"]:
            self.loss.critic_loss = self.twin_q_model.custom_loss(
                self.loss.critic_loss, input_dict)

        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=self.cur_observations,
            action_sampler=self.output_actions,
            loss=self.loss.actor_loss + self.loss.critic_loss,
            loss_inputs=self.loss_inputs,
            update_ops=q_batchnorm_update_ops + policy_batchnorm_update_ops)
        self.sess.run(tf.global_variables_initializer())

        # Note that this encompasses both the policy and Q-value networks and
        # their corresponding target networks
        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            tf.group(q_t_det_policy, q_tp1), self.sess)

        # Hard initial update
        self.update_target(tau=1.0)

    @override(TFPolicyGraph)
    def optimizer(self):
        # we don't use this because we have two separate optimisers
        return None

    @override(TFPolicyGraph)
    def build_apply_op(self, optimizer, grads_and_vars):
        # for policy gradient, update policy net one time v.s.
        # update critic net `policy_delay` time(s)
        global_step = tf.train.get_or_create_global_step()
        should_apply_actor_opt = tf.equal(
            tf.mod(global_step, self.config['policy_delay']), 0)
        actor_op_raw = self._actor_optimizer.apply_gradients(
            self._actor_grads_and_vars)
        actor_op = tf.cond(should_apply_actor_opt,
                           true_fn=lambda: actor_op_raw,
                           false_fn=lambda: tf.no_op())
        with tf.control_dependencies([actor_op]):
            # increment global_step after we check whether it's time to update
            # actor
            actor_op_increment = tf.assign_add(global_step, 1)
        critic_op = self._critic_optimizer.apply_gradients(
            self._critic_grads_and_vars)
        return tf.group(actor_op_increment, critic_op)

    @override(TFPolicyGraph)
    def gradients(self, optimizer, loss):
        if self.config["grad_norm_clipping"] is not None:
            actor_grads_and_vars = _minimize_and_clip(
                self._actor_optimizer,
                self.loss.actor_loss,
                var_list=self.policy_vars,
                clip_val=self.config["grad_norm_clipping"])
            critic_grads_and_vars = _minimize_and_clip(
                self._critic_optimizer,
                self.loss.critic_loss,
                var_list=self.q_func_vars + self.twin_q_func_vars
                if self.config["twin_q"] else self.q_func_vars,
                clip_val=self.config["grad_norm_clipping"])
        else:
            actor_grads_and_vars = self._actor_optimizer.compute_gradients(
                self.loss.actor_loss, var_list=self.policy_vars)
            critic_grads_and_vars = self._critic_optimizer.compute_gradients(
                self.loss.critic_loss,
                var_list=self.q_func_vars + self.twin_q_func_vars
                if self.config["twin_q"] else self.q_func_vars)
        # save these for later use in build_apply_op
        self._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                      if g is not None]
        self._critic_grads_and_vars = [(g, v)
                                       for (g, v) in critic_grads_and_vars
                                       if g is not None]
        grads_and_vars = self._actor_grads_and_vars \
            + self._critic_grads_and_vars
        return grads_and_vars

    @override(TFPolicyGraph)
    def extra_compute_action_feed_dict(self):
        return {
            self.stochastic: True,
            self.eps: self.cur_epsilon,
        }

    @override(TFPolicyGraph)
    def extra_compute_grad_fetches(self):
        return {
            "td_error": self.loss.td_error,
            LEARNER_STATS_KEY: self.stats,
        }

    @override(TFPolicyGraph)
    def get_weights(self):
        return self.variables.get_weights()

    @override(TFPolicyGraph)
    def set_weights(self, weights):
        self.variables.set_weights(weights)

    @override(PolicyGraph)
    def get_state(self):
        return [TFPolicyGraph.get_state(self), self.cur_epsilon]

    @override(PolicyGraph)
    def set_state(self, state):
        TFPolicyGraph.set_state(self, state[0])
        self.set_epsilon(state[1])

    def _build_q_network(self, obs, obs_space, action_space, actions):
        q_net = QNetwork(
            ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, action_space, 1, self.config["model"]), actions,
            self.config["critic_hiddens"],
            self.config["critic_hidden_activation"])
        return q_net.value, q_net.model

    def _build_policy_network(self, obs, obs_space, action_space):
        policy_net = PolicyNetwork(
            ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, action_space, 1, self.config["model"]),
            self.dim_actions, action_space.low, action_space.high,
            self.config["actor_hiddens"],
            self.config["actor_hidden_activation"],
            self.config["parameter_noise"])
        return policy_net.actions, policy_net.model

    def _build_action_network(self, policy_values, stochastic, eps,
                              is_target=False):
        action_opts = [
            "exploration_ou_theta", "exploration_ou_sigma",
            "exploration_gaussian_sigma", "exploration_noise_type",
            "smooth_target_policy", "target_noise", "target_noise_clip",
            "parameter_noise",
        ]
        action_opt_dict = {key: self.config[key] for key in action_opts}
        return ActionNetwork(
            deterministic_actions=policy_values, low_action=self.low_action,
            high_action=self.high_action, stochastic=stochastic, eps=eps,
            is_target=is_target, **action_opt_dict).actions

    def _build_actor_critic_loss(self,
                                 q_t,
                                 q_tp1,
                                 q_t_det_policy,
                                 twin_q_t=None,
                                 twin_q_tp1=None):
        return ActorCriticLoss(q_t, q_tp1, q_t_det_policy,
                               self.importance_weights,
                               self.rew_t, self.done_mask, twin_q_t,
                               twin_q_tp1, self.config["gamma"],
                               self.config["n_step"], self.config["use_huber"],
                               self.config["huber_threshold"],
                               self.config["twin_q"])

    def _build_parameter_noise(self, pnet_params):
        self.parameter_noise_sigma_val = self.config["exploration_ou_sigma"]
        self.parameter_noise_sigma = tf.get_variable(
            initializer=tf.constant_initializer(
                self.parameter_noise_sigma_val),
            name="parameter_noise_sigma",
            shape=(),
            trainable=False,
            dtype=tf.float32)
        self.parameter_noise = list()
        # No need to add any noise on LayerNorm parameters
        for var in pnet_params:
            noise_var = tf.get_variable(
                name=var.name.split(":")[0] + "_noise",
                shape=var.shape,
                initializer=tf.constant_initializer(.0),
                trainable=False)
            self.parameter_noise.append(noise_var)
        remove_noise_ops = list()
        for var, var_noise in zip(pnet_params, self.parameter_noise):
            remove_noise_ops.append(tf.assign_add(var, -var_noise))
        self.remove_noise_op = tf.group(*tuple(remove_noise_ops))
        generate_noise_ops = list()
        for var_noise in self.parameter_noise:
            generate_noise_ops.append(
                tf.assign(
                    var_noise,
                    tf.random_normal(
                        shape=var_noise.shape,
                        stddev=self.parameter_noise_sigma)))
        with tf.control_dependencies(generate_noise_ops):
            add_noise_ops = list()
            for var, var_noise in zip(pnet_params, self.parameter_noise):
                add_noise_ops.append(tf.assign_add(var, var_noise))
            self.add_noise_op = tf.group(*tuple(add_noise_ops))
        self.pi_distance = None

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
        sess.run(self.reset_noise_op)

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.sess.run(self.add_noise_op)

    # support both hard and soft sync
    def update_target(self, tau=None):
        return self.sess.run(
            self.update_target_expr,
            feed_dict={self.tau: tau or self.tau_value})

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon
