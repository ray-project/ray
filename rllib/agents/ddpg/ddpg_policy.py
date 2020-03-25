from gym.spaces import Box
import numpy as np

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.dqn.dqn_policy import postprocess_nstep_and_prio
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Deterministic
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.tf_ops import huber_loss, minimize_and_clip, scope_vars

tf = try_import_tf()

ACTION_SCOPE = "action"
POLICY_SCOPE = "policy"
POLICY_TARGET_SCOPE = "target_policy"
Q_SCOPE = "critic"
Q_TARGET_SCOPE = "target_critic"
TWIN_Q_SCOPE = "twin_critic"
TWIN_Q_TARGET_SCOPE = "twin_target_critic"

# Importance sampling weights for prioritized replay
PRIO_WEIGHTS = "weights"


class DDPGPostprocessing:
    """Implements n-step learning and param noise adjustments."""

    @override(Policy)
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
            self.sess.run(self.remove_parameter_noise_op)

            # TODO(sven): This won't work if exploration != Noise, which is
            #  probably fine as parameter_noise will soon be its own
            #  Exploration class.
            clean_actions, cur_noise_scale = self.sess.run(
                [self.output_actions,
                 self.exploration.get_info()],
                feed_dict={
                    self.cur_observations: states,
                    self._is_exploring: False,
                })
            distance_in_action_space = np.sqrt(
                np.mean(np.square(clean_actions - noisy_actions)))
            self.pi_distance = distance_in_action_space
            if distance_in_action_space < \
                    self.config["exploration_config"].get("ou_sigma", 0.2) * \
                    cur_noise_scale:
                # multiplying the sampled OU noise by noise scale is
                # equivalent to multiplying the sigma of OU by noise scale
                self.parameter_noise_sigma_val *= 1.01
            else:
                self.parameter_noise_sigma_val /= 1.01
            self.parameter_noise_sigma.load(
                self.parameter_noise_sigma_val, session=self.sess)

        return postprocess_nstep_and_prio(self, sample_batch)


class DDPGTFPolicy(DDPGPostprocessing, TFPolicy):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.ddpg.ddpg.DEFAULT_CONFIG, **config)
        if not isinstance(action_space, Box):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DDPG.".format(
                    action_space))
        if len(action_space.shape) > 1:
            raise UnsupportedSpaceException(
                "Action space has multiple dimensions "
                "{}. ".format(action_space.shape) +
                "Consider reshaping this into a single dimension, "
                "using a Tuple action space, or the multi-agent API.")

        self.config = config

        # Create global step for counting the number of update operations.
        self.global_step = tf.train.get_or_create_global_step()
        # Create sampling timestep placeholder.
        timestep = tf.placeholder(tf.int32, (), name="timestep")

        # use separate optimizers for actor & critic
        self._actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=self.config["actor_lr"])
        self._critic_optimizer = tf.train.AdamOptimizer(
            learning_rate=self.config["critic_lr"])

        # Observation inputs.
        self.cur_observations = tf.placeholder(
            tf.float32,
            shape=(None, ) + observation_space.shape,
            name="cur_obs")

        with tf.variable_scope(POLICY_SCOPE) as scope:
            policy_out, self.policy_model = self._build_policy_network(
                self.cur_observations, observation_space, action_space)
            self.policy_vars = scope_vars(scope.name)

        # Noise vars for P network except for layer normalization vars
        if self.config["parameter_noise"]:
            self._build_parameter_noise([
                var for var in self.policy_vars if "LayerNorm" not in var.name
            ])

        # Create exploration component.
        self.exploration = self._create_exploration(action_space, config)
        explore = tf.placeholder_with_default(True, (), name="is_exploring")
        # Action outputs
        with tf.variable_scope(ACTION_SCOPE):
            self.output_actions, _ = self.exploration.get_exploration_action(
                policy_out, Deterministic, self.policy_model, timestep,
                explore)

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

        # policy network evaluation
        with tf.variable_scope(POLICY_SCOPE, reuse=True) as scope:
            prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
            self.policy_t, _ = self._build_policy_network(
                self.obs_t, observation_space, action_space)
            policy_batchnorm_update_ops = list(
                set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) -
                prev_update_ops)

        # target policy network evaluation
        with tf.variable_scope(POLICY_TARGET_SCOPE) as scope:
            policy_tp1, _ = self._build_policy_network(
                self.obs_tp1, observation_space, action_space)
            target_policy_vars = scope_vars(scope.name)

        # Action outputs
        with tf.variable_scope(ACTION_SCOPE, reuse=True):
            if config["smooth_target_policy"]:
                target_noise_clip = self.config["target_noise_clip"]
                clipped_normal_sample = tf.clip_by_value(
                    tf.random_normal(
                        tf.shape(policy_tp1),
                        stddev=self.config["target_noise"]),
                    -target_noise_clip, target_noise_clip)
                policy_tp1_smoothed = tf.clip_by_value(
                    policy_tp1 + clipped_normal_sample,
                    action_space.low * tf.ones_like(policy_tp1),
                    action_space.high * tf.ones_like(policy_tp1))
            else:
                # no smoothing, just use deterministic actions
                policy_tp1_smoothed = policy_tp1

        # q network evaluation
        prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
        with tf.variable_scope(Q_SCOPE) as scope:
            # Q-values for given actions & observations in given current
            q_t, self.q_model = self._build_q_network(
                self.obs_t, observation_space, action_space, self.act_t)
            self.q_func_vars = scope_vars(scope.name)
        self.stats = {
            "mean_q": tf.reduce_mean(q_t),
            "max_q": tf.reduce_max(q_t),
            "min_q": tf.reduce_min(q_t),
        }
        with tf.variable_scope(Q_SCOPE, reuse=True):
            # Q-values for current policy (no noise) in given current state
            q_t_det_policy, _ = self._build_q_network(
                self.obs_t, observation_space, action_space, self.policy_t)
        if self.config["twin_q"]:
            with tf.variable_scope(TWIN_Q_SCOPE) as scope:
                twin_q_t, self.twin_q_model = self._build_q_network(
                    self.obs_t, observation_space, action_space, self.act_t)
                self.twin_q_func_vars = scope_vars(scope.name)
        q_batchnorm_update_ops = list(
            set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)

        # target q network evaluation
        with tf.variable_scope(Q_TARGET_SCOPE) as scope:
            q_tp1, _ = self._build_q_network(self.obs_tp1, observation_space,
                                             action_space, policy_tp1_smoothed)
            target_q_func_vars = scope_vars(scope.name)
        if self.config["twin_q"]:
            with tf.variable_scope(TWIN_Q_TARGET_SCOPE) as scope:
                twin_q_tp1, _ = self._build_q_network(
                    self.obs_tp1, observation_space, action_space,
                    policy_tp1_smoothed)
                twin_target_q_func_vars = scope_vars(scope.name)

        if self.config["twin_q"]:
            self.critic_loss, self.actor_loss, self.td_error \
                = self._build_actor_critic_loss(
                    q_t, q_tp1, q_t_det_policy, twin_q_t=twin_q_t,
                    twin_q_tp1=twin_q_tp1)
        else:
            self.critic_loss, self.actor_loss, self.td_error \
                = self._build_actor_critic_loss(
                    q_t, q_tp1, q_t_det_policy)

        if config["l2_reg"] is not None:
            for var in self.policy_vars:
                if "bias" not in var.name:
                    self.actor_loss += (config["l2_reg"] * tf.nn.l2_loss(var))
            for var in self.q_func_vars:
                if "bias" not in var.name:
                    self.critic_loss += (config["l2_reg"] * tf.nn.l2_loss(var))
            if self.config["twin_q"]:
                for var in self.twin_q_func_vars:
                    if "bias" not in var.name:
                        self.critic_loss += (
                            config["l2_reg"] * tf.nn.l2_loss(var))

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

        if self.config["use_state_preprocessor"]:
            # Model self-supervised losses
            self.actor_loss = self.policy_model.custom_loss(
                self.actor_loss, input_dict)
            self.critic_loss = self.q_model.custom_loss(
                self.critic_loss, input_dict)
            if self.config["twin_q"]:
                self.critic_loss = self.twin_q_model.custom_loss(
                    self.critic_loss, input_dict)

        TFPolicy.__init__(
            self,
            observation_space,
            action_space,
            self.config,
            self.sess,
            obs_input=self.cur_observations,
            sampled_action=self.output_actions,
            loss=self.actor_loss + self.critic_loss,
            loss_inputs=self.loss_inputs,
            update_ops=q_batchnorm_update_ops + policy_batchnorm_update_ops,
            explore=explore,
            timestep=timestep)
        self.sess.run(tf.global_variables_initializer())

        # Note that this encompasses both the policy and Q-value networks and
        # their corresponding target networks
        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            tf.group(q_t_det_policy, q_tp1, self._actor_optimizer.variables(),
                     self._critic_optimizer.variables()), self.sess)

        # Hard initial update
        self.update_target(tau=1.0)

    @override(TFPolicy)
    def optimizer(self):
        # we don't use this because we have two separate optimisers
        return None

    @override(TFPolicy)
    def build_apply_op(self, optimizer, grads_and_vars):
        # for policy gradient, update policy net one time v.s.
        # update critic net `policy_delay` time(s)
        should_apply_actor_opt = tf.equal(
            tf.mod(self.global_step, self.config["policy_delay"]), 0)

        def make_apply_op():
            return self._actor_optimizer.apply_gradients(
                self._actor_grads_and_vars)

        actor_op = tf.cond(
            should_apply_actor_opt,
            true_fn=make_apply_op,
            false_fn=lambda: tf.no_op())
        critic_op = self._critic_optimizer.apply_gradients(
            self._critic_grads_and_vars)
        # increment global step & apply ops
        with tf.control_dependencies([tf.assign_add(self.global_step, 1)]):
            return tf.group(actor_op, critic_op)

    @override(TFPolicy)
    def gradients(self, optimizer, loss):
        if self.config["grad_norm_clipping"] is not None:
            actor_grads_and_vars = minimize_and_clip(
                self._actor_optimizer,
                self.actor_loss,
                var_list=self.policy_vars,
                clip_val=self.config["grad_norm_clipping"])
            critic_grads_and_vars = minimize_and_clip(
                self._critic_optimizer,
                self.critic_loss,
                var_list=self.q_func_vars + self.twin_q_func_vars
                if self.config["twin_q"] else self.q_func_vars,
                clip_val=self.config["grad_norm_clipping"])
        else:
            actor_grads_and_vars = self._actor_optimizer.compute_gradients(
                self.actor_loss, var_list=self.policy_vars)
            if self.config["twin_q"]:
                critic_vars = self.q_func_vars + self.twin_q_func_vars
            else:
                critic_vars = self.q_func_vars
            critic_grads_and_vars = self._critic_optimizer.compute_gradients(
                self.critic_loss, var_list=critic_vars)
        # save these for later use in build_apply_op
        self._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                      if g is not None]
        self._critic_grads_and_vars = [(g, v)
                                       for (g, v) in critic_grads_and_vars
                                       if g is not None]
        grads_and_vars = self._actor_grads_and_vars \
            + self._critic_grads_and_vars
        return grads_and_vars

    @override(TFPolicy)
    def extra_compute_grad_fetches(self):
        return {
            "td_error": self.td_error,
            LEARNER_STATS_KEY: self.stats,
        }

    @override(TFPolicy)
    def get_weights(self):
        return self.variables.get_weights()

    @override(TFPolicy)
    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def _build_q_network(self, obs, obs_space, action_space, actions):
        if self.config["use_state_preprocessor"]:
            q_model = ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, action_space, 1, self.config["model"])
            q_out = tf.concat([q_model.last_layer, actions], axis=1)
        else:
            q_model = None
            q_out = tf.concat([obs, actions], axis=1)

        activation = getattr(tf.nn, self.config["critic_hidden_activation"])
        for hidden in self.config["critic_hiddens"]:
            q_out = tf.layers.dense(q_out, units=hidden, activation=activation)
        q_values = tf.layers.dense(q_out, units=1, activation=None)

        return q_values, q_model

    def _build_policy_network(self, obs, obs_space, action_space):
        if self.config["use_state_preprocessor"]:
            model = ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, action_space, 1, self.config["model"])
            action_out = model.last_layer
        else:
            model = None
            action_out = obs

        activation = getattr(tf.nn, self.config["actor_hidden_activation"])
        for hidden in self.config["actor_hiddens"]:
            if self.config["parameter_noise"]:
                import tensorflow.contrib.layers as layers
                action_out = layers.fully_connected(
                    action_out,
                    num_outputs=hidden,
                    activation_fn=activation,
                    normalizer_fn=layers.layer_norm)
            else:
                action_out = tf.layers.dense(
                    action_out, units=hidden, activation=activation)
        action_out = tf.layers.dense(
            action_out, units=action_space.shape[0], activation=None)

        # Use sigmoid to scale to [0,1], but also double magnitude of input to
        # emulate behaviour of tanh activation used in DDPG and TD3 papers.
        sigmoid_out = tf.nn.sigmoid(2 * action_out)
        # Rescale to actual env policy scale
        # (shape of sigmoid_out is [batch_size, dim_actions], so we reshape to
        # get same dims)
        action_range = (action_space.high - action_space.low)[None]
        low_action = action_space.low[None]
        actions = action_range * sigmoid_out + low_action

        return actions, model

    def _build_actor_critic_loss(self,
                                 q_t,
                                 q_tp1,
                                 q_t_det_policy,
                                 twin_q_t=None,
                                 twin_q_tp1=None):
        twin_q = self.config["twin_q"]
        gamma = self.config["gamma"]
        n_step = self.config["n_step"]
        use_huber = self.config["use_huber"]
        huber_threshold = self.config["huber_threshold"]

        q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
        if twin_q:
            twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
            q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

        q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
        q_tp1_best_masked = (1.0 - self.done_mask) * q_tp1_best

        # compute RHS of bellman equation
        q_t_selected_target = tf.stop_gradient(
            self.rew_t + gamma**n_step * q_tp1_best_masked)

        # compute the error (potentially clipped)
        if twin_q:
            td_error = q_t_selected - q_t_selected_target
            twin_td_error = twin_q_t_selected - q_t_selected_target
            td_error = td_error + twin_td_error
            if use_huber:
                errors = huber_loss(td_error, huber_threshold) \
                    + huber_loss(twin_td_error, huber_threshold)
            else:
                errors = 0.5 * tf.square(td_error) + 0.5 * tf.square(
                    twin_td_error)
        else:
            td_error = q_t_selected - q_t_selected_target
            if use_huber:
                errors = huber_loss(td_error, huber_threshold)
            else:
                errors = 0.5 * tf.square(td_error)

        critic_loss = tf.reduce_mean(self.importance_weights * errors)
        actor_loss = -tf.reduce_mean(q_t_det_policy)
        return critic_loss, actor_loss, td_error

    def _build_parameter_noise(self, pnet_params):
        self.parameter_noise_sigma_val = \
            self.config["exploration_config"].get("ou_sigma", 0.2)
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
        self.remove_parameter_noise_op = tf.group(*tuple(remove_noise_ops))
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

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.sess.run(self.add_noise_op)

    # support both hard and soft sync
    def update_target(self, tau=None):
        tau = tau or self.tau_value
        return self.sess.run(
            self.update_target_expr, feed_dict={self.tau: tau})
