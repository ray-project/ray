from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import numpy as np
from scipy.stats import entropy
import tensorflow as tf
import tensorflow.contrib.layers as layers

import ray
from ray.rllib.models import ModelCatalog, Categorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph

Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"


class QNetwork(object):
    def __init__(self,
                 model,
                 num_actions,
                 dueling=False,
                 hiddens=[256],
                 use_noisy=False,
                 num_atoms=1,
                 v_min=-10.0,
                 v_max=10.0,
                 sigma0=0.5,
                 parameter_noise=False):
        self.model = model
        with tf.variable_scope("action_value"):
            if hiddens:
                action_out = model.last_layer
                for i in range(len(hiddens)):
                    if use_noisy:
                        action_out = self.noisy_layer(
                            "hidden_%d" % i, action_out, hiddens[i], sigma0)
                    else:
                        action_out = layers.fully_connected(
                            action_out,
                            num_outputs=hiddens[i],
                            activation_fn=tf.nn.relu,
                            normalizer_fn=layers.layer_norm
                            if parameter_noise else None)
            else:
                # Avoid postprocessing the outputs. This enables custom models
                # to be used for parametric action DQN.
                action_out = model.outputs
            if use_noisy:
                action_scores = self.noisy_layer(
                    "output",
                    action_out,
                    num_actions * num_atoms,
                    sigma0,
                    non_linear=False)
            elif hiddens:
                action_scores = layers.fully_connected(
                    action_out,
                    num_outputs=num_actions * num_atoms,
                    activation_fn=None)
            else:
                action_scores = model.outputs
            if num_atoms > 1:
                # Distributional Q-learning uses a discrete support z
                # to represent the action value distribution
                z = tf.range(num_atoms, dtype=tf.float32)
                z = v_min + z * (v_max - v_min) / float(num_atoms - 1)
                support_logits_per_action = tf.reshape(
                    tensor=action_scores, shape=(-1, num_actions, num_atoms))
                support_prob_per_action = tf.nn.softmax(
                    logits=support_logits_per_action)
                action_scores = tf.reduce_sum(
                    input_tensor=z * support_prob_per_action, axis=-1)
                self.logits = support_logits_per_action
                self.dist = support_prob_per_action
            else:
                self.logits = tf.expand_dims(tf.ones_like(action_scores), -1)
                self.dist = tf.expand_dims(tf.ones_like(action_scores), -1)

        if dueling:
            with tf.variable_scope("state_value"):
                state_out = model.last_layer
                for i in range(len(hiddens)):
                    if use_noisy:
                        state_out = self.noisy_layer("dueling_hidden_%d" % i,
                                                     state_out, hiddens[i],
                                                     sigma0)
                    else:
                        state_out = layers.fully_connected(
                            state_out,
                            num_outputs=hiddens[i],
                            activation_fn=tf.nn.relu,
                            normalizer_fn=layers.layer_norm
                            if parameter_noise else None)
                if use_noisy:
                    state_score = self.noisy_layer(
                        "dueling_output",
                        state_out,
                        num_atoms,
                        sigma0,
                        non_linear=False)
                else:
                    state_score = layers.fully_connected(
                        state_out, num_outputs=num_atoms, activation_fn=None)
            if num_atoms > 1:
                support_logits_per_action_mean = tf.reduce_mean(
                    support_logits_per_action, 1)
                support_logits_per_action_centered = (
                    support_logits_per_action - tf.expand_dims(
                        support_logits_per_action_mean, 1))
                support_logits_per_action = tf.expand_dims(
                    state_score, 1) + support_logits_per_action_centered
                support_prob_per_action = tf.nn.softmax(
                    logits=support_logits_per_action)
                self.value = tf.reduce_sum(
                    input_tensor=z * support_prob_per_action, axis=-1)
                self.logits = support_logits_per_action
                self.dist = support_prob_per_action
            else:
                action_scores_mean = _reduce_mean_ignore_inf(action_scores, 1)
                action_scores_centered = action_scores - tf.expand_dims(
                    action_scores_mean, 1)
                self.value = state_score + action_scores_centered
        else:
            self.value = action_scores

    def f_epsilon(self, x):
        return tf.sign(x) * tf.sqrt(tf.abs(x))

    def noisy_layer(self, prefix, action_in, out_size, sigma0,
                    non_linear=True):
        """
        a common dense layer: y = w^{T}x + b
        a noisy layer: y = (w + \epsilon_w*\sigma_w)^{T}x +
            (b+\epsilon_b*\sigma_b)
        where \epsilon are random variables sampled from factorized normal
        distributions and \sigma are trainable variables which are expected to
        vanish along the training procedure
        """
        in_size = int(action_in.shape[1])

        epsilon_in = tf.random_normal(shape=[in_size])
        epsilon_out = tf.random_normal(shape=[out_size])
        epsilon_in = self.f_epsilon(epsilon_in)
        epsilon_out = self.f_epsilon(epsilon_out)
        epsilon_w = tf.matmul(
            a=tf.expand_dims(epsilon_in, -1), b=tf.expand_dims(epsilon_out, 0))
        epsilon_b = epsilon_out
        sigma_w = tf.get_variable(
            name=prefix + "_sigma_w",
            shape=[in_size, out_size],
            dtype=tf.float32,
            initializer=tf.random_uniform_initializer(
                minval=-1.0 / np.sqrt(float(in_size)),
                maxval=1.0 / np.sqrt(float(in_size))))
        # TF noise generation can be unreliable on GPU
        # If generating the noise on the CPU,
        # lowering sigma0 to 0.1 may be helpful
        sigma_b = tf.get_variable(
            name=prefix + "_sigma_b",
            shape=[out_size],
            dtype=tf.float32,  # 0.5~GPU, 0.1~CPU
            initializer=tf.constant_initializer(
                sigma0 / np.sqrt(float(in_size))))

        w = tf.get_variable(
            name=prefix + "_fc_w",
            shape=[in_size, out_size],
            dtype=tf.float32,
            initializer=layers.xavier_initializer())
        b = tf.get_variable(
            name=prefix + "_fc_b",
            shape=[out_size],
            dtype=tf.float32,
            initializer=tf.zeros_initializer())

        action_activation = tf.nn.xw_plus_b(action_in, w + sigma_w * epsilon_w,
                                            b + sigma_b * epsilon_b)

        if not non_linear:
            return action_activation
        return tf.nn.relu(action_activation)


class QValuePolicy(object):
    def __init__(self, q_values, observations, num_actions, stochastic, eps,
                 softmax, softmax_temp):
        if softmax:
            action_dist = Categorical(q_values / softmax_temp)
            self.action = action_dist.sample()
            self.action_prob = action_dist.sampled_action_prob()
            return

        deterministic_actions = tf.argmax(q_values, axis=1)
        batch_size = tf.shape(observations)[0]

        # Special case masked out actions (q_value ~= -inf) so that we don't
        # even consider them for exploration.
        random_valid_action_logits = tf.where(
            tf.equal(q_values, tf.float32.min),
            tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
        random_actions = tf.squeeze(
            tf.multinomial(random_valid_action_logits, 1), axis=1)

        chose_random = tf.random_uniform(
            tf.stack([batch_size]), minval=0, maxval=1, dtype=tf.float32) < eps
        stochastic_actions = tf.where(chose_random, random_actions,
                                      deterministic_actions)
        self.action = tf.cond(stochastic, lambda: stochastic_actions,
                              lambda: deterministic_actions)
        self.action_prob = None


class QLoss(object):
    def __init__(self,
                 q_t_selected,
                 q_logits_t_selected,
                 q_tp1_best,
                 q_dist_tp1_best,
                 importance_weights,
                 rewards,
                 done_mask,
                 gamma=0.99,
                 n_step=1,
                 num_atoms=1,
                 v_min=-10.0,
                 v_max=10.0):

        if num_atoms > 1:
            # Distributional Q-learning which corresponds to an entropy loss

            z = tf.range(num_atoms, dtype=tf.float32)
            z = v_min + z * (v_max - v_min) / float(num_atoms - 1)

            # (batch_size, 1) * (1, num_atoms) = (batch_size, num_atoms)
            r_tau = tf.expand_dims(
                rewards, -1) + gamma**n_step * tf.expand_dims(
                    1.0 - done_mask, -1) * tf.expand_dims(z, 0)
            r_tau = tf.clip_by_value(r_tau, v_min, v_max)
            b = (r_tau - v_min) / ((v_max - v_min) / float(num_atoms - 1))
            lb = tf.floor(b)
            ub = tf.ceil(b)
            # indispensable judgement which is missed in most implementations
            # when b happens to be an integer, lb == ub, so pr_j(s', a*) will
            # be discarded because (ub-b) == (b-lb) == 0
            floor_equal_ceil = tf.to_float(tf.less(ub - lb, 0.5))

            l_project = tf.one_hot(
                tf.cast(lb, dtype=tf.int32),
                num_atoms)  # (batch_size, num_atoms, num_atoms)
            u_project = tf.one_hot(
                tf.cast(ub, dtype=tf.int32),
                num_atoms)  # (batch_size, num_atoms, num_atoms)
            ml_delta = q_dist_tp1_best * (ub - b + floor_equal_ceil)
            mu_delta = q_dist_tp1_best * (b - lb)
            ml_delta = tf.reduce_sum(
                l_project * tf.expand_dims(ml_delta, -1), axis=1)
            mu_delta = tf.reduce_sum(
                u_project * tf.expand_dims(mu_delta, -1), axis=1)
            m = ml_delta + mu_delta

            # Rainbow paper claims that using this cross entropy loss for
            # priority is robust and insensitive to `prioritized_replay_alpha`
            self.td_error = tf.nn.softmax_cross_entropy_with_logits(
                labels=m, logits=q_logits_t_selected)
            self.loss = tf.reduce_mean(self.td_error * importance_weights)
            self.stats = {
                # TODO: better Q stats for dist dqn
                "mean_td_error": tf.reduce_mean(self.td_error),
            }
        else:
            q_tp1_best_masked = (1.0 - done_mask) * q_tp1_best

            # compute RHS of bellman equation
            q_t_selected_target = rewards + gamma**n_step * q_tp1_best_masked

            # compute the error (potentially clipped)
            self.td_error = (
                q_t_selected - tf.stop_gradient(q_t_selected_target))
            self.loss = tf.reduce_mean(
                importance_weights * _huber_loss(self.td_error))
            self.stats = {
                "mean_q": tf.reduce_mean(q_t_selected),
                "min_q": tf.reduce_min(q_t_selected),
                "max_q": tf.reduce_max(q_t_selected),
                "mean_td_error": tf.reduce_mean(self.td_error),
            }


class DQNPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG, **config)
        if not isinstance(action_space, Discrete):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DQN.".format(
                    action_space))

        self.config = config
        self.cur_epsilon = 1.0
        self.num_actions = action_space.n

        # Action inputs
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")
        self.cur_observations = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)

        # Action Q network
        with tf.variable_scope(Q_SCOPE) as scope:
            q_values, q_logits, q_dist, _ = self._build_q_network(
                self.cur_observations, observation_space, action_space)
            self.q_values = q_values
            self.q_func_vars = _scope_vars(scope.name)

        # Noise vars for Q network except for layer normalization vars
        if self.config["parameter_noise"]:
            self._build_parameter_noise([
                var for var in self.q_func_vars if "LayerNorm" not in var.name
            ])
            self.action_probs = tf.nn.softmax(self.q_values)

        # Action outputs
        self.output_actions, self.action_prob = self._build_q_value_policy(
            q_values)

        # Replay inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)
        self.act_t = tf.placeholder(tf.int32, [None], name="action")
        self.rew_t = tf.placeholder(tf.float32, [None], name="reward")
        self.obs_tp1 = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)
        self.done_mask = tf.placeholder(tf.float32, [None], name="done")
        self.importance_weights = tf.placeholder(
            tf.float32, [None], name="weight")

        # q network evaluation
        with tf.variable_scope(Q_SCOPE, reuse=True):
            prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
            q_t, q_logits_t, q_dist_t, model = self._build_q_network(
                self.obs_t, observation_space, action_space)
            q_batchnorm_update_ops = list(
                set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) -
                prev_update_ops)

        # target q network evalution
        with tf.variable_scope(Q_TARGET_SCOPE) as scope:
            q_tp1, q_logits_tp1, q_dist_tp1, _ = self._build_q_network(
                self.obs_tp1, observation_space, action_space)
            self.target_q_func_vars = _scope_vars(scope.name)

        # q scores for actions which we know were selected in the given state.
        one_hot_selection = tf.one_hot(self.act_t, self.num_actions)
        q_t_selected = tf.reduce_sum(q_t * one_hot_selection, 1)
        q_logits_t_selected = tf.reduce_sum(
            q_logits_t * tf.expand_dims(one_hot_selection, -1), 1)

        # compute estimate of best possible value starting from state at t + 1
        if config["double_q"]:
            with tf.variable_scope(Q_SCOPE, reuse=True):
                q_tp1_using_online_net, q_logits_tp1_using_online_net, \
                    q_dist_tp1_using_online_net, _ = self._build_q_network(
                        self.obs_tp1, observation_space, action_space)
            q_tp1_best_using_online_net = tf.argmax(q_tp1_using_online_net, 1)
            q_tp1_best_one_hot_selection = tf.one_hot(
                q_tp1_best_using_online_net, self.num_actions)
            q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
            q_dist_tp1_best = tf.reduce_sum(
                q_dist_tp1 * tf.expand_dims(q_tp1_best_one_hot_selection, -1),
                1)
        else:
            q_tp1_best_one_hot_selection = tf.one_hot(
                tf.argmax(q_tp1, 1), self.num_actions)
            q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
            q_dist_tp1_best = tf.reduce_sum(
                q_dist_tp1 * tf.expand_dims(q_tp1_best_one_hot_selection, -1),
                1)

        self.loss = self._build_q_loss(q_t_selected, q_logits_t_selected,
                                       q_tp1_best, q_dist_tp1_best)

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
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=self.cur_observations,
            action_sampler=self.output_actions,
            action_prob=self.action_prob,
            loss=self.loss.loss,
            model=model,
            loss_inputs=self.loss_inputs,
            update_ops=q_batchnorm_update_ops)
        self.sess.run(tf.global_variables_initializer())

    @override(TFPolicyGraph)
    def optimizer(self):
        return tf.train.AdamOptimizer(
            learning_rate=self.config["lr"],
            epsilon=self.config["adam_epsilon"])

    @override(TFPolicyGraph)
    def gradients(self, optimizer, loss):
        if self.config["grad_norm_clipping"] is not None:
            grads_and_vars = _minimize_and_clip(
                optimizer,
                loss,
                var_list=self.q_func_vars,
                clip_val=self.config["grad_norm_clipping"])
        else:
            grads_and_vars = optimizer.compute_gradients(
                loss, var_list=self.q_func_vars)
        grads_and_vars = [(g, v) for (g, v) in grads_and_vars if g is not None]
        return grads_and_vars

    @override(TFPolicyGraph)
    def extra_compute_action_fetches(self):
        return dict(
            TFPolicyGraph.extra_compute_action_fetches(self), **{
                "q_values": self.q_values,
            })

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
            "stats": self.loss.stats,
        }

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        if self.config["parameter_noise"]:
            # adjust the sigma of parameter space noise
            states = [list(x) for x in sample_batch.columns(["obs"])][0]

            noisy_action_distribution = self.sess.run(
                self.action_probs, feed_dict={self.cur_observations: states})
            self.sess.run(self.remove_noise_op)
            clean_action_distribution = self.sess.run(
                self.action_probs, feed_dict={self.cur_observations: states})
            distance_in_action_space = np.mean(
                entropy(clean_action_distribution.T,
                        noisy_action_distribution.T))
            self.pi_distance = distance_in_action_space
            if (distance_in_action_space <
                    -np.log(1 - self.cur_epsilon +
                            self.cur_epsilon / self.num_actions)):
                self.parameter_noise_sigma_val *= 1.01
            else:
                self.parameter_noise_sigma_val /= 1.01
            self.parameter_noise_sigma.load(
                self.parameter_noise_sigma_val, session=self.sess)

        return _postprocess_dqn(self, sample_batch)

    @override(PolicyGraph)
    def get_state(self):
        return [TFPolicyGraph.get_state(self), self.cur_epsilon]

    @override(PolicyGraph)
    def set_state(self, state):
        TFPolicyGraph.set_state(self, state[0])
        self.set_epsilon(state[1])

    def _build_parameter_noise(self, pnet_params):
        self.parameter_noise_sigma_val = 1.0
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
                name=var.name.split(':')[0] + "_noise",
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

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.sess.run(self.add_noise_op)

    def update_target(self):
        return self.sess.run(self.update_target_expr)

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon

    def _build_q_network(self, obs, obs_space, action_space):
        qnet = QNetwork(
            ModelCatalog.get_model({
                "obs": obs,
                "is_training": self._get_is_training_placeholder(),
            }, obs_space, action_space, self.num_actions,
                                   self.config["model"]), self.num_actions,
            self.config["dueling"], self.config["hiddens"],
            self.config["noisy"], self.config["num_atoms"],
            self.config["v_min"], self.config["v_max"], self.config["sigma0"],
            self.config["parameter_noise"])
        return qnet.value, qnet.logits, qnet.dist, qnet.model

    def _build_q_value_policy(self, q_values):
        policy = QValuePolicy(
            q_values, self.cur_observations, self.num_actions, self.stochastic,
            self.eps, self.config["soft_q"], self.config["softmax_temp"])
        return policy.action, policy.action_prob

    def _build_q_loss(self, q_t_selected, q_logits_t_selected, q_tp1_best,
                      q_dist_tp1_best):
        return QLoss(q_t_selected, q_logits_t_selected, q_tp1_best,
                     q_dist_tp1_best, self.importance_weights, self.rew_t,
                     self.done_mask, self.config["gamma"],
                     self.config["n_step"], self.config["num_atoms"],
                     self.config["v_min"], self.config["v_max"])


def _adjust_nstep(n_step, gamma, obs, actions, rewards, new_obs, dones):
    """Rewrites the given trajectory fragments to encode n-step rewards.

    reward[i] = (
        reward[i] * gamma**0 +
        reward[i+1] * gamma**1 +
        ... +
        reward[i+n_step-1] * gamma**(n_step-1))

    The ith new_obs is also adjusted to point to the (i+n_step-1)'th new obs.

    At the end of the trajectory, n is truncated to fit in the traj length.
    """

    assert not any(dones[:-1]), "Unexpected done in middle of trajectory"

    traj_length = len(rewards)
    for i in range(traj_length):
        for j in range(1, n_step):
            if i + j < traj_length:
                new_obs[i] = new_obs[i + j]
                dones[i] = dones[i + j]
                rewards[i] += gamma**j * rewards[i + j]


def _postprocess_dqn(policy_graph, batch):
    # N-step Q adjustments
    if policy_graph.config["n_step"] > 1:
        _adjust_nstep(policy_graph.config["n_step"],
                      policy_graph.config["gamma"], batch["obs"],
                      batch["actions"], batch["rewards"], batch["new_obs"],
                      batch["dones"])

    if "weights" not in batch:
        batch["weights"] = np.ones_like(batch["rewards"])

    # Prioritize on the worker side
    if batch.count > 0 and policy_graph.config["worker_side_prioritization"]:
        td_errors = policy_graph.compute_td_error(
            batch["obs"], batch["actions"], batch["rewards"], batch["new_obs"],
            batch["dones"], batch["weights"])
        new_priorities = (
            np.abs(td_errors) + policy_graph.config["prioritized_replay_eps"])
        batch.data["weights"] = new_priorities

    return batch


def _reduce_mean_ignore_inf(x, axis):
    """Same as tf.reduce_mean() but ignores -inf values."""
    mask = tf.not_equal(x, tf.float32.min)
    x_zeroed = tf.where(mask, x, tf.zeros_like(x))
    return (tf.reduce_sum(x_zeroed, axis) / tf.reduce_sum(
        tf.cast(mask, tf.float32), axis))


def _huber_loss(x, delta=1.0):
    """Reference: https://en.wikipedia.org/wiki/Huber_loss"""
    return tf.where(
        tf.abs(x) < delta,
        tf.square(x) * 0.5, delta * (tf.abs(x) - 0.5 * delta))


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
