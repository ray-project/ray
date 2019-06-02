from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Discrete
import numpy as np
from scipy.stats import entropy

import ray
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models import ModelCatalog, Categorical
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.tf_policy import TFPolicy, \
    LearningRateSchedule
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"

# Importance sampling weights for prioritized replay
PRIO_WEIGHTS = "weights"


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
                    elif parameter_noise:
                        import tensorflow.contrib.layers as layers
                        action_out = layers.fully_connected(
                            action_out,
                            num_outputs=hiddens[i],
                            activation_fn=tf.nn.relu,
                            normalizer_fn=layers.layer_norm)
                    else:
                        action_out = tf.layers.dense(
                            action_out,
                            units=hiddens[i],
                            activation=tf.nn.relu)
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
                action_scores = tf.layers.dense(
                    action_out, units=num_actions * num_atoms, activation=None)
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
                    elif parameter_noise:
                        state_out = tf.contrib.layers.fully_connected(
                            state_out,
                            num_outputs=hiddens[i],
                            activation_fn=tf.nn.relu,
                            normalizer_fn=tf.contrib.layers.layer_norm)
                    else:
                        state_out = tf.layers.dense(
                            state_out, units=hiddens[i], activation=tf.nn.relu)
                if use_noisy:
                    state_score = self.noisy_layer(
                        "dueling_output",
                        state_out,
                        num_atoms,
                        sigma0,
                        non_linear=False)
                else:
                    state_score = tf.layers.dense(
                        state_out, units=num_atoms, activation=None)
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
        import tensorflow.contrib.layers as layers

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


class ExplorationStateMixin(object):
    def __init__(self, obs_space, action_space, config):
        self.cur_epsilon = 1.0
        self.stochastic = tf.placeholder(tf.bool, (), name="stochastic")
        self.eps = tf.placeholder(tf.float32, (), name="eps")

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.sess.run(self.add_noise_op)

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon

    @override(Policy)
    def get_state(self):
        return [TFPolicy.get_state(self), self.cur_epsilon]

    @override(Policy)
    def set_state(self, state):
        TFPolicy.set_state(self, state[0])
        self.set_epsilon(state[1])


class TargetNetworkMixin(object):
    def __init__(self, obs_space, action_space, config):
        # update_target_fn will be called periodically to copy Q network to
        # target Q network
        update_target_expr = []
        assert len(self.q_func_vars) == len(self.target_q_func_vars), \
            (self.q_func_vars, self.target_q_func_vars)
        for var, var_target in zip(self.q_func_vars, self.target_q_func_vars):
            update_target_expr.append(var_target.assign(var))
        self.update_target_expr = tf.group(*update_target_expr)

    def update_target(self):
        return self.get_session().run(self.update_target_expr)


class ComputeTDErrorMixin(object):
    def compute_td_error(self, obs_t, act_t, rew_t, obs_tp1, done_mask,
                         importance_weights):
        if not self.loss_initialized():
            return np.zeros_like(rew_t)

        td_err = self.get_session().run(
            self.loss.td_error,
            feed_dict={
                self.get_placeholder(SampleBatch.CUR_OBS): [
                    np.array(ob) for ob in obs_t
                ],
                self.get_placeholder(SampleBatch.ACTIONS): act_t,
                self.get_placeholder(SampleBatch.REWARDS): rew_t,
                self.get_placeholder(SampleBatch.NEXT_OBS): [
                    np.array(ob) for ob in obs_tp1
                ],
                self.get_placeholder(SampleBatch.DONES): done_mask,
                self.get_placeholder(PRIO_WEIGHTS): importance_weights,
            })
        return td_err


def postprocess_trajectory(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    if policy.config["parameter_noise"]:
        # adjust the sigma of parameter space noise
        states = [list(x) for x in sample_batch.columns(["obs"])][0]

        noisy_action_distribution = policy.get_session().run(
            policy.action_probs, feed_dict={policy.cur_observations: states})
        policy.get_session().run(policy.remove_noise_op)
        clean_action_distribution = policy.get_session().run(
            policy.action_probs, feed_dict={policy.cur_observations: states})
        distance_in_action_space = np.mean(
            entropy(clean_action_distribution.T, noisy_action_distribution.T))
        policy.pi_distance = distance_in_action_space
        if (distance_in_action_space <
                -np.log(1 - policy.cur_epsilon +
                        policy.cur_epsilon / policy.num_actions)):
            policy.parameter_noise_sigma_val *= 1.01
        else:
            policy.parameter_noise_sigma_val /= 1.01
        policy.parameter_noise_sigma.load(
            policy.parameter_noise_sigma_val, session=policy.get_session())

    return _postprocess_dqn(policy, sample_batch)


def build_q_networks(policy, input_dict, observation_space, action_space,
                     config):

    if not isinstance(action_space, Discrete):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for DQN.".format(action_space))

    # Action Q network
    with tf.variable_scope(Q_SCOPE) as scope:
        q_values, q_logits, q_dist, _ = _build_q_network(
            policy, input_dict[SampleBatch.CUR_OBS], observation_space,
            action_space)
        policy.q_values = q_values
        policy.q_func_vars = _scope_vars(scope.name)

    # Noise vars for Q network except for layer normalization vars
    if config["parameter_noise"]:
        _build_parameter_noise(
            policy,
            [var for var in policy.q_func_vars if "LayerNorm" not in var.name])
        policy.action_probs = tf.nn.softmax(policy.q_values)

    # Action outputs
    qvp = QValuePolicy(q_values, input_dict[SampleBatch.CUR_OBS],
                       action_space.n, policy.stochastic, policy.eps,
                       policy.config["soft_q"], policy.config["softmax_temp"])
    policy.output_actions, policy.action_prob = qvp.action, qvp.action_prob

    return policy.output_actions, policy.action_prob


def _build_parameter_noise(policy, pnet_params):
    policy.parameter_noise_sigma_val = 1.0
    policy.parameter_noise_sigma = tf.get_variable(
        initializer=tf.constant_initializer(policy.parameter_noise_sigma_val),
        name="parameter_noise_sigma",
        shape=(),
        trainable=False,
        dtype=tf.float32)
    policy.parameter_noise = list()
    # No need to add any noise on LayerNorm parameters
    for var in pnet_params:
        noise_var = tf.get_variable(
            name=var.name.split(":")[0] + "_noise",
            shape=var.shape,
            initializer=tf.constant_initializer(.0),
            trainable=False)
        policy.parameter_noise.append(noise_var)
    remove_noise_ops = list()
    for var, var_noise in zip(pnet_params, policy.parameter_noise):
        remove_noise_ops.append(tf.assign_add(var, -var_noise))
    policy.remove_noise_op = tf.group(*tuple(remove_noise_ops))
    generate_noise_ops = list()
    for var_noise in policy.parameter_noise:
        generate_noise_ops.append(
            tf.assign(
                var_noise,
                tf.random_normal(
                    shape=var_noise.shape,
                    stddev=policy.parameter_noise_sigma)))
    with tf.control_dependencies(generate_noise_ops):
        add_noise_ops = list()
        for var, var_noise in zip(pnet_params, policy.parameter_noise):
            add_noise_ops.append(tf.assign_add(var, var_noise))
        policy.add_noise_op = tf.group(*tuple(add_noise_ops))
    policy.pi_distance = None


def build_q_losses(policy, batch_tensors):
    # q network evaluation
    with tf.variable_scope(Q_SCOPE, reuse=True):
        prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
        q_t, q_logits_t, q_dist_t, model = _build_q_network(
            policy, batch_tensors[SampleBatch.CUR_OBS],
            policy.observation_space, policy.action_space)
        policy.q_batchnorm_update_ops = list(
            set(tf.get_collection(tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)

    # target q network evalution
    with tf.variable_scope(Q_TARGET_SCOPE) as scope:
        q_tp1, q_logits_tp1, q_dist_tp1, _ = _build_q_network(
            policy, batch_tensors[SampleBatch.NEXT_OBS],
            policy.observation_space, policy.action_space)
        policy.target_q_func_vars = _scope_vars(scope.name)

    # q scores for actions which we know were selected in the given state.
    one_hot_selection = tf.one_hot(batch_tensors[SampleBatch.ACTIONS],
                                   policy.action_space.n)
    q_t_selected = tf.reduce_sum(q_t * one_hot_selection, 1)
    q_logits_t_selected = tf.reduce_sum(
        q_logits_t * tf.expand_dims(one_hot_selection, -1), 1)

    # compute estimate of best possible value starting from state at t + 1
    if policy.config["double_q"]:
        with tf.variable_scope(Q_SCOPE, reuse=True):
            q_tp1_using_online_net, q_logits_tp1_using_online_net, \
                q_dist_tp1_using_online_net, _ = _build_q_network(
                    policy,
                    batch_tensors[SampleBatch.NEXT_OBS],
                    policy.observation_space, policy.action_space)
        q_tp1_best_using_online_net = tf.argmax(q_tp1_using_online_net, 1)
        q_tp1_best_one_hot_selection = tf.one_hot(q_tp1_best_using_online_net,
                                                  policy.action_space.n)
        q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
        q_dist_tp1_best = tf.reduce_sum(
            q_dist_tp1 * tf.expand_dims(q_tp1_best_one_hot_selection, -1), 1)
    else:
        q_tp1_best_one_hot_selection = tf.one_hot(
            tf.argmax(q_tp1, 1), policy.action_space.n)
        q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
        q_dist_tp1_best = tf.reduce_sum(
            q_dist_tp1 * tf.expand_dims(q_tp1_best_one_hot_selection, -1), 1)

    policy.loss = _build_q_loss(
        q_t_selected, q_logits_t_selected, q_tp1_best, q_dist_tp1_best,
        batch_tensors[SampleBatch.REWARDS], batch_tensors[SampleBatch.DONES],
        batch_tensors[PRIO_WEIGHTS], policy.config)

    return policy.loss.loss


def adam_optimizer(policy, config):
    return tf.train.AdamOptimizer(
        learning_rate=policy.cur_lr, epsilon=config["adam_epsilon"])


def clip_gradients(policy, optimizer, loss):
    if policy.config["grad_norm_clipping"] is not None:
        grads_and_vars = _minimize_and_clip(
            optimizer,
            loss,
            var_list=policy.q_func_vars,
            clip_val=policy.config["grad_norm_clipping"])
    else:
        grads_and_vars = optimizer.compute_gradients(
            loss, var_list=policy.q_func_vars)
    grads_and_vars = [(g, v) for (g, v) in grads_and_vars if g is not None]
    return grads_and_vars


def exploration_setting_inputs(policy):
    return {
        policy.stochastic: True,
        policy.eps: policy.cur_epsilon,
    }


def build_q_stats(policy, batch_tensors):
    return dict({
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
    }, **policy.loss.stats)


def setup_early_mixins(policy, obs_space, action_space, config):
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    ExplorationStateMixin.__init__(policy, obs_space, action_space, config)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


def _build_q_network(policy, obs, obs_space, action_space):
    config = policy.config
    qnet = QNetwork(
        ModelCatalog.get_model({
            "obs": obs,
            "is_training": policy._get_is_training_placeholder(),
        }, obs_space, action_space, action_space.n, config["model"]),
        action_space.n, config["dueling"], config["hiddens"], config["noisy"],
        config["num_atoms"], config["v_min"], config["v_max"],
        config["sigma0"], config["parameter_noise"])
    return qnet.value, qnet.logits, qnet.dist, qnet.model


def _build_q_value_policy(policy, q_values):
    policy = QValuePolicy(q_values, policy.cur_observations,
                          policy.num_actions, policy.stochastic, policy.eps,
                          policy.config["soft_q"],
                          policy.config["softmax_temp"])
    return policy.action, policy.action_prob


def _build_q_loss(q_t_selected, q_logits_t_selected, q_tp1_best,
                  q_dist_tp1_best, rewards, dones, importance_weights, config):
    return QLoss(q_t_selected, q_logits_t_selected, q_tp1_best,
                 q_dist_tp1_best, importance_weights, rewards,
                 tf.cast(dones, tf.float32), config["gamma"], config["n_step"],
                 config["num_atoms"], config["v_min"], config["v_max"])


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


def _postprocess_dqn(policy, batch):
    # N-step Q adjustments
    if policy.config["n_step"] > 1:
        _adjust_nstep(policy.config["n_step"], policy.config["gamma"],
                      batch[SampleBatch.CUR_OBS], batch[SampleBatch.ACTIONS],
                      batch[SampleBatch.REWARDS], batch[SampleBatch.NEXT_OBS],
                      batch[SampleBatch.DONES])

    if PRIO_WEIGHTS not in batch:
        batch[PRIO_WEIGHTS] = np.ones_like(batch[SampleBatch.REWARDS])

    # Prioritize on the worker side
    if batch.count > 0 and policy.config["worker_side_prioritization"]:
        td_errors = policy.compute_td_error(
            batch[SampleBatch.CUR_OBS], batch[SampleBatch.ACTIONS],
            batch[SampleBatch.REWARDS], batch[SampleBatch.NEXT_OBS],
            batch[SampleBatch.DONES], batch[PRIO_WEIGHTS])
        new_priorities = (
            np.abs(td_errors) + policy.config["prioritized_replay_eps"])
        batch.data[PRIO_WEIGHTS] = new_priorities

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


DQNTFPolicy = build_tf_policy(
    name="DQNTFPolicy",
    get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
    make_action_sampler=build_q_networks,
    loss_fn=build_q_losses,
    stats_fn=build_q_stats,
    postprocess_fn=postprocess_trajectory,
    optimizer_fn=adam_optimizer,
    gradients_fn=clip_gradients,
    extra_action_feed_fn=exploration_setting_inputs,
    extra_action_fetches_fn=lambda policy: {"q_values": policy.q_values},
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.loss.td_error},
    update_ops_fn=lambda policy: policy.q_batchnorm_update_ops,
    before_init=setup_early_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False,
    mixins=[
        ExplorationStateMixin,
        TargetNetworkMixin,
        ComputeTDErrorMixin,
        LearningRateSchedule,
    ])
