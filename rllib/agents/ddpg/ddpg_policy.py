from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Box
import numpy as np
import logging

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.ddpg.ddpg_model import DDPGModel
from ray.rllib.agents.ddpg.noop_model import NoopModel
from ray.rllib.agents.dqn.dqn_policy import _postprocess_dqn, PRIO_WEIGHTS
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.tf_ops import huber_loss, minimize_and_clip, \
    make_tf_callable

tf = try_import_tf()
logger = logging.getLogger(__name__)


def build_ddpg_model(policy, obs_space, action_space, config):
    if config["model"]["custom_model"]:
        logger.warning(
            "Setting use_state_preprocessor=True since a custom model "
            "was specified.")
        config["use_state_preprocessor"] = True
    if not isinstance(action_space, Box):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for DDPG.".format(action_space))
    if len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space has multiple dimensions "
            "{}. ".format(action_space.shape) +
            "Consider reshaping this into a single dimension, "
            "using a Tuple action space, or the multi-agent API.")

    if config["use_state_preprocessor"]:
        default_model = None  # catalog decides
        num_outputs = 256  # arbitrary
        config["model"]["no_final_linear"] = True
    else:
        default_model = NoopModel
        num_outputs = int(np.product(obs_space.shape))

    policy.model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        model_interface=DDPGModel,
        default_model=default_model,
        name="ddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        parameter_noise=config["parameter_noise"],
        twin_q=config["twin_q"])

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        model_interface=DDPGModel,
        default_model=default_model,
        name="target_ddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        parameter_noise=config["parameter_noise"],
        twin_q=config["twin_q"])

    return policy.model


def postprocess_trajectory(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    if policy.config["parameter_noise"]:
        policy.adjust_param_noise_sigma(sample_batch)
    return _postprocess_dqn(policy, sample_batch)


def build_action_output(policy, model, input_dict, obs_space, action_space,
                        config):
    model_out, _ = model({
        "obs": input_dict[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)
    action_out = model.get_policy_output(model_out)

    # Use sigmoid to scale to [0,1], but also double magnitude of input to
    # emulate behaviour of tanh activation used in DDPG and TD3 papers.
    sigmoid_out = tf.nn.sigmoid(2 * action_out)
    # Rescale to actual env policy scale
    # (shape of sigmoid_out is [batch_size, dim_actions], so we reshape to
    # get same dims)
    action_range = (action_space.high - action_space.low)[None]
    low_action = action_space.low[None]
    deterministic_actions = action_range * sigmoid_out + low_action

    noise_type = config["exploration_noise_type"]
    action_low = action_space.low
    action_high = action_space.high
    action_range = action_space.high - action_low

    def compute_stochastic_actions():
        def make_noisy_actions():
            # shape of deterministic_actions is [None, dim_action]
            if noise_type == "gaussian":
                # add IID Gaussian noise for exploration, TD3-style
                normal_sample = policy.noise_scale * tf.random_normal(
                    tf.shape(deterministic_actions),
                    stddev=config["exploration_gaussian_sigma"])
                stochastic_actions = tf.clip_by_value(
                    deterministic_actions + normal_sample,
                    action_low * tf.ones_like(deterministic_actions),
                    action_high * tf.ones_like(deterministic_actions))
            elif noise_type == "ou":
                # add OU noise for exploration, DDPG-style
                zero_acts = action_low.size * [.0]
                exploration_sample = tf.get_variable(
                    name="ornstein_uhlenbeck",
                    dtype=tf.float32,
                    initializer=zero_acts,
                    trainable=False)
                normal_sample = tf.random_normal(
                    shape=[action_low.size], mean=0.0, stddev=1.0)
                ou_new = config["exploration_ou_theta"] \
                    * -exploration_sample \
                    + config["exploration_ou_sigma"] * normal_sample
                exploration_value = tf.assign_add(exploration_sample, ou_new)
                base_scale = config["exploration_ou_noise_scale"]
                noise = policy.noise_scale * base_scale \
                    * exploration_value * action_range
                stochastic_actions = tf.clip_by_value(
                    deterministic_actions + noise,
                    action_low * tf.ones_like(deterministic_actions),
                    action_high * tf.ones_like(deterministic_actions))
            else:
                raise ValueError(
                    "Unknown noise type '%s' (try 'ou' or 'gaussian')" %
                    noise_type)
            return stochastic_actions

        def make_uniform_random_actions():
            # pure random exploration option
            uniform_random_actions = tf.random_uniform(
                tf.shape(deterministic_actions))
            # rescale uniform random actions according to action range
            tf_range = tf.constant(action_range[None], dtype="float32")
            tf_low = tf.constant(action_low[None], dtype="float32")
            uniform_random_actions = uniform_random_actions * tf_range \
                + tf_low
            return uniform_random_actions

        stochastic_actions = tf.cond(
            # need to condition on noise_scale > 0 because zeroing
            # noise_scale is how a worker signals no noise should be used
            # (this is ugly and should be fixed by adding an "eval_mode"
            # config flag or something)
            tf.logical_and(policy.pure_exploration_phase,
                           policy.noise_scale > 0),
            true_fn=make_uniform_random_actions,
            false_fn=make_noisy_actions)
        return stochastic_actions

    enable_stochastic = tf.logical_and(policy.stochastic,
                                       not config["parameter_noise"])
    actions = tf.cond(enable_stochastic, compute_stochastic_actions,
                      lambda: deterministic_actions)
    policy.output_actions = actions
    return actions, None


def actor_critic_loss(policy, model, _, train_batch):
    model_out_t, _ = model({
        "obs": train_batch[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    model_out_tp1, _ = model({
        "obs": train_batch[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    target_model_out_tp1, _ = policy.target_model({
        "obs": train_batch[SampleBatch.NEXT_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    policy_t = model.get_policy_output(model_out_t)
    policy_tp1 = model.get_policy_output(model_out_tp1)

    if policy.config["smooth_target_policy"]:
        target_noise_clip = policy.config["target_noise_clip"]
        clipped_normal_sample = tf.clip_by_value(
            tf.random_normal(
                tf.shape(policy_tp1), stddev=policy.config["target_noise"]),
            -target_noise_clip, target_noise_clip)
        policy_tp1_smoothed = tf.clip_by_value(
            policy_tp1 + clipped_normal_sample,
            policy.action_space.low * tf.ones_like(policy_tp1),
            policy.action_space.high * tf.ones_like(policy_tp1))
    else:
        policy_tp1_smoothed = policy_tp1

    # q network evaluation
    q_t = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS])
    if policy.config["twin_q"]:
        twin_q_t = model.get_twin_q_values(model_out_t,
                                           train_batch[SampleBatch.ACTIONS])

    # Q-values for current policy (no noise) in given current state
    q_t_det_policy = model.get_q_values(model_out_t, policy_t)

    # target q network evaluation
    q_tp1 = policy.target_model.get_q_values(target_model_out_tp1,
                                             policy_tp1_smoothed)
    if policy.config["twin_q"]:
        twin_q_tp1 = policy.target_model.get_twin_q_values(
            target_model_out_tp1, policy_tp1_smoothed)

    q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
    if policy.config["twin_q"]:
        twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

    q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    q_tp1_best_masked = (
        1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = tf.stop_gradient(
        train_batch[SampleBatch.REWARDS] +
        policy.config["gamma"]**policy.config["n_step"] * q_tp1_best_masked)

    # compute the error (potentially clipped)
    if policy.config["twin_q"]:
        td_error = q_t_selected - q_t_selected_target
        twin_td_error = twin_q_t_selected - q_t_selected_target
        td_error = td_error + twin_td_error
        if policy.config["use_huber"]:
            errors = huber_loss(td_error, policy.config["huber_threshold"]) \
                + huber_loss(twin_td_error, policy.config["huber_threshold"])
        else:
            errors = 0.5 * tf.square(td_error) + 0.5 * tf.square(twin_td_error)
    else:
        td_error = q_t_selected - q_t_selected_target
        if policy.config["use_huber"]:
            errors = huber_loss(td_error, policy.config["huber_threshold"])
        else:
            errors = 0.5 * tf.square(td_error)

    critic_loss = model.custom_loss(
        tf.reduce_mean(
            tf.cast(train_batch[PRIO_WEIGHTS], tf.float32) * errors),
        train_batch)
    actor_loss = -tf.reduce_mean(q_t_det_policy)

    if policy.config["l2_reg"] is not None:
        for var in model.policy_variables():
            if "bias" not in var.name:
                actor_loss += policy.config["l2_reg"] * tf.nn.l2_loss(var)
        for var in model.q_variables():
            if "bias" not in var.name:
                critic_loss += policy.config["l2_reg"] * tf.nn.l2_loss(var)

    # save for stats function
    policy.q_t = q_t
    policy.td_error = td_error
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss

    # in a custom apply op we handle the losses separately, but return them
    # combined in one loss for now
    return actor_loss + critic_loss


def gradients(policy, optimizer, loss):
    if policy.config["grad_norm_clipping"] is not None:
        actor_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.actor_loss,
            var_list=policy.model.policy_variables(),
            clip_val=policy.config["grad_norm_clipping"])
        critic_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.critic_loss,
            var_list=policy.model.q_variables(),
            clip_val=policy.config["grad_norm_clipping"])
    else:
        actor_grads_and_vars = optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables())
        critic_grads_and_vars = optimizer.compute_gradients(
            policy.critic_loss, var_list=policy.model.q_variables())
    # save these for later use in build_apply_op
    policy._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                    if g is not None]
    policy._critic_grads_and_vars = [(g, v) for (g, v) in critic_grads_and_vars
                                     if g is not None]
    grads_and_vars = (
        policy._actor_grads_and_vars + policy._critic_grads_and_vars)
    return grads_and_vars


def apply_gradients(policy, optimizer, grads_and_vars):
    # for policy gradient, update policy net one time v.s.
    # update critic net `policy_delay` time(s)
    should_apply_actor_opt = tf.equal(
        tf.mod(policy.global_step, policy.config["policy_delay"]), 0)

    def make_apply_op():
        return policy._actor_optimizer.apply_gradients(
            policy._actor_grads_and_vars)

    actor_op = tf.cond(
        should_apply_actor_opt,
        true_fn=make_apply_op,
        false_fn=lambda: tf.no_op())
    critic_op = policy._critic_optimizer.apply_gradients(
        policy._critic_grads_and_vars)

    # increment global step & apply ops
    with tf.control_dependencies([tf.assign_add(policy.global_step, 1)]):
        return tf.group(actor_op, critic_op)


def stats(policy, train_batch):
    return {
        "td_error": tf.reduce_mean(policy.td_error),
        "actor_loss": tf.reduce_mean(policy.actor_loss),
        "critic_loss": tf.reduce_mean(policy.critic_loss),
        "mean_q": tf.reduce_mean(policy.q_t),
        "max_q": tf.reduce_max(policy.q_t),
        "min_q": tf.reduce_min(policy.q_t),
    }


class ExplorationStateMixin(object):
    def __init__(self, obs_space, action_space, config):
        self.cur_noise_scale = 1.0
        self.cur_pure_exploration_phase = False
        self.stochastic = tf.get_variable(
            initializer=tf.constant_initializer(True),
            name="stochastic",
            shape=(),
            trainable=False,
            dtype=tf.bool)
        self.noise_scale = tf.get_variable(
            initializer=tf.constant_initializer(self.cur_noise_scale),
            name="noise_scale",
            shape=(),
            trainable=False,
            dtype=tf.float32)
        self.pure_exploration_phase = tf.get_variable(
            initializer=tf.constant_initializer(
                self.cur_pure_exploration_phase),
            name="pure_exploration_phase",
            shape=(),
            trainable=False,
            dtype=tf.bool)

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.get_session().run(self.model.add_noise_op)

    def adjust_param_noise_sigma(self, sample_batch):
        assert not tf.executing_eagerly(), "eager not supported with p noise"
        # adjust the sigma of parameter space noise
        states, noisy_actions = [
            list(x) for x in sample_batch.columns(
                [SampleBatch.CUR_OBS, SampleBatch.ACTIONS])
        ]
        self.get_session().run(self.model.remove_noise_op)
        clean_actions = self.get_session().run(
            self.output_actions,
            feed_dict={
                self.get_placeholder(SampleBatch.CUR_OBS): states,
                self.stochastic: False,
                self.noise_scale: .0,
                self.pure_exploration_phase: False,
            })
        distance_in_action_space = np.sqrt(
            np.mean(np.square(clean_actions - noisy_actions)))
        self.model.update_action_noise(
            self.get_session(), distance_in_action_space,
            self.config["exploration_ou_sigma"], self.cur_noise_scale)

    def set_epsilon(self, epsilon):
        # set_epsilon is called by optimizer to anneal exploration as
        # necessary, and to turn it off during evaluation. The "epsilon" part
        # is a carry-over from DQN, which uses epsilon-greedy exploration
        # rather than adding action noise to the output of a policy network.
        self.cur_noise_scale = epsilon
        self.noise_scale.load(self.cur_noise_scale, self.get_session())

    def set_pure_exploration_phase(self, pure_exploration_phase):
        self.cur_pure_exploration_phase = pure_exploration_phase
        self.pure_exploration_phase.load(self.cur_pure_exploration_phase,
                                         self.get_session())

    @override(Policy)
    def get_state(self):
        return [
            TFPolicy.get_state(self), self.cur_noise_scale,
            self.cur_pure_exploration_phase
        ]

    @override(Policy)
    def set_state(self, state):
        TFPolicy.set_state(self, state[0])
        self.set_epsilon(state[1])
        self.set_pure_exploration_phase(state[2])


class TargetNetworkMixin(object):
    def __init__(self, config):
        @make_tf_callable(self.get_session())
        def update_target_fn(tau):
            tau = tf.convert_to_tensor(tau, dtype=tf.float32)
            update_target_expr = []
            model_vars = self.model.trainable_variables()
            target_model_vars = self.target_model.trainable_variables()
            assert len(model_vars) == len(target_model_vars), \
                (model_vars, target_model_vars)
            for var, var_target in zip(model_vars, target_model_vars):
                update_target_expr.append(
                    var_target.assign(tau * var + (1.0 - tau) * var_target))
                logger.debug("Update target op {}".format(var_target))
            return tf.group(*update_target_expr)

        # Hard initial update
        self._do_update = update_target_fn
        self.update_target(tau=1.0)

    # support both hard and soft sync
    def update_target(self, tau=None):
        self._do_update(np.float32(tau or self.config.get("tau")))


class ActorCriticOptimizerMixin(object):
    def __init__(self, config):
        # create global step for counting the number of update operations
        self.global_step = tf.train.get_or_create_global_step()

        # use separate optimizers for actor & critic
        self._actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["actor_lr"])
        self._critic_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["critic_lr"])


class ComputeTDErrorMixin(object):
    def __init__(self):
        @make_tf_callable(self.get_session(), dynamic_shape=True)
        def compute_td_error(obs_t, act_t, rew_t, obs_tp1, done_mask,
                             importance_weights):
            if not self.loss_initialized():
                return tf.zeros_like(rew_t)

            # Do forward pass on loss to update td error attribute
            actor_critic_loss(
                self, self.model, None, {
                    SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_t),
                    SampleBatch.ACTIONS: tf.convert_to_tensor(act_t),
                    SampleBatch.REWARDS: tf.convert_to_tensor(rew_t),
                    SampleBatch.NEXT_OBS: tf.convert_to_tensor(obs_tp1),
                    SampleBatch.DONES: tf.convert_to_tensor(done_mask),
                    PRIO_WEIGHTS: tf.convert_to_tensor(importance_weights),
                })

            return self.td_error

        self.compute_td_error = compute_td_error


def setup_early_mixins(policy, obs_space, action_space, config):
    ExplorationStateMixin.__init__(policy, obs_space, action_space, config)
    ActorCriticOptimizerMixin.__init__(policy, config)


def setup_mid_mixins(policy, obs_space, action_space, config):
    ComputeTDErrorMixin.__init__(policy)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, config)


DDPGTFPolicy = build_tf_policy(
    name="DDPGTFPolicy",
    get_default_config=lambda: ray.rllib.agents.ddpg.ddpg.DEFAULT_CONFIG,
    make_model=build_ddpg_model,
    postprocess_fn=postprocess_trajectory,
    action_sampler_fn=build_action_output,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    gradients_fn=gradients,
    apply_gradients_fn=apply_gradients,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    mixins=[
        TargetNetworkMixin, ExplorationStateMixin, ActorCriticOptimizerMixin,
        ComputeTDErrorMixin
    ],
    before_init=setup_early_mixins,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False)
