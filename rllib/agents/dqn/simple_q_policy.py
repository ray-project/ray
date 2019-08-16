from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Basic example of a DQN policy without any optimizations."""

from gym.spaces import Discrete
import logging

import ray
from ray.rllib.agents.dqn.simple_q_model import SimpleQModel
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.tf_ops import huber_loss, make_tf_callable

tf = try_import_tf()
logger = logging.getLogger(__name__)

Q_SCOPE = "q_func"
Q_TARGET_SCOPE = "target_q_func"


class ExplorationStateMixin(object):
    def __init__(self, obs_space, action_space, config):
        # Python value, should always be same as the TF variable
        self.cur_epsilon_value = 1.0
        self.cur_epsilon = tf.get_variable(
            initializer=tf.constant_initializer(self.cur_epsilon_value),
            name="eps",
            shape=(),
            trainable=False,
            dtype=tf.float32)

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.sess.run(self.add_noise_op)

    def set_epsilon(self, epsilon):
        self.cur_epsilon_value = epsilon
        self.cur_epsilon.load(
            self.cur_epsilon_value, session=self.get_session())

    @override(Policy)
    def get_state(self):
        return [TFPolicy.get_state(self), self.cur_epsilon_value]

    @override(Policy)
    def set_state(self, state):
        TFPolicy.set_state(self, state[0])
        self.set_epsilon(state[1])


class TargetNetworkMixin(object):
    def __init__(self, obs_space, action_space, config):
        @make_tf_callable(self.get_session())
        def do_update():
            # update_target_fn will be called periodically to copy Q network to
            # target Q network
            update_target_expr = []
            assert len(self.q_func_vars) == len(self.target_q_func_vars), \
                (self.q_func_vars, self.target_q_func_vars)
            for var, var_target in zip(self.q_func_vars,
                                       self.target_q_func_vars):
                update_target_expr.append(var_target.assign(var))
                logger.debug("Update target op {}".format(var_target))
            return tf.group(*update_target_expr)

        self.update_target = do_update


def build_q_models(policy, obs_space, action_space, config):

    if not isinstance(action_space, Discrete):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for DQN.".format(action_space))

    if config["hiddens"]:
        num_outputs = 256
        config["model"]["no_final_linear"] = True
    else:
        num_outputs = action_space.n

    policy.q_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        name=Q_SCOPE,
        model_interface=SimpleQModel,
        q_hiddens=config["hiddens"])

    policy.target_q_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        name=Q_TARGET_SCOPE,
        model_interface=SimpleQModel,
        q_hiddens=config["hiddens"])

    return policy.q_model


def build_action_sampler(policy, q_model, input_dict, obs_space, action_space,
                         config):

    # Action Q network
    q_values = _compute_q_values(policy, q_model,
                                 input_dict[SampleBatch.CUR_OBS], obs_space,
                                 action_space)
    policy.q_values = q_values
    policy.q_func_vars = q_model.variables()

    # Action outputs
    deterministic_actions = tf.argmax(q_values, axis=1)
    batch_size = tf.shape(input_dict[SampleBatch.CUR_OBS])[0]

    # Special case masked out actions (q_value ~= -inf) so that we don't
    # even consider them for exploration.
    random_valid_action_logits = tf.where(
        tf.equal(q_values, tf.float32.min),
        tf.ones_like(q_values) * tf.float32.min, tf.ones_like(q_values))
    random_actions = tf.squeeze(
        tf.multinomial(random_valid_action_logits, 1), axis=1)

    chose_random = tf.random_uniform(
        tf.stack([batch_size]), minval=0, maxval=1,
        dtype=tf.float32) < policy.cur_epsilon
    stochastic_actions = tf.where(chose_random, random_actions,
                                  deterministic_actions)
    action_logp = None

    return stochastic_actions, action_logp


def build_q_losses(policy, model, dist_class, train_batch):
    # q network evaluation
    q_t = _compute_q_values(policy, policy.q_model,
                            train_batch[SampleBatch.CUR_OBS],
                            policy.observation_space, policy.action_space)

    # target q network evalution
    q_tp1 = _compute_q_values(policy, policy.target_q_model,
                              train_batch[SampleBatch.NEXT_OBS],
                              policy.observation_space, policy.action_space)
    policy.target_q_func_vars = policy.target_q_model.variables()

    # q scores for actions which we know were selected in the given state.
    one_hot_selection = tf.one_hot(
        tf.cast(train_batch[SampleBatch.ACTIONS], tf.int32),
        policy.action_space.n)
    q_t_selected = tf.reduce_sum(q_t * one_hot_selection, 1)

    # compute estimate of best possible value starting from state at t + 1
    dones = tf.cast(train_batch[SampleBatch.DONES], tf.float32)
    q_tp1_best_one_hot_selection = tf.one_hot(
        tf.argmax(q_tp1, 1), policy.action_space.n)
    q_tp1_best = tf.reduce_sum(q_tp1 * q_tp1_best_one_hot_selection, 1)
    q_tp1_best_masked = (1.0 - dones) * q_tp1_best

    # compute RHS of bellman equation
    q_t_selected_target = (train_batch[SampleBatch.REWARDS] +
                           policy.config["gamma"] * q_tp1_best_masked)

    # compute the error (potentially clipped)
    td_error = q_t_selected - tf.stop_gradient(q_t_selected_target)
    loss = tf.reduce_mean(huber_loss(td_error))

    # save TD error as an attribute for outside access
    policy.td_error = td_error

    return loss


def _compute_q_values(policy, model, obs, obs_space, action_space):
    input_dict = {
        "obs": obs,
        "is_training": policy._get_is_training_placeholder(),
    }
    model_out, _ = model(input_dict, [], None)
    return model.get_q_values(model_out)


def setup_early_mixins(policy, obs_space, action_space, config):
    ExplorationStateMixin.__init__(policy, obs_space, action_space, config)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


SimpleQPolicy = build_tf_policy(
    name="SimpleQPolicy",
    get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
    make_model=build_q_models,
    action_sampler_fn=build_action_sampler,
    loss_fn=build_q_losses,
    extra_action_fetches_fn=lambda policy: {"q_values": policy.q_values},
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    before_init=setup_early_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False,
    mixins=[
        ExplorationStateMixin,
        TargetNetworkMixin,
    ])
