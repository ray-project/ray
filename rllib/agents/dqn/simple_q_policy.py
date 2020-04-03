"""Basic example of a DQN policy without any optimizations."""

from gym.spaces import Discrete
import logging

import ray
from ray.rllib.agents.dqn.simple_q_model import SimpleQModel
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Categorical
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


class ParameterNoiseMixin:
    def __init__(self, obs_space, action_space, config):
        pass

    def add_parameter_noise(self):
        if self.config["parameter_noise"]:
            self.sess.run(self.add_noise_op)


class TargetNetworkMixin:
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

    @override(TFPolicy)
    def variables(self):
        return self.q_func_vars + self.target_q_func_vars


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


def get_distribution_inputs_and_class(policy,
                                      q_model,
                                      obs_batch,
                                      *,
                                      explore=True,
                                      **kwargs):
    q_vals = compute_q_values(policy, q_model, obs_batch, explore)
    q_vals = q_vals[0] if isinstance(q_vals, tuple) else q_vals

    policy.q_values = q_vals
    policy.q_func_vars = q_model.variables()
    return policy.q_values, Categorical, []  # state-outs


def build_q_losses(policy, model, dist_class, train_batch):
    # q network evaluation
    q_t = compute_q_values(
        policy,
        policy.q_model,
        train_batch[SampleBatch.CUR_OBS],
        explore=False)

    # target q network evalution
    q_tp1 = compute_q_values(
        policy,
        policy.target_q_model,
        train_batch[SampleBatch.NEXT_OBS],
        explore=False)
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


def compute_q_values(policy, model, obs, explore):
    model_out, _ = model({
        SampleBatch.CUR_OBS: obs,
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)

    return model.get_q_values(model_out)


def setup_early_mixins(policy, obs_space, action_space, config):
    ParameterNoiseMixin.__init__(policy, obs_space, action_space, config)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


SimpleQPolicy = build_tf_policy(
    name="SimpleQPolicy",
    get_default_config=lambda: ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG,
    make_model=build_q_models,
    action_distribution_fn=get_distribution_inputs_and_class,
    loss_fn=build_q_losses,
    extra_action_fetches_fn=lambda policy: {"q_values": policy.q_values},
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    before_init=setup_early_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False,
    mixins=[ParameterNoiseMixin, TargetNetworkMixin])
