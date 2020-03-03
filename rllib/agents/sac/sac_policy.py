import sys
from gym.spaces import Box, Discrete
import numpy as np
import logging

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.sac.sac_model import SACModel
from ray.rllib.agents.ddpg.noop_model import NoopModel
from ray.rllib.agents.dqn.dqn_policy import postprocess_nstep_and_prio, \
    PRIO_WEIGHTS
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.models import ModelCatalog
from ray.rllib.models.tf.tf_action_dist import Categorical, SquashedGaussian, \
    DiagGaussian
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils import try_import_tf, try_import_tfp
from ray.rllib.utils.annotations import override
from ray.rllib.utils.tf_ops import minimize_and_clip, make_tf_callable

tf = try_import_tf()
tfp = try_import_tfp()

logger = logging.getLogger(__name__)


def build_sac_model(policy, obs_space, action_space, config):
    if config["model"]["custom_model"]:
        logger.warning(
            "Setting use_state_preprocessor=True since a custom model "
            "was specified.")
        config["use_state_preprocessor"] = True
    if not isinstance(action_space, (Box, Discrete)):
        raise UnsupportedSpaceException(
            "Action space {} is not supported for SAC.".format(action_space))
    if isinstance(action_space, Box) and len(action_space.shape) > 1:
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
        model_interface=SACModel,
        default_model=default_model,
        name="sac_model",
        actor_hidden_activation=config["policy_model"]["hidden_activation"],
        actor_hiddens=config["policy_model"]["hidden_layer_sizes"],
        critic_hidden_activation=config["Q_model"]["hidden_activation"],
        critic_hiddens=config["Q_model"]["hidden_layer_sizes"],
        twin_q=config["twin_q"],
        initial_alpha=config["initial_alpha"])

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs,
        config["model"],
        framework="tf",
        model_interface=SACModel,
        default_model=default_model,
        name="target_sac_model",
        actor_hidden_activation=config["policy_model"]["hidden_activation"],
        actor_hiddens=config["policy_model"]["hidden_layer_sizes"],
        critic_hidden_activation=config["Q_model"]["hidden_activation"],
        critic_hiddens=config["Q_model"]["hidden_layer_sizes"],
        twin_q=config["twin_q"],
        initial_alpha=config["initial_alpha"])

    return policy.model


def postprocess_trajectory(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    return postprocess_nstep_and_prio(policy, sample_batch)


def get_dist_class(config, action_space):
    if isinstance(action_space, Discrete):
        action_dist_class = Categorical
    else:
        action_dist_class = SquashedGaussian if \
            config["normalize_actions"] is True else DiagGaussian
    return action_dist_class


def get_log_likelihood(policy, model, actions, input_dict, obs_space,
                       action_space, config):
    model_out, _ = model({
        "obs": input_dict[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)
    distribution_inputs = model.action_model(model_out)
    action_dist_class = get_dist_class(policy.config, action_space)
    return action_dist_class(distribution_inputs, model).logp(actions)


def build_action_output(policy, model, input_dict, obs_space, action_space,
                        explore, config, timestep):
    model_out, _ = model({
        "obs": input_dict[SampleBatch.CUR_OBS],
        "is_training": policy._get_is_training_placeholder(),
    }, [], None)
    distribution_inputs = model.action_model(model_out)
    action_dist_class = get_dist_class(policy.config, action_space)

    policy.output_actions, policy.sampled_action_logp = \
        policy.exploration.get_exploration_action(
            distribution_inputs, action_dist_class, model, explore, timestep)

    return policy.output_actions, policy.sampled_action_logp


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

    # Discrete case: Get action probs directly from pi and form their logp.
    policy_t = model.action_model(model_out_t)
    policy.raw_policy_out = policy_t

    #policy_t = tf.nn.softmax(policy_t)
    #log_pis_t = tf.log(policy_t)
    log_pis_t = tf.nn.log_softmax(policy_t, -1)
    policy_t = tf.exp(log_pis_t)

    #policy_tp1 = tf.nn.softmax(model.action_model(model_out_tp1), -1)
    log_pis_tp1 = tf.nn.log_softmax(model.action_model(model_out_tp1), -1)
    policy_tp1 = tf.exp(log_pis_tp1)
    #log_pis_tp1 = tf.log(policy_tp1)
    # Q-values for current policy in given current state.
    q_t = model.get_q_values(model_out_t)
    policy.q_t = q_t

    # Target Q-network evaluation.
    q_tp1 = policy.target_model.get_q_values(target_model_out_tp1) - \
            model.alpha * log_pis_tp1
    policy.q_tp1 = q_tp1

    assert policy.config["n_step"] == 1, "TODO(hartikainen) n_step > 1"

    #q_t_selected = tf.reduce_sum(tf.multiply(tf.stop_gradient(policy_t), q_t), axis=-1)
    one_hot = tf.one_hot(train_batch[SampleBatch.ACTIONS], depth=q_t.shape.as_list()[-1])
    q_t_selected = tf.reduce_sum(q_t * one_hot, axis=-1)
    policy.q_t_selected = q_t_selected
    #q_t_selected = q_t

    q_tp1_best = tf.reduce_sum(tf.multiply(policy_tp1, q_tp1), axis=-1)
    policy.q_tp1_best = q_tp1_best
    #q_tp1_best = q_tp1
    #q_tp1_best_masked = \
    #    tf.expand_dims(
    #        1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32),
    #        axis=-1) * q_tp1_best
    q_tp1_best_masked = \
        (1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)) * \
        q_tp1_best

    # compute RHS of bellman equation
    #q_t_selected_target = tf.stop_gradient(
    #    tf.expand_dims(train_batch[SampleBatch.REWARDS], axis=-1) +
    #    (tf.expand_dims(policy.config["gamma"], axis=-1) * q_tp1_best_masked))
    q_t_selected_target = tf.stop_gradient(
        train_batch[SampleBatch.REWARDS] +
        (policy.config["gamma"] * q_tp1_best_masked))
    policy.q_t_selected_target = q_t_selected_target

    # TODO(sven): Should TD-errors not just be the abs of the difference
    #  (w/o the square)?
    #td_error = tf.reduce_sum(tf.multiply(tf.stop_gradient(policy_t), tf.abs(q_t_selected - q_t_selected_target)), axis=-1)
    td_error = tf.abs(q_t_selected_target - q_t_selected)

    critic_loss = [
        #0.5 * tf.reduce_mean(tf.square(td_error))
        tf.losses.mean_squared_error(
            labels=q_t_selected_target, predictions=q_t_selected, weights=0.5)
    ]
    #critic_loss[0] = tf.Print(critic_loss[0], [critic_loss[0]], "CriticLoss")

    # Calculate the alpha-loss.
    if policy.config["target_entropy"] == "auto":
        if isinstance(policy.action_space, Discrete):
            target_entropy = np.array(-policy.action_space.n, dtype=np.float32)
        else:
            target_entropy = -np.prod(policy.action_space.shape)
    else:
        target_entropy = policy.config["target_entropy"]
    policy.target_entropy = target_entropy
    # Note: In the papers, alpha is used directly, here we take the log.
    # Discrete case: Multiply the action probs as weights with the original
    # loss terms (no expectations needed).
    alpha_loss = tf.reduce_mean(tf.reduce_sum(tf.multiply(
        tf.stop_gradient(policy_t),
        -model.log_alpha * tf.stop_gradient(log_pis_t + target_entropy)),
        axis=-1))
    #alpha_loss = tf.Print(alpha_loss, [alpha_loss], "AlphaLoss")
    actor_loss = tf.reduce_mean(tf.reduce_sum(tf.multiply(
        policy_t,
        model.alpha * log_pis_t - tf.stop_gradient(q_t)), axis=-1))
    #actor_loss = tf.Print(actor_loss, [actor_loss], "ActorLoss")
    # save for stats function
    policy.q_t = q_t
    policy.td_error = td_error
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.alpha_loss = alpha_loss
    policy.alpha_value = model.alpha

    #p1 = tf.Print(alpha_loss, [q_t, td_error, actor_loss, critic_loss, alpha_loss], "Q_t|TDErr|AcL|CrL|AlL")
    #print("q_t={}".format(q_t))
    #print("td_error={}".format(td_error))
    #print("actor_loss={}".format(actor_loss))
    #print("critic_loss={}".format(critic_loss))
    #print("alpha_loss={}".format(alpha_loss))

    # in a custom apply op we handle the losses separately, but return them
    # combined in one loss for now
    #with tf.control_dependencies([p1]):
    return actor_loss + tf.add_n(critic_loss) + alpha_loss


def gradients(policy, optimizer, loss):
    if policy.config["grad_norm_clipping"]:
        actor_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.actor_loss,
            var_list=policy.model.policy_variables(),
            clip_val=policy.config["grad_norm_clipping"])
        critic_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.critic_loss[0],
            var_list=policy.model.q_variables(),
            clip_val=policy.config["grad_norm_clipping"])
        alpha_grads_and_vars = minimize_and_clip(
            optimizer,
            policy.alpha_loss,
            var_list=[policy.model.log_alpha],
            clip_val=policy.config["grad_norm_clipping"])
    else:
        actor_grads_and_vars = policy._actor_optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables())
        critic_grads_and_vars = policy._critic_optimizer[
            0].compute_gradients(
                policy.critic_loss[0], var_list=policy.model.q_variables())
        #print("alpha-loss={}".format(policy.alpha_loss))
        alpha_grads_and_vars = policy._alpha_optimizer.compute_gradients(
            policy.alpha_loss, var_list=[policy.model.log_alpha])

    # save these for later use in build_apply_op
    policy._actor_grads_and_vars = [(g, v) for (g, v) in actor_grads_and_vars
                                    if g is not None]
    policy._critic_grads_and_vars = [(g, v) for (g, v) in critic_grads_and_vars
                                     if g is not None]
    policy._alpha_grads_and_vars = [(g, v) for (g, v) in alpha_grads_and_vars
                                    if g is not None]
    #print("alpha_grads_and_vars={}".format(policy._alpha_grads_and_vars))
    grads_and_vars = (
        policy._actor_grads_and_vars + policy._critic_grads_and_vars +
        policy._alpha_grads_and_vars)
    return grads_and_vars


def apply_gradients(policy, optimizer, grads_and_vars):
    actor_apply_ops = policy._actor_optimizer.apply_gradients(
        policy._actor_grads_and_vars)

    cgrads = policy._critic_grads_and_vars

    critic_apply_ops = [
        policy._critic_optimizer[0].apply_gradients(cgrads)
    ]

    alpha_apply_ops = policy._alpha_optimizer.apply_gradients(
        policy._alpha_grads_and_vars,
        global_step=tf.train.get_or_create_global_step())
    return tf.group([actor_apply_ops, alpha_apply_ops] + critic_apply_ops)


def stats(policy, train_batch):
    return {
        "td_error": tf.reduce_mean(policy.td_error),
        "actor_loss": tf.reduce_mean(policy.actor_loss),
        "critic_loss": tf.reduce_mean(policy.critic_loss),
        "alpha_loss": tf.reduce_mean(policy.alpha_loss),
        "alpha_value": tf.reduce_mean(policy.alpha_value),
        "target_entropy_value": tf.constant(policy.target_entropy),
        "policy_out": policy.raw_policy_out,
        #"q_t": policy.q_t,
        #"q_tp1": policy.q_tp1,
        "mean_q_t_selected": tf.reduce_mean(policy.q_t_selected),
        "mean_q_t_selected_target": tf.reduce_mean(policy.q_t_selected_target),
        "mean_q": tf.reduce_mean(policy.q_t),
        "mean_q_tp1": tf.reduce_mean(policy.q_tp1),
        "max_q": tf.reduce_max(policy.q_t),
        "min_q": tf.reduce_min(policy.q_t),
    }


class ActorCriticOptimizerMixin:
    def __init__(self, config):
        # create global step for counting the number of update operations
        self.global_step = tf.train.get_or_create_global_step()

        # use separate optimizers for actor & critic
        self._actor_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["optimization"]["actor_learning_rate"])
        self._critic_optimizer = [
            tf.train.AdamOptimizer(
                learning_rate=config["optimization"]["critic_learning_rate"])
        ]
        #if config["twin_q"]:
        #    self._critic_optimizer.append(
        #        tf.train.AdamOptimizer(learning_rate=config["optimization"][
        #            "critic_learning_rate"]))
        self._alpha_optimizer = tf.train.AdamOptimizer(
            learning_rate=config["optimization"]["entropy_learning_rate"])


class ComputeTDErrorMixin:
    def __init__(self):
        @make_tf_callable(self.get_session(), dynamic_shape=True)
        def compute_td_error(obs_t, act_t, rew_t, obs_tp1, done_mask,
                             importance_weights):
            # Do forward pass on loss to update td errors attribute
            # (one TD-error value per item in batch to update PR weights).
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


class TargetNetworkMixin:
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

    @override(TFPolicy)
    def variables(self):
        return self.model.variables() + self.target_model.variables()


def setup_early_mixins(policy, obs_space, action_space, config):
    ActorCriticOptimizerMixin.__init__(policy, config)


def setup_mid_mixins(policy, obs_space, action_space, config):
    ComputeTDErrorMixin.__init__(policy)


def setup_late_mixins(policy, obs_space, action_space, config):
    TargetNetworkMixin.__init__(policy, config)


SACTFPolicy = build_tf_policy(
    name="SACTFPolicy",
    get_default_config=lambda: ray.rllib.agents.sac.sac.DEFAULT_CONFIG,
    make_model=build_sac_model,
    postprocess_fn=postprocess_trajectory,
    action_sampler_fn=build_action_output,
    log_likelihood_fn=get_log_likelihood,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    gradients_fn=gradients,
    apply_gradients_fn=apply_gradients,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    mixins=[
        TargetNetworkMixin, ActorCriticOptimizerMixin, ComputeTDErrorMixin
    ],
    before_init=setup_early_mixins,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
    obs_include_prev_action_reward=False)
