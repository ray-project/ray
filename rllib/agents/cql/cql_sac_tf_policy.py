"""
TF policy class used for CQL.
"""
from functools import partial

import numpy as np
import gym
import logging
from typing import Dict, Union, Type, List

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.cql.cql_sac_tf_model import CQLSACTFModel
from ray.rllib.agents.sac.sac_tf_policy import ActorCriticOptimizerMixin, \
    ComputeTDErrorMixin, TargetNetworkMixin, stats, \
    compute_and_clip_gradients, apply_gradients, SACTFPolicy, sac_actor_critic_loss
from ray.rllib.models import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.typing import TensorType, TrainerConfigDict, LocalOptimizer, \
    ModelGradients

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()

logger = logging.getLogger(__name__)


def build_cql_sac_model(policy: Policy, obs_space: gym.spaces.Space,
                    action_space: gym.spaces.Space,
                    config: TrainerConfigDict) -> ModelV2:
    """Constructs the necessary ModelV2 for the Policy and returns it.

    Args:
        policy (Policy): The TFPolicy that will use the models.
        obs_space (gym.spaces.Space): The observation space.
        action_space (gym.spaces.Space): The action space.
        config (TrainerConfigDict): The CQL trainer's config dict.

    Returns:
        ModelV2: The ModelV2 to be used by the Policy. Note: An additional
            target model will be created in this function and assigned to
            `policy.target_model`.
    """
    # With separate state-preprocessor (before obs+action concat).
    num_outputs = int(np.product(obs_space.shape))

    # Force-ignore any additionally provided hidden layer sizes.
    # Everything should be configured using CQL_SAC's "Q_model" and "policy_model"
    # settings.
    policy_model_config = MODEL_DEFAULTS.copy()
    policy_model_config.update(config["policy_model"])
    q_model_config = MODEL_DEFAULTS.copy()
    q_model_config.update(config["Q_model"])

    assert config["framework"] != "torch"
    default_model_cls = CQLSACTFModel

    model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework=config["framework"],
        default_model=default_model_cls,
        name="cql_sac_model",
        policy_model_config=policy_model_config,
        q_model_config=q_model_config,
        twin_q=config["twin_q"],
        initial_alpha=config["initial_alpha"],
        target_entropy=config["target_entropy"],
        lagrangian=config["lagrangian"],
        initial_alpha_prime=config["initial_alpha_prime"])

    assert isinstance(model, default_model_cls)

    # Create an exact copy of the model and store it in `policy.target_model`.
    # This will be used for tau-synched Q-target models that run behind the
    # actual Q-networks and are used for target q-value calculations in the
    # loss terms.
    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework=config["framework"],
        default_model=default_model_cls,
        name="target_cql_sac_model",
        policy_model_config=policy_model_config,
        q_model_config=q_model_config,
        twin_q=config["twin_q"],
        initial_alpha=config["initial_alpha"],
        target_entropy=config["target_entropy"],
        lagrangian=config["lagrangian"],
        initial_alpha_prime=config["initial_alpha_prime"])

    assert isinstance(policy.target_model, default_model_cls)

    return model


# Returns policy tiled actions and log probabilities for CQL Loss
def policy_actions_repeat(model, action_dist, obs, num_repeat=1):
    obs_temp = tf.tile(obs, [num_repeat, 1])
    policy_dist = action_dist(model.get_policy_output(obs_temp), model)
    actions = policy_dist.sample()
    log_p = tf.expand_dims(policy_dist.logp(actions), -1)
    return actions, tf.squeeze(log_p, axis=len(log_p.shape) - 1)


def q_values_repeat(model, obs, actions, twin=False):
    action_shape = tf.shape(actions)[0]
    obs_shape = tf.shape(obs)[0]
    num_repeat = action_shape // obs_shape
    obs_temp = tf.tile(obs, [num_repeat, 1])
    if twin:
        preds = model.get_q_values(obs_temp, actions)
    else:
        preds = model.get_twin_q_values(obs_temp, actions)
    preds = tf.reshape(preds, [obs_shape, num_repeat, 1])
    return preds


def cql_loss(policy: Policy, model: ModelV2, dist_class: Type[TFActionDistribution],
             train_batch: SampleBatch) -> Union[TensorType, List[TensorType]]:
    """Constructs the loss for the Soft Actor Critic.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]: The action distr. class.
        train_batch (SampleBatch): The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    # For best performance, turn deterministic off
    deterministic = policy.config["_deterministic_loss"]
    twin_q = policy.config["twin_q"]
    discount = policy.config["gamma"]

    # CQL Parameters
    bc_iters = policy.config["bc_iters"]
    cql_temp = policy.config["temperature"]
    num_actions = policy.config["num_actions"]
    min_q_weight = policy.config["min_q_weight"]
    use_lagrange = policy.config["lagrangian"]
    target_action_gap = policy.config["lagrangian_thresh"]

    obs = train_batch[SampleBatch.CUR_OBS]
    actions = train_batch[SampleBatch.ACTIONS]
    rewards = train_batch[SampleBatch.REWARDS]
    next_obs = train_batch[SampleBatch.NEXT_OBS]
    terminals = train_batch[SampleBatch.DONES]

    # Execute SAC Policy as it is
    sac_loss_res = sac_actor_critic_loss(policy, model, dist_class, train_batch)

    # CQL Loss (We are using Entropy version of CQL (the best version))
    rand_actions = policy._unif_dist.sample([tf.shape(actions)[0] * num_actions,
                                             actions.shape[-1]])
    curr_actions, curr_logp = policy_actions_repeat(model, policy.action_dist_class,
                                                    obs, num_actions)
    next_actions, next_logp = policy_actions_repeat(model, policy.action_dist_class,
                                                    next_obs, num_actions)
    curr_logp = tf.reshape(curr_logp, [tf.shape(actions)[0], num_actions, 1])
    next_logp = tf.reshape(next_logp, [tf.shape(actions)[0], num_actions, 1])

    q1_rand = q_values_repeat(model, policy.model_out_t, rand_actions)
    q1_curr_actions = q_values_repeat(model, policy.model_out_t, curr_actions)
    q1_next_actions = q_values_repeat(model, policy.model_out_t, next_actions)

    if twin_q:
        q2_rand = q_values_repeat(model, policy.model_out_t, rand_actions, twin=True)
        q2_curr_actions = q_values_repeat(
            model, policy.model_out_t, curr_actions, twin=True)
        q2_next_actions = q_values_repeat(
            model, policy.model_out_t, next_actions, twin=True)

    random_density = np.log(0.5**curr_actions.shape[-1].value)
    cat_q1 = tf.concat([
        q1_rand - random_density, q1_next_actions - tf.stop_gradient(next_logp),
        q1_curr_actions - tf.stop_gradient(curr_logp)
    ], 1)
    if twin_q:
        cat_q2 = tf.concat([
            q2_rand - random_density, q2_next_actions - tf.stop_gradient(next_logp),
            q2_curr_actions - tf.stop_gradient(curr_logp)
        ], 1)

    min_qf1_loss = tf.reduce_mean(tf.reduce_logsumexp(
        cat_q1 / cql_temp, axis=1)) * min_q_weight * cql_temp
    min_qf1_loss = min_qf1_loss - tf.reduce_mean(policy.q_t_selected) * min_q_weight
    if twin_q:
        min_qf2_loss = tf.reduce_mean(tf.reduce_logsumexp(
            cat_q2 / cql_temp, axis=1)) * min_q_weight * cql_temp
        min_qf2_loss = min_qf2_loss - tf.reduce_mean(policy.twin_q_t_selected) * min_q_weight

    if use_lagrange:
        alpha_upper_bound = policy.config["alpha_upper_bound"]
        alpha_lower_bound = policy.config["alpha_lower_bound"]
        alpha_prime = tf.clip_by_value(
            tf.exp(model.log_alpha_prime), clip_value_min=alpha_lower_bound, clip_value_max=alpha_upper_bound)
        min_qf1_loss = alpha_prime * (min_qf1_loss - target_action_gap)
        if twin_q:
            min_qf2_loss = alpha_prime * (min_qf2_loss - target_action_gap)
            alpha_prime_loss = 0.5 * (-min_qf1_loss - min_qf2_loss)
        else:
            alpha_prime_loss = -min_qf1_loss

    cql_loss = [min_qf1_loss]
    if twin_q:
        cql_loss.append(min_qf2_loss)

    policy.critic_loss[0] += min_qf1_loss
    if twin_q:
        policy.critic_loss[1] += min_qf2_loss

    # Save for stats function.
    # CQL Stats
    policy.cql_loss = cql_loss
    if use_lagrange:
        policy.log_alpha_prime_value = model.log_alpha_prime
        policy.alpha_prime_value = model.alpha_prime
        policy.alpha_prime_loss = alpha_prime_loss
        # In a custom apply op we handle the losses separately, but return them
        # combined in one loss here.
        return sac_loss_res + alpha_prime_loss
    else:
        return sac_loss_res


def cql_compute_and_clip_gradients(policy: Policy, optimizer: LocalOptimizer,
                               loss: TensorType) -> ModelGradients:
    """Gradients computing function (from loss tensor, using local optimizer).

    Note: For CQL, optimizer and loss are ignored b/c we have 1 extra
    loss and 1 local optimizer (all stored in policy).
    `optimizer` will be used, though, in the tf-eager case b/c it is then a
    fake optimizer (OptimizerWrapper) object with a `tape` property to
    generate a GradientTape object for gradient recording.

    Args:
        policy (Policy): The Policy object that generated the loss tensor and
            that holds the given local optimizer.
        optimizer (LocalOptimizer): The tf (local) optimizer object to
            calculate the gradients with.
        loss (TensorType): The loss tensor for which gradients should be
            calculated.

    Returns:
        ModelGradients: List of the possibly clipped gradients- and variable
            tuples.
    """
    # Eager: Use GradientTape (which is a property of the `optimizer` object
    # (an OptimizerWrapper): see rllib/policy/eager_tf_policy.py).
    grads_and_vars = compute_and_clip_gradients(policy, optimizer, loss)
    if policy.config["lagrangian"]:
        if policy.config["framework"] in ["tf2", "tfe"]:
            tape = optimizer.tape
            alpha_prime_vars = [policy.model.log_alpha_prime]
            alpha_prime_grads_and_vars = list(
                zip(tape.gradient(policy.alpha_prime_loss, alpha_prime_vars), alpha_prime_vars))
        # Tf1.x: Use optimizer.compute_gradients()
        else:
            alpha_prime_grads_and_vars = policy._alpha_prime_optimizer.compute_gradients(
            policy.alpha_prime_loss, var_list=[policy.model.log_alpha_prime])

        # Clip if necessary.
        if policy.config["grad_clip"]:
            clip_func = partial(
                tf.clip_by_norm, clip_norm=policy.config["grad_clip"])
        else:
            clip_func = tf.identity

        # Save grads and vars for later use in `build_apply_op`.
        policy._alpha_prime_grads_and_vars = [(clip_func(g), v)
                                              for (g, v) in alpha_prime_grads_and_vars
                                              if g is not None]

        grads_and_vars = tuple(list(grads_and_vars) + policy._alpha_prime_grads_and_vars)

    return grads_and_vars


def cql_apply_gradients(
        policy: Policy, optimizer: LocalOptimizer,
        grads_and_vars: ModelGradients) -> Union["tf.Operation", None]:
    """Gradients applying function (from list of "grad_and_var" tuples).

    Args:
        policy (Policy): The Policy object whose Model(s) the given gradients
            should be applied to.
        optimizer (LocalOptimizer): The tf (local) optimizer object through
            which to apply the gradients.
        grads_and_vars (ModelGradients): The list of grad_and_var tuples to
            apply via the given optimizer.

    Returns:
        Union[tf.Operation, None]: The tf op to be used to run the apply
            operation. None for eager mode.
    """
    grads_group_ops = apply_gradients(policy, optimizer, grads_and_vars)
    if policy.config["lagrangian"]:
        # Eager mode -> Just apply and return None.
        if policy.config["framework"] in ["tf2", "tfe"]:
            policy._alpha_prime_optimizer.apply_gradients(
                policy._alpha_prime_grads_and_vars)
        # Tf static graph -> Return op.
        else:
            alpha_prime_apply_ops = policy._alpha_prime_optimizer.apply_gradients(
                policy._alpha_prime_grads_and_vars)
            grads_group_ops = tf.group([grads_group_ops, alpha_prime_apply_ops])

    return grads_group_ops


def cql_stats(policy: Policy,
              train_batch: SampleBatch) -> Dict[str, TensorType]:
    cql_dict = stats(policy, train_batch)
    cql_dict["cql_loss"] = tf.reduce_mean(tf.stack(policy.cql_loss))
    if policy.config["lagrangian"]:
        cql_dict["log_alpha_prime_value"] = policy.log_alpha_prime_value
        cql_dict["alpha_prime_value"] = policy.alpha_prime_value
        cql_dict["alpha_prime_loss"] = policy.alpha_prime_loss
    return cql_dict


class CQLActorCriticOptimizerMixin(ActorCriticOptimizerMixin):
    def __init__(self, config):
        super().__init__(config)
        if config["framework"] in ["tf2", "tfe"]:
            if config["lagrangian"]:
                self._alpha_prime_optimizer = tf.keras.optimizers.Adam(
                    learning_rate=config["optimization"]["critic_learning_rate"])
        else:
            if config["lagrangian"]:
                self._alpha_prime_optimizer = tf1.train.AdamOptimizer(
                    learning_rate=config["optimization"]["critic_learning_rate"])


def cql_setup_early_mixins(policy: Policy, obs_space: gym.spaces.Space,
                           action_space: gym.spaces.Space,
                           config: TrainerConfigDict):
    """Call mixin classes' constructors before Policy's initialization.

    Adds the necessary optimizers to the given Policy.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    CQLActorCriticOptimizerMixin.__init__(policy, config)


def cql_setup_mid_mixins(policy: Policy, obs_space: gym.spaces.Space,
                     action_space: gym.spaces.Space,
                     config: TrainerConfigDict) -> None:
    action_low = policy.model.action_space.low[0]
    action_high = policy.model.action_space.high[0]
    policy._unif_dist = tfp.distributions.Uniform(action_low, action_high,
                                                  name = "uniform_rand_actions")
    ComputeTDErrorMixin.__init__(policy, cql_loss)


# Build a child class of `TFPolicy`, given the custom functions defined
# above.
CQLSACTFPolicy = SACTFPolicy.with_updates(
    name="CQLSACTFPolicy",
    get_default_config=lambda: ray.rllib.agents.cql.CQLSAC_DEFAULT_CONFIG,
    make_model=build_cql_sac_model,
    loss_fn=cql_loss,
    stats_fn=cql_stats,
    gradients_fn=cql_compute_and_clip_gradients,
    apply_gradients_fn=cql_apply_gradients,
    mixins=[
        TargetNetworkMixin, CQLActorCriticOptimizerMixin, ComputeTDErrorMixin
    ],
    before_init=cql_setup_early_mixins,
    before_loss_init=cql_setup_mid_mixins,
)
