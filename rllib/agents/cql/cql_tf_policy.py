"""
TensorFlow policy class used for CQL.
"""
from functools import partial
import numpy as np
import gym
import logging
import tree
from typing import Dict, List, Type, Union

import ray
import ray.experimental.tf_utils
from ray.rllib.agents.sac.sac_tf_policy import (
    apply_gradients as sac_apply_gradients,
    compute_and_clip_gradients as sac_compute_and_clip_gradients,
    get_distribution_inputs_and_class,
    _get_dist_class,
    build_sac_model,
    postprocess_trajectory,
    setup_late_mixins,
    stats,
    validate_spaces,
    ActorCriticOptimizerMixin as SACActorCriticOptimizerMixin,
    ComputeTDErrorMixin,
    TargetNetworkMixin,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.exploration.random import Random
from ray.rllib.utils.framework import get_variable, try_import_tf, try_import_tfp
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorType,
    TrainerConfigDict,
)

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()

logger = logging.getLogger(__name__)

MEAN_MIN = -9.0
MEAN_MAX = 9.0


def _repeat_tensor(t: TensorType, n: int):
    # Insert new axis at position 1 into tensor t
    t_rep = tf.expand_dims(t, 1)
    # Repeat tensor t_rep along new axis n times
    multiples = tf.concat([[1, n], tf.tile([1], tf.expand_dims(tf.rank(t) - 1, 0))], 0)
    t_rep = tf.tile(t_rep, multiples)
    # Merge new axis into batch axis
    t_rep = tf.reshape(t_rep, tf.concat([[-1], tf.shape(t)[1:]], 0))
    return t_rep


# Returns policy tiled actions and log probabilities for CQL Loss
def policy_actions_repeat(model, action_dist, obs, num_repeat=1):
    batch_size = tf.shape(tree.flatten(obs)[0])[0]
    obs_temp = tree.map_structure(lambda t: _repeat_tensor(t, num_repeat), obs)
    logits, _ = model.get_action_model_outputs(obs_temp)
    policy_dist = action_dist(logits, model)
    actions, logp_ = policy_dist.sample_logp()
    logp = tf.expand_dims(logp_, -1)
    return actions, tf.reshape(logp, [batch_size, num_repeat, 1])


def q_values_repeat(model, obs, actions, twin=False):
    action_shape = tf.shape(actions)[0]
    obs_shape = tf.shape(tree.flatten(obs)[0])[0]
    num_repeat = action_shape // obs_shape
    obs_temp = tree.map_structure(lambda t: _repeat_tensor(t, num_repeat), obs)
    if not twin:
        preds_, _ = model.get_q_values(obs_temp, actions)
    else:
        preds_, _ = model.get_twin_q_values(obs_temp, actions)
    preds = tf.reshape(preds_, [obs_shape, num_repeat, 1])
    return preds


def cql_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TFActionDistribution],
    train_batch: SampleBatch,
) -> Union[TensorType, List[TensorType]]:
    logger.info(f"Current iteration = {policy.cur_iter}")
    policy.cur_iter += 1

    # For best performance, turn deterministic off
    deterministic = policy.config["_deterministic_loss"]
    assert not deterministic
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
    actions = tf.cast(train_batch[SampleBatch.ACTIONS], tf.float32)
    rewards = tf.cast(train_batch[SampleBatch.REWARDS], tf.float32)
    next_obs = train_batch[SampleBatch.NEXT_OBS]
    terminals = train_batch[SampleBatch.DONES]

    model_out_t, _ = model(SampleBatch(obs=obs, _is_training=True), [], None)

    model_out_tp1, _ = model(SampleBatch(obs=next_obs, _is_training=True), [], None)

    target_model_out_tp1, _ = policy.target_model(
        SampleBatch(obs=next_obs, _is_training=True), [], None
    )

    action_dist_class = _get_dist_class(policy, policy.config, policy.action_space)
    action_dist_inputs_t, _ = model.get_action_model_outputs(model_out_t)
    action_dist_t = action_dist_class(action_dist_inputs_t, model)
    policy_t, log_pis_t = action_dist_t.sample_logp()
    log_pis_t = tf.expand_dims(log_pis_t, -1)

    # Unlike original SAC, Alpha and Actor Loss are computed first.
    # Alpha Loss
    alpha_loss = -tf.reduce_mean(
        model.log_alpha * tf.stop_gradient(log_pis_t + model.target_entropy)
    )

    # Policy Loss (Either Behavior Clone Loss or SAC Loss)
    alpha = tf.math.exp(model.log_alpha)
    if policy.cur_iter >= bc_iters:
        min_q, _ = model.get_q_values(model_out_t, policy_t)
        if twin_q:
            twin_q_, _ = model.get_twin_q_values(model_out_t, policy_t)
            min_q = tf.math.minimum(min_q, twin_q_)
        actor_loss = tf.reduce_mean(tf.stop_gradient(alpha) * log_pis_t - min_q)
    else:
        bc_logp = action_dist_t.logp(actions)
        actor_loss = tf.reduce_mean(tf.stop_gradient(alpha) * log_pis_t - bc_logp)
        # actor_loss = -tf.reduce_mean(bc_logp)

    # Critic Loss (Standard SAC Critic L2 Loss + CQL Entropy Loss)
    # SAC Loss:
    # Q-values for the batched actions.
    action_dist_inputs_tp1, _ = model.get_action_model_outputs(model_out_tp1)
    action_dist_tp1 = action_dist_class(action_dist_inputs_tp1, model)
    policy_tp1, _ = action_dist_tp1.sample_logp()

    q_t, _ = model.get_q_values(model_out_t, actions)
    q_t_selected = tf.squeeze(q_t, axis=-1)
    if twin_q:
        twin_q_t, _ = model.get_twin_q_values(model_out_t, actions)
        twin_q_t_selected = tf.squeeze(twin_q_t, axis=-1)

    # Target q network evaluation.
    q_tp1, _ = policy.target_model.get_q_values(target_model_out_tp1, policy_tp1)
    if twin_q:
        twin_q_tp1, _ = policy.target_model.get_twin_q_values(
            target_model_out_tp1, policy_tp1
        )
        # Take min over both twin-NNs.
        q_tp1 = tf.math.minimum(q_tp1, twin_q_tp1)

    q_tp1_best = tf.squeeze(input=q_tp1, axis=-1)
    q_tp1_best_masked = (1.0 - tf.cast(terminals, tf.float32)) * q_tp1_best

    # compute RHS of bellman equation
    q_t_target = tf.stop_gradient(
        rewards + (discount ** policy.config["n_step"]) * q_tp1_best_masked
    )

    # Compute the TD-error (potentially clipped), for priority replay buffer
    base_td_error = tf.math.abs(q_t_selected - q_t_target)
    if twin_q:
        twin_td_error = tf.math.abs(twin_q_t_selected - q_t_target)
        td_error = 0.5 * (base_td_error + twin_td_error)
    else:
        td_error = base_td_error

    critic_loss_1 = tf.keras.losses.MSE(q_t_selected, q_t_target)
    if twin_q:
        critic_loss_2 = tf.keras.losses.MSE(twin_q_t_selected, q_t_target)

    # CQL Loss (We are using Entropy version of CQL (the best version))
    rand_actions, _ = policy._random_action_generator.get_exploration_action(
        action_distribution=action_dist_class(
            tf.tile(action_dist_tp1.inputs, (num_actions, 1)), model
        ),
        timestep=0,
        explore=True,
    )
    curr_actions, curr_logp = policy_actions_repeat(
        model, action_dist_class, model_out_t, num_actions
    )
    next_actions, next_logp = policy_actions_repeat(
        model, action_dist_class, model_out_tp1, num_actions
    )

    q1_rand = q_values_repeat(model, model_out_t, rand_actions)
    q1_curr_actions = q_values_repeat(model, model_out_t, curr_actions)
    q1_next_actions = q_values_repeat(model, model_out_t, next_actions)

    if twin_q:
        q2_rand = q_values_repeat(model, model_out_t, rand_actions, twin=True)
        q2_curr_actions = q_values_repeat(model, model_out_t, curr_actions, twin=True)
        q2_next_actions = q_values_repeat(model, model_out_t, next_actions, twin=True)

    random_density = np.log(0.5 ** int(curr_actions.shape[-1]))
    cat_q1 = tf.concat(
        [
            q1_rand - random_density,
            q1_next_actions - tf.stop_gradient(next_logp),
            q1_curr_actions - tf.stop_gradient(curr_logp),
        ],
        1,
    )
    if twin_q:
        cat_q2 = tf.concat(
            [
                q2_rand - random_density,
                q2_next_actions - tf.stop_gradient(next_logp),
                q2_curr_actions - tf.stop_gradient(curr_logp),
            ],
            1,
        )

    min_qf1_loss_ = (
        tf.reduce_mean(tf.reduce_logsumexp(cat_q1 / cql_temp, axis=1))
        * min_q_weight
        * cql_temp
    )
    min_qf1_loss = min_qf1_loss_ - (tf.reduce_mean(q_t) * min_q_weight)
    if twin_q:
        min_qf2_loss_ = (
            tf.reduce_mean(tf.reduce_logsumexp(cat_q2 / cql_temp, axis=1))
            * min_q_weight
            * cql_temp
        )
        min_qf2_loss = min_qf2_loss_ - (tf.reduce_mean(twin_q_t) * min_q_weight)

    if use_lagrange:
        alpha_prime = tf.clip_by_value(model.log_alpha_prime.exp(), 0.0, 1000000.0)[0]
        min_qf1_loss = alpha_prime * (min_qf1_loss - target_action_gap)
        if twin_q:
            min_qf2_loss = alpha_prime * (min_qf2_loss - target_action_gap)
            alpha_prime_loss = 0.5 * (-min_qf1_loss - min_qf2_loss)
        else:
            alpha_prime_loss = -min_qf1_loss

    cql_loss = [min_qf1_loss]
    if twin_q:
        cql_loss.append(min_qf2_loss)

    critic_loss = [critic_loss_1 + min_qf1_loss]
    if twin_q:
        critic_loss.append(critic_loss_2 + min_qf2_loss)

    # Save for stats function.
    policy.q_t = q_t_selected
    policy.policy_t = policy_t
    policy.log_pis_t = log_pis_t
    policy.td_error = td_error
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.alpha_loss = alpha_loss
    policy.log_alpha_value = model.log_alpha
    policy.alpha_value = alpha
    policy.target_entropy = model.target_entropy
    # CQL Stats
    policy.cql_loss = cql_loss
    if use_lagrange:
        policy.log_alpha_prime_value = model.log_alpha_prime[0]
        policy.alpha_prime_value = alpha_prime
        policy.alpha_prime_loss = alpha_prime_loss

    # Return all loss terms corresponding to our optimizers.
    if use_lagrange:
        return actor_loss + tf.math.add_n(critic_loss) + alpha_loss + alpha_prime_loss
    return actor_loss + tf.math.add_n(critic_loss) + alpha_loss


def cql_stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    sac_dict = stats(policy, train_batch)
    sac_dict["cql_loss"] = tf.reduce_mean(tf.stack(policy.cql_loss))
    if policy.config["lagrangian"]:
        sac_dict["log_alpha_prime_value"] = policy.log_alpha_prime_value
        sac_dict["alpha_prime_value"] = policy.alpha_prime_value
        sac_dict["alpha_prime_loss"] = policy.alpha_prime_loss
    return sac_dict


class ActorCriticOptimizerMixin(SACActorCriticOptimizerMixin):
    def __init__(self, config):
        super().__init__(config)
        if config["lagrangian"]:
            # Eager mode.
            if config["framework"] in ["tf2", "tfe"]:
                self._alpha_prime_optimizer = tf.keras.optimizers.Adam(
                    learning_rate=config["optimization"]["critic_learning_rate"]
                )
            # Static graph mode.
            else:
                self._alpha_prime_optimizer = tf1.train.AdamOptimizer(
                    learning_rate=config["optimization"]["critic_learning_rate"]
                )


def setup_early_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Call mixin classes' constructors before Policy's initialization.

    Adds the necessary optimizers to the given Policy.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    policy.cur_iter = 0
    ActorCriticOptimizerMixin.__init__(policy, config)
    if config["lagrangian"]:
        policy.model.log_alpha_prime = get_variable(
            0.0, framework="tf", trainable=True, tf_name="log_alpha_prime"
        )
        policy.alpha_prime_optim = tf.keras.optimizers.Adam(
            learning_rate=config["optimization"]["critic_learning_rate"],
        )
    # Generic random action generator for calculating CQL-loss.
    policy._random_action_generator = Random(
        action_space,
        model=None,
        framework="tf2",
        policy_config=config,
        num_workers=0,
        worker_index=0,
    )


def compute_gradients_fn(
    policy: Policy, optimizer: LocalOptimizer, loss: TensorType
) -> ModelGradients:
    grads_and_vars = sac_compute_and_clip_gradients(policy, optimizer, loss)

    if policy.config["lagrangian"]:
        # Eager: Use GradientTape (which is a property of the `optimizer`
        # object (an OptimizerWrapper): see rllib/policy/eager_tf_policy.py).
        if policy.config["framework"] in ["tf2", "tfe"]:
            tape = optimizer.tape
            log_alpha_prime = [policy.model.log_alpha_prime]
            alpha_prime_grads_and_vars = list(
                zip(
                    tape.gradient(policy.alpha_prime_loss, log_alpha_prime),
                    log_alpha_prime,
                )
            )
        # Tf1.x: Use optimizer.compute_gradients()
        else:
            alpha_prime_grads_and_vars = (
                policy._alpha_prime_optimizer.compute_gradients(
                    policy.alpha_prime_loss, var_list=[policy.model.log_alpha_prime]
                )
            )

        # Clip if necessary.
        if policy.config["grad_clip"]:
            clip_func = partial(tf.clip_by_norm, clip_norm=policy.config["grad_clip"])
        else:
            clip_func = tf.identity

        # Save grads and vars for later use in `build_apply_op`.
        policy._alpha_prime_grads_and_vars = [
            (clip_func(g), v) for (g, v) in alpha_prime_grads_and_vars if g is not None
        ]

        grads_and_vars += policy._alpha_prime_grads_and_vars
    return grads_and_vars


def apply_gradients_fn(policy, optimizer, grads_and_vars):
    sac_results = sac_apply_gradients(policy, optimizer, grads_and_vars)

    if policy.config["lagrangian"]:
        # Eager mode -> Just apply and return None.
        if policy.config["framework"] in ["tf2", "tfe"]:
            policy._alpha_prime_optimizer.apply_gradients(
                policy._alpha_prime_grads_and_vars
            )
            return
        # Tf static graph -> Return grouped op.
        else:
            alpha_prime_apply_op = policy._alpha_prime_optimizer.apply_gradients(
                policy._alpha_prime_grads_and_vars,
                global_step=tf1.train.get_or_create_global_step(),
            )
            return tf.group([sac_results, alpha_prime_apply_op])
    return sac_results


# Build a child class of `TFPolicy`, given the custom functions defined
# above.
CQLTFPolicy = build_tf_policy(
    name="CQLTFPolicy",
    loss_fn=cql_loss,
    get_default_config=lambda: ray.rllib.agents.cql.cql.CQL_DEFAULT_CONFIG,
    validate_spaces=validate_spaces,
    stats_fn=cql_stats,
    postprocess_fn=postprocess_trajectory,
    before_init=setup_early_mixins,
    after_init=setup_late_mixins,
    make_model=build_sac_model,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    mixins=[ActorCriticOptimizerMixin, TargetNetworkMixin, ComputeTDErrorMixin],
    action_distribution_fn=get_distribution_inputs_and_class,
    compute_gradients_fn=compute_gradients_fn,
    apply_gradients_fn=apply_gradients_fn,
)
