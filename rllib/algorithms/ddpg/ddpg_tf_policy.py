from gym.spaces import Box
from functools import partial
import logging
import numpy as np
import gym
from typing import Dict, Tuple, List

import ray
import ray.experimental.tf_utils
from ray.rllib.algorithms.ddpg.ddpg_tf_model import DDPGTFModel
from ray.rllib.algorithms.ddpg.ddpg_torch_model import DDPGTorchModel
from ray.rllib.algorithms.ddpg.noop_model import NoopModel, TorchNoopModel
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    postprocess_nstep_and_prio,
    PRIO_WEIGHTS,
)
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Deterministic, Dirichlet
from ray.rllib.models.torch.torch_action_dist import TorchDeterministic, TorchDirichlet
from ray.rllib.utils.annotations import override
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import get_variable, try_import_tf
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.tf_utils import huber_loss, make_tf_callable
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    TensorType,
    LocalOptimizer,
    ModelGradients,
)
from ray.util.debug import log_once

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


def build_ddpg_models(
    policy: Policy,
    observation_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> ModelV2:
    if policy.config["use_state_preprocessor"]:
        default_model = None  # catalog decides
        num_outputs = 256  # arbitrary
        config["model"]["no_final_linear"] = True
    else:
        default_model = TorchNoopModel if config["framework"] == "torch" else NoopModel
        num_outputs = int(np.product(observation_space.shape))

    policy.model = ModelCatalog.get_model_v2(
        obs_space=observation_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework=config["framework"],
        model_interface=(
            DDPGTorchModel if config["framework"] == "torch" else DDPGTFModel
        ),
        default_model=default_model,
        name="ddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        twin_q=config["twin_q"],
        add_layer_norm=(
            policy.config["exploration_config"].get("type") == "ParameterNoise"
        ),
    )

    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=observation_space,
        action_space=action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework=config["framework"],
        model_interface=(
            DDPGTorchModel if config["framework"] == "torch" else DDPGTFModel
        ),
        default_model=default_model,
        name="target_ddpg_model",
        actor_hidden_activation=config["actor_hidden_activation"],
        actor_hiddens=config["actor_hiddens"],
        critic_hidden_activation=config["critic_hidden_activation"],
        critic_hiddens=config["critic_hiddens"],
        twin_q=config["twin_q"],
        add_layer_norm=(
            policy.config["exploration_config"].get("type") == "ParameterNoise"
        ),
    )

    return policy.model


def get_distribution_inputs_and_class(
    policy: Policy,
    model: ModelV2,
    obs_batch: SampleBatch,
    *,
    explore: bool = True,
    is_training: bool = False,
    **kwargs
) -> Tuple[TensorType, ActionDistribution, List[TensorType]]:
    model_out, _ = model(SampleBatch(obs=obs_batch, _is_training=is_training), [], None)
    dist_inputs = model.get_policy_output(model_out)

    if isinstance(policy.action_space, Simplex):
        distr_class = (
            TorchDirichlet if policy.config["framework"] == "torch" else Dirichlet
        )
    else:
        distr_class = (
            TorchDeterministic
            if policy.config["framework"] == "torch"
            else Deterministic
        )
    return dist_inputs, distr_class, []  # []=state out


def ddpg_actor_critic_loss(
    policy: Policy, model: ModelV2, _, train_batch: SampleBatch
) -> TensorType:
    twin_q = policy.config["twin_q"]
    gamma = policy.config["gamma"]
    n_step = policy.config["n_step"]
    use_huber = policy.config["use_huber"]
    huber_threshold = policy.config["huber_threshold"]
    l2_reg = policy.config["l2_reg"]

    input_dict = SampleBatch(obs=train_batch[SampleBatch.CUR_OBS], _is_training=True)
    input_dict_next = SampleBatch(
        obs=train_batch[SampleBatch.NEXT_OBS], _is_training=True
    )

    model_out_t, _ = model(input_dict, [], None)
    model_out_tp1, _ = model(input_dict_next, [], None)
    target_model_out_tp1, _ = policy.target_model(input_dict_next, [], None)

    policy.target_q_func_vars = policy.target_model.variables()

    # Policy network evaluation.
    policy_t = model.get_policy_output(model_out_t)
    policy_tp1 = policy.target_model.get_policy_output(target_model_out_tp1)

    # Action outputs.
    if policy.config["smooth_target_policy"]:
        target_noise_clip = policy.config["target_noise_clip"]
        clipped_normal_sample = tf.clip_by_value(
            tf.random.normal(
                tf.shape(policy_tp1), stddev=policy.config["target_noise"]
            ),
            -target_noise_clip,
            target_noise_clip,
        )
        policy_tp1_smoothed = tf.clip_by_value(
            policy_tp1 + clipped_normal_sample,
            policy.action_space.low * tf.ones_like(policy_tp1),
            policy.action_space.high * tf.ones_like(policy_tp1),
        )
    else:
        # No smoothing, just use deterministic actions.
        policy_tp1_smoothed = policy_tp1

    # Q-net(s) evaluation.
    # prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
    # Q-values for given actions & observations in given current
    q_t = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS])

    # Q-values for current policy (no noise) in given current state
    q_t_det_policy = model.get_q_values(model_out_t, policy_t)

    if twin_q:
        twin_q_t = model.get_twin_q_values(
            model_out_t, train_batch[SampleBatch.ACTIONS]
        )

    # Target q-net(s) evaluation.
    q_tp1 = policy.target_model.get_q_values(target_model_out_tp1, policy_tp1_smoothed)

    if twin_q:
        twin_q_tp1 = policy.target_model.get_twin_q_values(
            target_model_out_tp1, policy_tp1_smoothed
        )

    q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
    if twin_q:
        twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

    q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    q_tp1_best_masked = (
        1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)
    ) * q_tp1_best

    # Compute RHS of bellman equation.
    q_t_selected_target = tf.stop_gradient(
        tf.cast(train_batch[SampleBatch.REWARDS], tf.float32)
        + gamma ** n_step * q_tp1_best_masked
    )

    # Compute the error (potentially clipped).
    if twin_q:
        td_error = q_t_selected - q_t_selected_target
        twin_td_error = twin_q_t_selected - q_t_selected_target
        if use_huber:
            errors = huber_loss(td_error, huber_threshold) + huber_loss(
                twin_td_error, huber_threshold
            )
        else:
            errors = 0.5 * tf.math.square(td_error) + 0.5 * tf.math.square(
                twin_td_error
            )
    else:
        td_error = q_t_selected - q_t_selected_target
        if use_huber:
            errors = huber_loss(td_error, huber_threshold)
        else:
            errors = 0.5 * tf.math.square(td_error)

    critic_loss = tf.reduce_mean(
        tf.cast(train_batch[PRIO_WEIGHTS], tf.float32) * errors
    )
    actor_loss = -tf.reduce_mean(q_t_det_policy)

    # Add l2-regularization if required.
    if l2_reg is not None:
        for var in policy.model.policy_variables():
            if "bias" not in var.name:
                actor_loss += l2_reg * tf.nn.l2_loss(var)
        for var in policy.model.q_variables():
            if "bias" not in var.name:
                critic_loss += l2_reg * tf.nn.l2_loss(var)

    # Model self-supervised losses.
    if policy.config["use_state_preprocessor"]:
        # Expand input_dict in case custom_loss' need them.
        input_dict[SampleBatch.ACTIONS] = train_batch[SampleBatch.ACTIONS]
        input_dict[SampleBatch.REWARDS] = train_batch[SampleBatch.REWARDS]
        input_dict[SampleBatch.DONES] = train_batch[SampleBatch.DONES]
        input_dict[SampleBatch.NEXT_OBS] = train_batch[SampleBatch.NEXT_OBS]
        if log_once("ddpg_custom_loss"):
            logger.warning(
                "You are using a state-preprocessor with DDPG and "
                "therefore, `custom_loss` will be called on your Model! "
                "Please be aware that DDPG now uses the ModelV2 API, which "
                "merges all previously separate sub-models (policy_model, "
                "q_model, and twin_q_model) into one ModelV2, on which "
                "`custom_loss` is called, passing it "
                "[actor_loss, critic_loss] as 1st argument. "
                "You may have to change your custom loss function to handle "
                "this."
            )
        [actor_loss, critic_loss] = model.custom_loss(
            [actor_loss, critic_loss], input_dict
        )

    # Store values for stats function.
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.td_error = td_error
    policy.q_t = q_t

    # Return one loss value (even though we treat them separately in our
    # 2 optimizers: actor and critic).
    return policy.critic_loss + policy.actor_loss


def build_apply_op(
    policy: Policy, optimizer: LocalOptimizer, grads_and_vars: ModelGradients
) -> TensorType:
    # For policy gradient, update policy net one time v.s.
    # update critic net `policy_delay` time(s).
    should_apply_actor_opt = tf.equal(
        tf.math.floormod(policy.global_step, policy.config["policy_delay"]), 0
    )

    def make_apply_op():
        return policy._actor_optimizer.apply_gradients(policy._actor_grads_and_vars)

    actor_op = tf.cond(
        should_apply_actor_opt, true_fn=make_apply_op, false_fn=lambda: tf.no_op()
    )
    critic_op = policy._critic_optimizer.apply_gradients(policy._critic_grads_and_vars)
    # Increment global step & apply ops.
    if policy.config["framework"] in ["tf2", "tfe"]:
        policy.global_step.assign_add(1)
        return tf.no_op()
    else:
        with tf1.control_dependencies([tf1.assign_add(policy.global_step, 1)]):
            return tf.group(actor_op, critic_op)


def gradients_fn(
    policy: Policy, optimizer: LocalOptimizer, loss: TensorType
) -> ModelGradients:
    if policy.config["framework"] in ["tf2", "tfe"]:
        tape = optimizer.tape
        pol_weights = policy.model.policy_variables()
        actor_grads_and_vars = list(
            zip(tape.gradient(policy.actor_loss, pol_weights), pol_weights)
        )
        q_weights = policy.model.q_variables()
        critic_grads_and_vars = list(
            zip(tape.gradient(policy.critic_loss, q_weights), q_weights)
        )
    else:
        actor_grads_and_vars = policy._actor_optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables()
        )
        critic_grads_and_vars = policy._critic_optimizer.compute_gradients(
            policy.critic_loss, var_list=policy.model.q_variables()
        )

    # Clip if necessary.
    if policy.config["grad_clip"]:
        clip_func = partial(tf.clip_by_norm, clip_norm=policy.config["grad_clip"])
    else:
        clip_func = tf.identity

    # Save grads and vars for later use in `build_apply_op`.
    policy._actor_grads_and_vars = [
        (clip_func(g), v) for (g, v) in actor_grads_and_vars if g is not None
    ]
    policy._critic_grads_and_vars = [
        (clip_func(g), v) for (g, v) in critic_grads_and_vars if g is not None
    ]

    grads_and_vars = policy._actor_grads_and_vars + policy._critic_grads_and_vars

    return grads_and_vars


def build_ddpg_stats(policy: Policy, batch: SampleBatch) -> Dict[str, TensorType]:
    stats = {
        "mean_q": tf.reduce_mean(policy.q_t),
        "max_q": tf.reduce_max(policy.q_t),
        "min_q": tf.reduce_min(policy.q_t),
    }
    return stats


class ActorCriticOptimizerMixin:
    """Mixin class to generate the necessary optimizers for actor-critic algos.

    - Creates global step for counting the number of update operations.
    - Creates separate optimizers for actor, critic, and alpha.
    """

    def __init__(self, config):
        # Eager mode.
        if config["framework"] in ["tf2", "tfe"]:
            self.global_step = get_variable(0, tf_name="global_step")
            self._actor_optimizer = tf.keras.optimizers.Adam(
                learning_rate=config["actor_lr"]
            )
            self._critic_optimizer = tf.keras.optimizers.Adam(
                learning_rate=config["critic_lr"]
            )
        # Static graph mode.
        else:
            self.global_step = tf1.train.get_or_create_global_step()
            self._actor_optimizer = tf1.train.AdamOptimizer(
                learning_rate=config["actor_lr"]
            )
            self._critic_optimizer = tf1.train.AdamOptimizer(
                learning_rate=config["critic_lr"]
            )


def setup_early_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    """Call mixin classes' constructors before Policy's initialization.

    Adds the necessary optimizers to the given Policy.

    Args:
        policy: The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config: The Policy's config.
    """
    ActorCriticOptimizerMixin.__init__(policy, config)


class ComputeTDErrorMixin:
    def __init__(self, loss_fn):
        @make_tf_callable(self.get_session(), dynamic_shape=True)
        def compute_td_error(
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights
        ):
            # Do forward pass on loss to update td errors attribute
            # (one TD-error value per item in batch to update PR weights).
            loss_fn(
                self,
                self.model,
                None,
                {
                    SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_t),
                    SampleBatch.ACTIONS: tf.convert_to_tensor(act_t),
                    SampleBatch.REWARDS: tf.convert_to_tensor(rew_t),
                    SampleBatch.NEXT_OBS: tf.convert_to_tensor(obs_tp1),
                    SampleBatch.DONES: tf.convert_to_tensor(done_mask),
                    PRIO_WEIGHTS: tf.convert_to_tensor(importance_weights),
                },
            )
            # `self.td_error` is set in loss_fn.
            return self.td_error

        self.compute_td_error = compute_td_error


def setup_mid_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    ComputeTDErrorMixin.__init__(policy, ddpg_actor_critic_loss)


class TargetNetworkMixin:
    def __init__(self, config: AlgorithmConfigDict):
        @make_tf_callable(self.get_session())
        def update_target_fn(tau):
            tau = tf.convert_to_tensor(tau, dtype=tf.float32)
            update_target_expr = []
            model_vars = self.model.trainable_variables()
            target_model_vars = self.target_model.trainable_variables()
            assert len(model_vars) == len(target_model_vars), (
                model_vars,
                target_model_vars,
            )
            for var, var_target in zip(model_vars, target_model_vars):
                update_target_expr.append(
                    var_target.assign(tau * var + (1.0 - tau) * var_target)
                )
                logger.debug("Update target op {}".format(var_target))
            return tf.group(*update_target_expr)

        # Hard initial update.
        self._do_update = update_target_fn
        self.update_target(tau=1.0)

    # Support both hard and soft sync.
    def update_target(self, tau: int = None) -> None:
        self._do_update(np.float32(tau or self.config.get("tau")))

    @override(TFPolicy)
    def variables(self) -> List[TensorType]:
        return self.model.variables() + self.target_model.variables()


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    TargetNetworkMixin.__init__(policy, config)


def validate_spaces(
    policy: Policy,
    observation_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    if not isinstance(action_space, Box):
        raise UnsupportedSpaceException(
            "Action space ({}) of {} is not supported for "
            "DDPG.".format(action_space, policy)
        )
    elif len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space ({}) of {} has multiple dimensions "
            "{}. ".format(action_space, policy, action_space.shape)
            + "Consider reshaping this into a single dimension, "
            "using a Tuple action space, or the multi-agent API."
        )


DDPGTFPolicy = build_tf_policy(
    name="DDPGTFPolicy",
    get_default_config=lambda: ray.rllib.algorithms.ddpg.ddpg.DEFAULT_CONFIG,
    make_model=build_ddpg_models,
    action_distribution_fn=get_distribution_inputs_and_class,
    loss_fn=ddpg_actor_critic_loss,
    stats_fn=build_ddpg_stats,
    postprocess_fn=postprocess_nstep_and_prio,
    compute_gradients_fn=gradients_fn,
    apply_gradients_fn=build_apply_op,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    validate_spaces=validate_spaces,
    before_init=setup_early_mixins,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
    mixins=[
        TargetNetworkMixin,
        ActorCriticOptimizerMixin,
        ComputeTDErrorMixin,
    ],
)
