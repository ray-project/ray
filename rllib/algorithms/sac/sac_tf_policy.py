"""
TensorFlow policy class used for SAC.
"""

import copy
import gym
from gym.spaces import Box, Discrete
from functools import partial
import logging
from typing import Dict, List, Optional, Tuple, Type, Union

import ray
import ray.experimental.tf_utils
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    postprocess_nstep_and_prio,
    PRIO_WEIGHTS,
)
from ray.rllib.algorithms.sac.sac_tf_model import SACTFModel
from ray.rllib.algorithms.sac.sac_torch_model import SACTorchModel
from ray.rllib.evaluation.episode import Episode
from ray.rllib.models import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import (
    Beta,
    Categorical,
    DiagGaussian,
    Dirichlet,
    SquashedGaussian,
    TFActionDistribution,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import TargetNetworkMixin
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.framework import get_variable, try_import_tf
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.tf_utils import huber_loss, make_tf_callable
from ray.rllib.utils.typing import (
    AgentID,
    LocalOptimizer,
    ModelGradients,
    TensorType,
    AlgorithmConfigDict,
)

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


def build_sac_model(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> ModelV2:
    """Constructs the necessary ModelV2 for the Policy and returns it.

    Args:
        policy: The TFPolicy that will use the models.
        obs_space (gym.spaces.Space): The observation space.
        action_space (gym.spaces.Space): The action space.
        config: The SAC trainer's config dict.

    Returns:
        ModelV2: The ModelV2 to be used by the Policy. Note: An additional
            target model will be created in this function and assigned to
            `policy.target_model`.
    """
    # Force-ignore any additionally provided hidden layer sizes.
    # Everything should be configured using SAC's `q_model_config` and
    # `policy_model_config` config settings.
    policy_model_config = copy.deepcopy(MODEL_DEFAULTS)
    policy_model_config.update(config["policy_model_config"])
    q_model_config = copy.deepcopy(MODEL_DEFAULTS)
    q_model_config.update(config["q_model_config"])

    default_model_cls = SACTorchModel if config["framework"] == "torch" else SACTFModel

    model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=None,
        model_config=config["model"],
        framework=config["framework"],
        default_model=default_model_cls,
        name="sac_model",
        policy_model_config=policy_model_config,
        q_model_config=q_model_config,
        twin_q=config["twin_q"],
        initial_alpha=config["initial_alpha"],
        target_entropy=config["target_entropy"],
    )

    assert isinstance(model, default_model_cls)

    # Create an exact copy of the model and store it in `policy.target_model`.
    # This will be used for tau-synched Q-target models that run behind the
    # actual Q-networks and are used for target q-value calculations in the
    # loss terms.
    policy.target_model = ModelCatalog.get_model_v2(
        obs_space=obs_space,
        action_space=action_space,
        num_outputs=None,
        model_config=config["model"],
        framework=config["framework"],
        default_model=default_model_cls,
        name="target_sac_model",
        policy_model_config=policy_model_config,
        q_model_config=q_model_config,
        twin_q=config["twin_q"],
        initial_alpha=config["initial_alpha"],
        target_entropy=config["target_entropy"],
    )

    assert isinstance(policy.target_model, default_model_cls)

    return model


def postprocess_trajectory(
    policy: Policy,
    sample_batch: SampleBatch,
    other_agent_batches: Optional[Dict[AgentID, SampleBatch]] = None,
    episode: Optional[Episode] = None,
) -> SampleBatch:
    """Postprocesses a trajectory and returns the processed trajectory.

    The trajectory contains only data from one episode and from one agent.
    - If  `config.batch_mode=truncate_episodes` (default), sample_batch may
    contain a truncated (at-the-end) episode, in case the
    `config.rollout_fragment_length` was reached by the sampler.
    - If `config.batch_mode=complete_episodes`, sample_batch will contain
    exactly one episode (no matter how long).
    New columns can be added to sample_batch and existing ones may be altered.

    Args:
        policy: The Policy used to generate the trajectory
            (`sample_batch`)
        sample_batch: The SampleBatch to postprocess.
        other_agent_batches (Optional[Dict[AgentID, SampleBatch]]): Optional
            dict of AgentIDs mapping to other agents' trajectory data (from the
            same episode). NOTE: The other agents use the same policy.
        episode (Optional[Episode]): Optional multi-agent episode
            object in which the agents operated.

    Returns:
        SampleBatch: The postprocessed, modified SampleBatch (or a new one).
    """
    return postprocess_nstep_and_prio(policy, sample_batch)


def _get_dist_class(
    policy: Policy, config: AlgorithmConfigDict, action_space: gym.spaces.Space
) -> Type[TFActionDistribution]:
    """Helper function to return a dist class based on config and action space.

    Args:
        policy: The policy for which to return the action
            dist class.
        config: The Algorithm's config dict.
        action_space (gym.spaces.Space): The action space used.

    Returns:
        Type[TFActionDistribution]: A TF distribution class.
    """
    if hasattr(policy, "dist_class") and policy.dist_class is not None:
        return policy.dist_class
    elif config["model"].get("custom_action_dist"):
        action_dist_class, _ = ModelCatalog.get_action_dist(
            action_space, config["model"], framework="tf"
        )
        return action_dist_class
    elif isinstance(action_space, Discrete):
        return Categorical
    elif isinstance(action_space, Simplex):
        return Dirichlet
    else:
        assert isinstance(action_space, Box)
        if config["normalize_actions"]:
            return SquashedGaussian if not config["_use_beta_distribution"] else Beta
        else:
            return DiagGaussian


def get_distribution_inputs_and_class(
    policy: Policy,
    model: ModelV2,
    obs_batch: TensorType,
    *,
    explore: bool = True,
    **kwargs
) -> Tuple[TensorType, Type[TFActionDistribution], List[TensorType]]:
    """The action distribution function to be used the algorithm.

    An action distribution function is used to customize the choice of action
    distribution class and the resulting action distribution inputs (to
    parameterize the distribution object).
    After parameterizing the distribution, a `sample()` call
    will be made on it to generate actions.

    Args:
        policy: The Policy being queried for actions and calling this
            function.
        model: The SAC specific Model to use to generate the
            distribution inputs (see sac_tf|torch_model.py). Must support the
            `get_action_model_outputs` method.
        obs_batch: The observations to be used as inputs to the
            model.
        explore: Whether to activate exploration or not.

    Returns:
        Tuple[TensorType, Type[TFActionDistribution], List[TensorType]]: The
            dist inputs, dist class, and a list of internal state outputs
            (in the RNN case).
    """
    # Get base-model (forward) output (this should be a noop call).
    forward_out, state_out = model(
        SampleBatch(obs=obs_batch, _is_training=policy._get_is_training_placeholder()),
        [],
        None,
    )
    # Use the base output to get the policy outputs from the SAC model's
    # policy components.
    distribution_inputs, _ = model.get_action_model_outputs(forward_out)
    # Get a distribution class to be used with the just calculated dist-inputs.
    action_dist_class = _get_dist_class(policy, policy.config, policy.action_space)

    return distribution_inputs, action_dist_class, state_out


def sac_actor_critic_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TFActionDistribution],
    train_batch: SampleBatch,
) -> Union[TensorType, List[TensorType]]:
    """Constructs the loss for the Soft Actor Critic.

    Args:
        policy: The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]: The action distr. class.
        train_batch: The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    # Should be True only for debugging purposes (e.g. test cases)!
    deterministic = policy.config["_deterministic_loss"]

    _is_training = policy._get_is_training_placeholder()
    # Get the base model output from the train batch.
    model_out_t, _ = model(
        SampleBatch(obs=train_batch[SampleBatch.CUR_OBS], _is_training=_is_training),
        [],
        None,
    )

    # Get the base model output from the next observations in the train batch.
    model_out_tp1, _ = model(
        SampleBatch(obs=train_batch[SampleBatch.NEXT_OBS], _is_training=_is_training),
        [],
        None,
    )

    # Get the target model's base outputs from the next observations in the
    # train batch.
    target_model_out_tp1, _ = policy.target_model(
        SampleBatch(obs=train_batch[SampleBatch.NEXT_OBS], _is_training=_is_training),
        [],
        None,
    )

    # Discrete actions case.
    if model.discrete:
        # Get all action probs directly from pi and form their logp.
        action_dist_inputs_t, _ = model.get_action_model_outputs(model_out_t)
        log_pis_t = tf.nn.log_softmax(action_dist_inputs_t, -1)
        policy_t = tf.math.exp(log_pis_t)

        action_dist_inputs_tp1, _ = model.get_action_model_outputs(model_out_tp1)
        log_pis_tp1 = tf.nn.log_softmax(action_dist_inputs_tp1, -1)
        policy_tp1 = tf.math.exp(log_pis_tp1)

        # Q-values.
        q_t, _ = model.get_q_values(model_out_t)
        # Target Q-values.
        q_tp1, _ = policy.target_model.get_q_values(target_model_out_tp1)
        if policy.config["twin_q"]:
            twin_q_t, _ = model.get_twin_q_values(model_out_t)
            twin_q_tp1, _ = policy.target_model.get_twin_q_values(target_model_out_tp1)
            q_tp1 = tf.reduce_min((q_tp1, twin_q_tp1), axis=0)
        q_tp1 -= model.alpha * log_pis_tp1

        # Actually selected Q-values (from the actions batch).
        one_hot = tf.one_hot(
            train_batch[SampleBatch.ACTIONS], depth=q_t.shape.as_list()[-1]
        )
        q_t_selected = tf.reduce_sum(q_t * one_hot, axis=-1)
        if policy.config["twin_q"]:
            twin_q_t_selected = tf.reduce_sum(twin_q_t * one_hot, axis=-1)
        # Discrete case: "Best" means weighted by the policy (prob) outputs.
        q_tp1_best = tf.reduce_sum(tf.multiply(policy_tp1, q_tp1), axis=-1)
        q_tp1_best_masked = (
            1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)
        ) * q_tp1_best
    # Continuous actions case.
    else:
        # Sample simgle actions from distribution.
        action_dist_class = _get_dist_class(policy, policy.config, policy.action_space)
        action_dist_inputs_t, _ = model.get_action_model_outputs(model_out_t)
        action_dist_t = action_dist_class(action_dist_inputs_t, policy.model)
        policy_t = (
            action_dist_t.sample()
            if not deterministic
            else action_dist_t.deterministic_sample()
        )
        log_pis_t = tf.expand_dims(action_dist_t.logp(policy_t), -1)

        action_dist_inputs_tp1, _ = model.get_action_model_outputs(model_out_tp1)
        action_dist_tp1 = action_dist_class(action_dist_inputs_tp1, policy.model)
        policy_tp1 = (
            action_dist_tp1.sample()
            if not deterministic
            else action_dist_tp1.deterministic_sample()
        )
        log_pis_tp1 = tf.expand_dims(action_dist_tp1.logp(policy_tp1), -1)

        # Q-values for the actually selected actions.
        q_t, _ = model.get_q_values(
            model_out_t, tf.cast(train_batch[SampleBatch.ACTIONS], tf.float32)
        )
        if policy.config["twin_q"]:
            twin_q_t, _ = model.get_twin_q_values(
                model_out_t, tf.cast(train_batch[SampleBatch.ACTIONS], tf.float32)
            )

        # Q-values for current policy in given current state.
        q_t_det_policy, _ = model.get_q_values(model_out_t, policy_t)
        if policy.config["twin_q"]:
            twin_q_t_det_policy, _ = model.get_twin_q_values(model_out_t, policy_t)
            q_t_det_policy = tf.reduce_min(
                (q_t_det_policy, twin_q_t_det_policy), axis=0
            )

        # target q network evaluation
        q_tp1, _ = policy.target_model.get_q_values(target_model_out_tp1, policy_tp1)
        if policy.config["twin_q"]:
            twin_q_tp1, _ = policy.target_model.get_twin_q_values(
                target_model_out_tp1, policy_tp1
            )
            # Take min over both twin-NNs.
            q_tp1 = tf.reduce_min((q_tp1, twin_q_tp1), axis=0)

        q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
        if policy.config["twin_q"]:
            twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 -= model.alpha * log_pis_tp1

        q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
        q_tp1_best_masked = (
            1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)
        ) * q_tp1_best

    # Compute RHS of bellman equation for the Q-loss (critic(s)).
    q_t_selected_target = tf.stop_gradient(
        tf.cast(train_batch[SampleBatch.REWARDS], tf.float32)
        + policy.config["gamma"] ** policy.config["n_step"] * q_tp1_best_masked
    )

    # Compute the TD-error (potentially clipped).
    base_td_error = tf.math.abs(q_t_selected - q_t_selected_target)
    if policy.config["twin_q"]:
        twin_td_error = tf.math.abs(twin_q_t_selected - q_t_selected_target)
        td_error = 0.5 * (base_td_error + twin_td_error)
    else:
        td_error = base_td_error

    # Calculate one or two critic losses (2 in the twin_q case).
    prio_weights = tf.cast(train_batch[PRIO_WEIGHTS], tf.float32)
    critic_loss = [tf.reduce_mean(prio_weights * huber_loss(base_td_error))]
    if policy.config["twin_q"]:
        critic_loss.append(tf.reduce_mean(prio_weights * huber_loss(twin_td_error)))

    # Alpha- and actor losses.
    # Note: In the papers, alpha is used directly, here we take the log.
    # Discrete case: Multiply the action probs as weights with the original
    # loss terms (no expectations needed).
    if model.discrete:
        alpha_loss = tf.reduce_mean(
            tf.reduce_sum(
                tf.multiply(
                    tf.stop_gradient(policy_t),
                    -model.log_alpha
                    * tf.stop_gradient(log_pis_t + model.target_entropy),
                ),
                axis=-1,
            )
        )
        actor_loss = tf.reduce_mean(
            tf.reduce_sum(
                tf.multiply(
                    # NOTE: No stop_grad around policy output here
                    # (compare with q_t_det_policy for continuous case).
                    policy_t,
                    model.alpha * log_pis_t - tf.stop_gradient(q_t),
                ),
                axis=-1,
            )
        )
    else:
        alpha_loss = -tf.reduce_mean(
            model.log_alpha * tf.stop_gradient(log_pis_t + model.target_entropy)
        )
        actor_loss = tf.reduce_mean(model.alpha * log_pis_t - q_t_det_policy)

    # Save for stats function.
    policy.policy_t = policy_t
    policy.q_t = q_t
    policy.td_error = td_error
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.alpha_loss = alpha_loss
    policy.alpha_value = model.alpha
    policy.target_entropy = model.target_entropy

    # In a custom apply op we handle the losses separately, but return them
    # combined in one loss here.
    return actor_loss + tf.math.add_n(critic_loss) + alpha_loss


def compute_and_clip_gradients(
    policy: Policy, optimizer: LocalOptimizer, loss: TensorType
) -> ModelGradients:
    """Gradients computing function (from loss tensor, using local optimizer).

    Note: For SAC, optimizer and loss are ignored b/c we have 3
    losses and 3 local optimizers (all stored in policy).
    `optimizer` will be used, though, in the tf-eager case b/c it is then a
    fake optimizer (OptimizerWrapper) object with a `tape` property to
    generate a GradientTape object for gradient recording.

    Args:
        policy: The Policy object that generated the loss tensor and
            that holds the given local optimizer.
        optimizer: The tf (local) optimizer object to
            calculate the gradients with.
        loss: The loss tensor for which gradients should be
            calculated.

    Returns:
        ModelGradients: List of the possibly clipped gradients- and variable
            tuples.
    """
    # Eager: Use GradientTape (which is a property of the `optimizer` object
    # (an OptimizerWrapper): see rllib/policy/eager_tf_policy.py).
    if policy.config["framework"] in ["tf2", "tfe"]:
        tape = optimizer.tape
        pol_weights = policy.model.policy_variables()
        actor_grads_and_vars = list(
            zip(tape.gradient(policy.actor_loss, pol_weights), pol_weights)
        )
        q_weights = policy.model.q_variables()
        if policy.config["twin_q"]:
            half_cutoff = len(q_weights) // 2
            grads_1 = tape.gradient(policy.critic_loss[0], q_weights[:half_cutoff])
            grads_2 = tape.gradient(policy.critic_loss[1], q_weights[half_cutoff:])
            critic_grads_and_vars = list(zip(grads_1, q_weights[:half_cutoff])) + list(
                zip(grads_2, q_weights[half_cutoff:])
            )
        else:
            critic_grads_and_vars = list(
                zip(tape.gradient(policy.critic_loss[0], q_weights), q_weights)
            )

        alpha_vars = [policy.model.log_alpha]
        alpha_grads_and_vars = list(
            zip(tape.gradient(policy.alpha_loss, alpha_vars), alpha_vars)
        )
    # Tf1.x: Use optimizer.compute_gradients()
    else:
        actor_grads_and_vars = policy._actor_optimizer.compute_gradients(
            policy.actor_loss, var_list=policy.model.policy_variables()
        )

        q_weights = policy.model.q_variables()
        if policy.config["twin_q"]:
            half_cutoff = len(q_weights) // 2
            base_q_optimizer, twin_q_optimizer = policy._critic_optimizer
            critic_grads_and_vars = base_q_optimizer.compute_gradients(
                policy.critic_loss[0], var_list=q_weights[:half_cutoff]
            ) + twin_q_optimizer.compute_gradients(
                policy.critic_loss[1], var_list=q_weights[half_cutoff:]
            )
        else:
            critic_grads_and_vars = policy._critic_optimizer[0].compute_gradients(
                policy.critic_loss[0], var_list=q_weights
            )
        alpha_grads_and_vars = policy._alpha_optimizer.compute_gradients(
            policy.alpha_loss, var_list=[policy.model.log_alpha]
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
    policy._alpha_grads_and_vars = [
        (clip_func(g), v) for (g, v) in alpha_grads_and_vars if g is not None
    ]

    grads_and_vars = (
        policy._actor_grads_and_vars
        + policy._critic_grads_and_vars
        + policy._alpha_grads_and_vars
    )
    return grads_and_vars


def apply_gradients(
    policy: Policy, optimizer: LocalOptimizer, grads_and_vars: ModelGradients
) -> Union["tf.Operation", None]:
    """Gradients applying function (from list of "grad_and_var" tuples).

    Note: For SAC, optimizer and grads_and_vars are ignored b/c we have 3
    losses and optimizers (stored in policy).

    Args:
        policy: The Policy object whose Model(s) the given gradients
            should be applied to.
        optimizer: The tf (local) optimizer object through
            which to apply the gradients.
        grads_and_vars: The list of grad_and_var tuples to
            apply via the given optimizer.

    Returns:
        Union[tf.Operation, None]: The tf op to be used to run the apply
            operation. None for eager mode.
    """
    actor_apply_ops = policy._actor_optimizer.apply_gradients(
        policy._actor_grads_and_vars
    )

    cgrads = policy._critic_grads_and_vars
    half_cutoff = len(cgrads) // 2
    if policy.config["twin_q"]:
        critic_apply_ops = [
            policy._critic_optimizer[0].apply_gradients(cgrads[:half_cutoff]),
            policy._critic_optimizer[1].apply_gradients(cgrads[half_cutoff:]),
        ]
    else:
        critic_apply_ops = [policy._critic_optimizer[0].apply_gradients(cgrads)]

    # Eager mode -> Just apply and return None.
    if policy.config["framework"] in ["tf2", "tfe"]:
        policy._alpha_optimizer.apply_gradients(policy._alpha_grads_and_vars)
        return
    # Tf static graph -> Return op.
    else:
        alpha_apply_ops = policy._alpha_optimizer.apply_gradients(
            policy._alpha_grads_and_vars,
            global_step=tf1.train.get_or_create_global_step(),
        )
        return tf.group([actor_apply_ops, alpha_apply_ops] + critic_apply_ops)


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    """Stats function for SAC. Returns a dict with important loss stats.

    Args:
        policy: The Policy to generate stats for.
        train_batch: The SampleBatch (already) used for training.

    Returns:
        Dict[str, TensorType]: The stats dict.
    """
    return {
        "mean_td_error": tf.reduce_mean(policy.td_error),
        "actor_loss": tf.reduce_mean(policy.actor_loss),
        "critic_loss": tf.reduce_mean(policy.critic_loss),
        "alpha_loss": tf.reduce_mean(policy.alpha_loss),
        "alpha_value": tf.reduce_mean(policy.alpha_value),
        "target_entropy": tf.constant(policy.target_entropy),
        "mean_q": tf.reduce_mean(policy.q_t),
        "max_q": tf.reduce_max(policy.q_t),
        "min_q": tf.reduce_min(policy.q_t),
    }


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
                learning_rate=config["optimization"]["actor_learning_rate"]
            )
            self._critic_optimizer = [
                tf.keras.optimizers.Adam(
                    learning_rate=config["optimization"]["critic_learning_rate"]
                )
            ]
            if config["twin_q"]:
                self._critic_optimizer.append(
                    tf.keras.optimizers.Adam(
                        learning_rate=config["optimization"]["critic_learning_rate"]
                    )
                )
            self._alpha_optimizer = tf.keras.optimizers.Adam(
                learning_rate=config["optimization"]["entropy_learning_rate"]
            )
        # Static graph mode.
        else:
            self.global_step = tf1.train.get_or_create_global_step()
            self._actor_optimizer = tf1.train.AdamOptimizer(
                learning_rate=config["optimization"]["actor_learning_rate"]
            )
            self._critic_optimizer = [
                tf1.train.AdamOptimizer(
                    learning_rate=config["optimization"]["critic_learning_rate"]
                )
            ]
            if config["twin_q"]:
                self._critic_optimizer.append(
                    tf1.train.AdamOptimizer(
                        learning_rate=config["optimization"]["critic_learning_rate"]
                    )
                )
            self._alpha_optimizer = tf1.train.AdamOptimizer(
                learning_rate=config["optimization"]["entropy_learning_rate"]
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


# TODO: Unify with DDPG's ComputeTDErrorMixin when SAC policy subclasses PolicyV2
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
    """Call mixin classes' constructors before Policy's loss initialization.

    Adds the `compute_td_error` method to the given policy.
    Calling `compute_td_error` with batch data will re-calculate the loss
    on that batch AND return the per-batch-item TD-error for prioritized
    replay buffer record weight updating (in case a prioritized replay buffer
    is used).

    Args:
        policy: The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config: The Policy's config.
    """
    ComputeTDErrorMixin.__init__(policy, sac_actor_critic_loss)


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    """Call mixin classes' constructors after Policy initialization.

    Adds the `update_target` method to the given policy.
    Calling `update_target` updates all target Q-networks' weights from their
    respective "main" Q-metworks, based on tau (smooth, partial updating).

    Args:
        policy: The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config: The Policy's config.
    """
    TargetNetworkMixin.__init__(policy)


def validate_spaces(
    policy: Policy,
    observation_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    """Validates the observation- and action spaces used for the Policy.

    Args:
        policy: The policy, whose spaces are being validated.
        observation_space (gym.spaces.Space): The observation space to
            validate.
        action_space (gym.spaces.Space): The action space to validate.
        config: The Policy's config dict.

    Raises:
        UnsupportedSpaceException: If one of the spaces is not supported.
    """
    # Only support single Box or single Discrete spaces.
    if not isinstance(action_space, (Box, Discrete, Simplex)):
        raise UnsupportedSpaceException(
            "Action space ({}) of {} is not supported for "
            "SAC. Must be [Box|Discrete|Simplex].".format(action_space, policy)
        )
    # If Box, make sure it's a 1D vector space.
    elif isinstance(action_space, (Box, Simplex)) and len(action_space.shape) > 1:
        raise UnsupportedSpaceException(
            "Action space ({}) of {} has multiple dimensions "
            "{}. ".format(action_space, policy, action_space.shape)
            + "Consider reshaping this into a single dimension, "
            "using a Tuple action space, or the multi-agent API."
        )


# Build a child class of `DynamicTFPolicy`, given the custom functions defined
# above.
SACTFPolicy = build_tf_policy(
    name="SACTFPolicy",
    get_default_config=lambda: ray.rllib.algorithms.sac.sac.DEFAULT_CONFIG,
    make_model=build_sac_model,
    postprocess_fn=postprocess_trajectory,
    action_distribution_fn=get_distribution_inputs_and_class,
    loss_fn=sac_actor_critic_loss,
    stats_fn=stats,
    compute_gradients_fn=compute_and_clip_gradients,
    apply_gradients_fn=apply_gradients,
    extra_learn_fetches_fn=lambda policy: {"td_error": policy.td_error},
    mixins=[TargetNetworkMixin, ActorCriticOptimizerMixin, ComputeTDErrorMixin],
    validate_spaces=validate_spaces,
    before_init=setup_early_mixins,
    before_loss_init=setup_mid_mixins,
    after_init=setup_late_mixins,
)
