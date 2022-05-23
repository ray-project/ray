"""
TensorFlow policy class used for APPO.

Adapted from VTraceTFPolicy to use the PPO surrogate loss.
Keep in sync with changes to VTraceTFPolicy.
"""

import numpy as np
import logging
import gym
from typing import Dict, List, Optional, Type, Union

from ray.rllib.agents.impala import vtrace_tf as vtrace
from ray.rllib.agents.impala.vtrace_tf_policy import (
    _make_time_major,
    clip_gradients,
    choose_optimizer,
)
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
    Postprocessing,
)
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    EntropyCoeffSchedule,
    LearningRateSchedule,
    KLCoeffMixin,
    ValueNetworkMixin,
)
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance, make_tf_callable
from ray.rllib.utils.typing import AgentID, TensorType, TrainerConfigDict

tf1, tf, tfv = try_import_tf()

POLICY_SCOPE = "func"
TARGET_POLICY_SCOPE = "target_func"

logger = logging.getLogger(__name__)


def make_appo_model(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> ModelV2:
    """Builds model and target model for APPO.

    Args:
        policy (Policy): The Policy, which will use the model for optimization.
        obs_space (gym.spaces.Space): The policy's observation space.
        action_space (gym.spaces.Space): The policy's action space.
        config (TrainerConfigDict):

    Returns:
        ModelV2: The Model for the Policy to use.
            Note: The target model will not be returned, just assigned to
            `policy.target_model`.
    """
    # Get the num_outputs for the following model construction calls.
    _, logit_dim = ModelCatalog.get_action_dist(action_space, config["model"])

    # Construct the (main) model.
    policy.model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        logit_dim,
        config["model"],
        name=POLICY_SCOPE,
        framework="torch" if config["framework"] == "torch" else "tf",
    )
    policy.model_variables = policy.model.variables()

    # Construct the target model.
    policy.target_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        logit_dim,
        config["model"],
        name=TARGET_POLICY_SCOPE,
        framework="torch" if config["framework"] == "torch" else "tf",
    )
    policy.target_model_variables = policy.target_model.variables()

    # Return only the model (not the target model).
    return policy.model


def appo_surrogate_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TFActionDistribution],
    train_batch: SampleBatch,
) -> Union[TensorType, List[TensorType]]:
    """Constructs the loss for APPO.

    With IS modifications and V-trace for Advantage Estimation.

    Args:
        policy (Policy): The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[ActionDistribution]): The action distr. class.
        train_batch (SampleBatch): The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    model_out, _ = model(train_batch)
    action_dist = dist_class(model_out, model)

    if isinstance(policy.action_space, gym.spaces.Discrete):
        is_multidiscrete = False
        output_hidden_shape = [policy.action_space.n]
    elif isinstance(policy.action_space, gym.spaces.multi_discrete.MultiDiscrete):
        is_multidiscrete = True
        output_hidden_shape = policy.action_space.nvec.astype(np.int32)
    else:
        is_multidiscrete = False
        output_hidden_shape = 1

    # TODO: (sven) deprecate this when trajectory view API gets activated.
    def make_time_major(*args, **kw):
        return _make_time_major(
            policy, train_batch.get(SampleBatch.SEQ_LENS), *args, **kw
        )

    actions = train_batch[SampleBatch.ACTIONS]
    dones = train_batch[SampleBatch.DONES]
    rewards = train_batch[SampleBatch.REWARDS]
    behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]

    target_model_out, _ = policy.target_model(train_batch)
    prev_action_dist = dist_class(behaviour_logits, policy.model)
    values = policy.model.value_function()
    values_time_major = make_time_major(values)

    policy.model_vars = policy.model.variables()
    policy.target_model_vars = policy.target_model.variables()

    if policy.is_recurrent():
        max_seq_len = tf.reduce_max(train_batch[SampleBatch.SEQ_LENS])
        mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
        mask = tf.reshape(mask, [-1])
        mask = make_time_major(mask, drop_last=policy.config["vtrace"])

        def reduce_mean_valid(t):
            return tf.reduce_mean(tf.boolean_mask(t, mask))

    else:
        reduce_mean_valid = tf.reduce_mean

    if policy.config["vtrace"]:
        drop_last = policy.config["vtrace_drop_last_ts"]
        logger.debug(
            "Using V-Trace surrogate loss (vtrace=True; " f"drop_last={drop_last})"
        )

        # Prepare actions for loss.
        loss_actions = actions if is_multidiscrete else tf.expand_dims(actions, axis=1)

        old_policy_behaviour_logits = tf.stop_gradient(target_model_out)
        old_policy_action_dist = dist_class(old_policy_behaviour_logits, model)

        # Prepare KL for Loss
        mean_kl = make_time_major(
            old_policy_action_dist.multi_kl(action_dist), drop_last=drop_last
        )

        unpacked_behaviour_logits = tf.split(
            behaviour_logits, output_hidden_shape, axis=1
        )
        unpacked_old_policy_behaviour_logits = tf.split(
            old_policy_behaviour_logits, output_hidden_shape, axis=1
        )

        # Compute vtrace on the CPU for better perf.
        with tf.device("/cpu:0"):
            vtrace_returns = vtrace.multi_from_logits(
                behaviour_policy_logits=make_time_major(
                    unpacked_behaviour_logits, drop_last=drop_last
                ),
                target_policy_logits=make_time_major(
                    unpacked_old_policy_behaviour_logits, drop_last=drop_last
                ),
                actions=tf.unstack(
                    make_time_major(loss_actions, drop_last=drop_last), axis=2
                ),
                discounts=tf.cast(
                    ~make_time_major(tf.cast(dones, tf.bool), drop_last=drop_last),
                    tf.float32,
                )
                * policy.config["gamma"],
                rewards=make_time_major(rewards, drop_last=drop_last),
                values=values_time_major[:-1] if drop_last else values_time_major,
                bootstrap_value=values_time_major[-1],
                dist_class=Categorical if is_multidiscrete else dist_class,
                model=model,
                clip_rho_threshold=tf.cast(
                    policy.config["vtrace_clip_rho_threshold"], tf.float32
                ),
                clip_pg_rho_threshold=tf.cast(
                    policy.config["vtrace_clip_pg_rho_threshold"], tf.float32
                ),
            )

        actions_logp = make_time_major(action_dist.logp(actions), drop_last=drop_last)
        prev_actions_logp = make_time_major(
            prev_action_dist.logp(actions), drop_last=drop_last
        )
        old_policy_actions_logp = make_time_major(
            old_policy_action_dist.logp(actions), drop_last=drop_last
        )

        is_ratio = tf.clip_by_value(
            tf.math.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0
        )
        logp_ratio = is_ratio * tf.exp(actions_logp - prev_actions_logp)
        policy._is_ratio = is_ratio

        advantages = vtrace_returns.pg_advantages
        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages
            * tf.clip_by_value(
                logp_ratio,
                1 - policy.config["clip_param"],
                1 + policy.config["clip_param"],
            ),
        )

        action_kl = tf.reduce_mean(mean_kl, axis=0) if is_multidiscrete else mean_kl
        mean_kl_loss = reduce_mean_valid(action_kl)
        mean_policy_loss = -reduce_mean_valid(surrogate_loss)

        # The value function loss.
        if drop_last:
            delta = values_time_major[:-1] - vtrace_returns.vs
        else:
            delta = values_time_major - vtrace_returns.vs
        value_targets = vtrace_returns.vs
        mean_vf_loss = 0.5 * reduce_mean_valid(tf.math.square(delta))

        # The entropy loss.
        actions_entropy = make_time_major(action_dist.multi_entropy(), drop_last=True)
        mean_entropy = reduce_mean_valid(actions_entropy)

    else:
        logger.debug("Using PPO surrogate loss (vtrace=False)")

        # Prepare KL for Loss
        mean_kl = make_time_major(prev_action_dist.multi_kl(action_dist))

        logp_ratio = tf.math.exp(
            make_time_major(action_dist.logp(actions))
            - make_time_major(prev_action_dist.logp(actions))
        )

        advantages = make_time_major(train_batch[Postprocessing.ADVANTAGES])
        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages
            * tf.clip_by_value(
                logp_ratio,
                1 - policy.config["clip_param"],
                1 + policy.config["clip_param"],
            ),
        )

        action_kl = tf.reduce_mean(mean_kl, axis=0) if is_multidiscrete else mean_kl
        mean_kl_loss = reduce_mean_valid(action_kl)
        mean_policy_loss = -reduce_mean_valid(surrogate_loss)

        # The value function loss.
        value_targets = make_time_major(train_batch[Postprocessing.VALUE_TARGETS])
        delta = values_time_major - value_targets
        mean_vf_loss = 0.5 * reduce_mean_valid(tf.math.square(delta))

        # The entropy loss.
        mean_entropy = reduce_mean_valid(make_time_major(action_dist.multi_entropy()))

    # The summed weighted loss.
    total_loss = mean_policy_loss - mean_entropy * policy.entropy_coeff
    # Optional KL loss.
    if policy.config["use_kl_loss"]:
        total_loss += policy.kl_coeff * mean_kl_loss
    # Optional vf loss (or in a separate term due to separate
    # optimizers/networks).
    loss_wo_vf = total_loss
    if not policy.config["_separate_vf_optimizer"]:
        total_loss += mean_vf_loss * policy.config["vf_loss_coeff"]

    # Store stats in policy for stats_fn.
    policy._total_loss = total_loss
    policy._loss_wo_vf = loss_wo_vf
    policy._mean_policy_loss = mean_policy_loss
    # Backward compatibility: Deprecate policy._mean_kl.
    policy._mean_kl_loss = policy._mean_kl = mean_kl_loss
    policy._mean_vf_loss = mean_vf_loss
    policy._mean_entropy = mean_entropy
    policy._value_targets = value_targets

    # Return one total loss or two losses: vf vs rest (policy + kl).
    if policy.config["_separate_vf_optimizer"]:
        return loss_wo_vf, mean_vf_loss
    else:
        return total_loss


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    """Stats function for APPO. Returns a dict with important loss stats.

    Args:
        policy (Policy): The Policy to generate stats for.
        train_batch (SampleBatch): The SampleBatch (already) used for training.

    Returns:
        Dict[str, TensorType]: The stats dict.
    """
    values_batched = _make_time_major(
        policy,
        train_batch.get(SampleBatch.SEQ_LENS),
        policy.model.value_function(),
        drop_last=policy.config["vtrace"] and policy.config["vtrace_drop_last_ts"],
    )

    stats_dict = {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "total_loss": policy._total_loss,
        "policy_loss": policy._mean_policy_loss,
        "entropy": policy._mean_entropy,
        "var_gnorm": tf.linalg.global_norm(policy.model.trainable_variables()),
        "vf_loss": policy._mean_vf_loss,
        "vf_explained_var": explained_variance(
            tf.reshape(policy._value_targets, [-1]), tf.reshape(values_batched, [-1])
        ),
        "entropy_coeff": tf.cast(policy.entropy_coeff, tf.float64),
    }

    if policy.config["vtrace"]:
        is_stat_mean, is_stat_var = tf.nn.moments(policy._is_ratio, [0, 1])
        stats_dict["mean_IS"] = is_stat_mean
        stats_dict["var_IS"] = is_stat_var

    if policy.config["use_kl_loss"]:
        stats_dict["kl"] = policy._mean_kl_loss
        stats_dict["KL_Coeff"] = policy.kl_coeff

    return stats_dict


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
        policy (Policy): The Policy used to generate the trajectory
            (`sample_batch`)
        sample_batch (SampleBatch): The SampleBatch to postprocess.
        other_agent_batches (Optional[Dict[PolicyID, SampleBatch]]): Optional
            dict of AgentIDs mapping to other agents' trajectory data (from the
            same episode). NOTE: The other agents use the same policy.
        episode (Optional[Episode]): Optional multi-agent episode
            object in which the agents operated.

    Returns:
        SampleBatch: The postprocessed, modified SampleBatch (or a new one).
    """
    if not policy.config["vtrace"]:
        sample_batch = compute_gae_for_sample_batch(
            policy, sample_batch, other_agent_batches, episode
        )

    return sample_batch


def add_values(policy):
    out = {}
    if not policy.config["vtrace"]:
        out[SampleBatch.VF_PREDS] = policy.model.value_function()
    return out


class TargetNetworkMixin:
    """Target NN is updated by master learner via the `update_target` method.

    Updates happen every `trainer.update_target_frequency` steps. All worker
    batches are importance sampled wrt the target network to ensure a more
    stable pi_old in PPO.
    """

    def __init__(self, obs_space, action_space, config):
        @make_tf_callable(self.get_session())
        def do_update():
            assign_ops = []
            assert len(self.model_vars) == len(self.target_model_vars)
            for var, var_target in zip(self.model_vars, self.target_model_vars):
                assign_ops.append(var_target.assign(var))
            return tf.group(*assign_ops)

        self.update_target = do_update

    @override(TFPolicy)
    def variables(self):
        return self.model_vars + self.target_model_vars


def setup_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Call all mixin classes' constructors before APPOPolicy initialization.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    KLCoeffMixin.__init__(policy, config)
    ValueNetworkMixin.__init__(policy, config)
    EntropyCoeffSchedule.__init__(
        policy, config["entropy_coeff"], config["entropy_coeff_schedule"]
    )


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    """Call all mixin classes' constructors after APPOPolicy initialization.

    Args:
        policy (Policy): The Policy object.
        obs_space (gym.spaces.Space): The Policy's observation space.
        action_space (gym.spaces.Space): The Policy's action space.
        config (TrainerConfigDict): The Policy's config.
    """
    TargetNetworkMixin.__init__(policy, obs_space, action_space, config)


# Build a child class of `DynamicTFPolicy`, given the custom functions defined
# above.
AsyncPPOTFPolicy = build_tf_policy(
    name="AsyncPPOTFPolicy",
    make_model=make_appo_model,
    loss_fn=appo_surrogate_loss,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    optimizer_fn=choose_optimizer,
    compute_gradients_fn=clip_gradients,
    extra_action_out_fn=add_values,
    before_loss_init=setup_mixins,
    after_init=setup_late_mixins,
    mixins=[
        LearningRateSchedule,
        KLCoeffMixin,
        TargetNetworkMixin,
        ValueNetworkMixin,
        EntropyCoeffSchedule,
    ],
    get_batch_divisibility_req=lambda p: p.config["rollout_fragment_length"],
)
