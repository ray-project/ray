"""
PyTorch policy class used for REDQ.
"""
import logging
from typing import List, Tuple, Type, Union

import gymnasium as gym
import redq
from utils import build_redq_model

from ray.rllib.algorithms.dqn.dqn_tf_policy import PRIO_WEIGHTS
from ray.rllib.algorithms.sac.sac_tf_policy import (
    postprocess_trajectory,
    validate_spaces,
)
from ray.rllib.algorithms.sac.sac_torch_policy import (
    ComputeTDErrorMixin,
    F,
    TargetNetworkMixin,
    _get_dist_class,
    action_distribution_fn,
    setup_late_mixins,
    stats,
    torch,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDistributionWrapper
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    concat_multi_gpu_td_errors,
    huber_loss,
)
from ray.rllib.utils.typing import AlgorithmConfigDict, LocalOptimizer, TensorType

logger = logging.getLogger(__name__)


def build_redq_model_and_action_dist(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
    """Constructs the necessary ModelV2 and action dist class for the Policy.

    Args:
        policy: The TFPolicy that will use the models.
        obs_space (gym.spaces.Space): The observation space.
        action_space (gym.spaces.Space): The action space.
        config: The REDQ trainer's config dict.

    Returns:
        ModelV2: The ModelV2 to be used by the Policy. Note: An additional
            target model will be created in this function and assigned to
            `policy.target_model`.
    """
    model = build_redq_model(policy, obs_space, action_space, config)
    action_dist_class = _get_dist_class(policy, config, action_space)
    return model, action_dist_class


def actor_critic_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: Type[TorchDistributionWrapper],
    train_batch: SampleBatch,
) -> Union[TensorType, List[TensorType]]:
    """Constructs the loss for the REDQ.

    Args:
        policy: The Policy to calculate the loss for.
        model (ModelV2): The Model to calculate the loss for.
        dist_class (Type[TorchDistributionWrapper]: The action distr. class.
        train_batch: The training data.

    Returns:
        Union[TensorType, List[TensorType]]: A single loss tensor or a list
            of loss tensors.
    """
    # Look up the target model (tower) using the model tower.
    target_model = policy.target_models[model]

    # Should be True only for debugging purposes (e.g. test cases)!
    deterministic = policy.config["_deterministic_loss"]

    model_out_t, _ = model(
        SampleBatch(obs=train_batch[SampleBatch.CUR_OBS], _is_training=True), [], None
    )

    model_out_tp1, _ = model(
        SampleBatch(obs=train_batch[SampleBatch.NEXT_OBS], _is_training=True), [], None
    )

    target_model_out_tp1, _ = target_model(
        SampleBatch(obs=train_batch[SampleBatch.NEXT_OBS], _is_training=True), [], None
    )

    num_critics = policy.config["num_critics"]

    target_q_fcn_aggregator = model.target_q_fcn_aggregator

    q_fcn_aggregator = model.q_fcn_aggregator

    alpha = torch.exp(model.log_alpha)

    # Discrete case.
    if model.discrete:
        # Get all action probs directly from pi and form their logp.
        action_dist_inputs_t, _ = model.get_action_model_outputs(model_out_t)
        log_pis_t = F.log_softmax(action_dist_inputs_t, dim=-1)
        policy_t = torch.exp(log_pis_t)

        action_dist_inputs_tp1, _ = model.get_action_model_outputs(model_out_tp1)
        log_pis_tp1 = F.log_softmax(action_dist_inputs_tp1, -1)
        policy_tp1 = torch.exp(log_pis_tp1)

        # For critic loss. Q-values for all critics
        q_ts, _ = model.get_q_values(model_out_t, num_critics=-1)
        one_hot = F.one_hot(
            train_batch[SampleBatch.ACTIONS].long(), num_classes=q_ts[0].size()[-1]
        )
        q_ts_selected = [torch.sum(q_t * one_hot, dim=-1) for q_t in q_ts]

        # For target value update.
        # Discrete case: "Best" means weighted by the policy (prob) outputs.
        q_tp1s, _ = target_model.get_q_values(
            target_model_out_tp1, num_critics=num_critics
        )
        q_tp1 = target_q_fcn_aggregator(q_tp1s) - alpha * log_pis_tp1

        q_tp1_best = torch.sum(torch.mul(policy_tp1, q_tp1), dim=-1)

        # For actor loss.
        # Q-values from all critics with the "re-parameterized" policy.
        q_t_with_reparam_policy = q_fcn_aggregator(q_ts)
    # Continuous actions case.
    else:
        # Sample single actions from distribution.
        action_dist_class = _get_dist_class(policy, policy.config, policy.action_space)
        action_dist_inputs_t, _ = model.get_action_model_outputs(model_out_t)
        action_dist_t = action_dist_class(action_dist_inputs_t, model)
        policy_t = (
            action_dist_t.sample()
            if not deterministic
            else action_dist_t.deterministic_sample()
        )
        log_pis_t = torch.unsqueeze(action_dist_t.logp(policy_t), -1)

        action_dist_inputs_tp1, _ = model.get_action_model_outputs(model_out_tp1)
        action_dist_tp1 = action_dist_class(action_dist_inputs_tp1, model)
        policy_tp1 = (
            action_dist_tp1.sample()
            if not deterministic
            else action_dist_tp1.deterministic_sample()
        )
        log_pis_tp1 = torch.unsqueeze(action_dist_tp1.logp(policy_tp1), -1)

        # For critic Loss. Q-values from all critics for the actually selected actions.
        q_ts, _ = model.get_q_values(
            model_out_t, train_batch[SampleBatch.ACTIONS], num_critics=-1
        )
        q_ts_selected = [torch.squeeze(q_t, dim=-1) for q_t in q_ts]

        # For target value update.
        # Target Q network evaluation for sampled target critics
        q_tp1s, _ = target_model.get_q_values(
            target_model_out_tp1, policy_tp1, num_critics=num_critics
        )
        q_tp1 = target_q_fcn_aggregator(q_tp1s) - alpha * log_pis_tp1
        q_tp1_best = torch.squeeze(input=q_tp1, dim=-1)

        # For actor loss.
        # Q-values from all critics with re-parameterized policy in given current state.
        q_t_with_reparam_policy, _ = model.get_q_values(
            model_out_t, policy_t, num_critics=-1
        )
        q_t_with_reparam_policy = q_fcn_aggregator(q_t_with_reparam_policy)

    # compute RHS of bellman equation
    q_tp1_best_masked = (1.0 - train_batch[SampleBatch.DONES].float()) * q_tp1_best
    y_target = (
        train_batch[SampleBatch.REWARDS]
        + (policy.config["gamma"] ** policy.config["n_step"]) * q_tp1_best_masked
    ).detach()

    # Critic loss
    # Compute the mean TD-error for the ensemble.
    td_error = 0
    critic_loss = []
    for cur_qts in q_ts_selected:
        cur_error = torch.abs(cur_qts - y_target)
        td_error += cur_error
        critic_loss.append(
            torch.mean(train_batch[PRIO_WEIGHTS] * huber_loss(cur_error))
        )
    td_error /= float(len(q_ts_selected))

    # Alpha loss.
    # Note: Since we optimize over model.log_alpha,
    # we need to take the gradient wrt to model.log_alpha
    # so that we compute the gradient of the alpha loss correctly
    weighted_alpha = -model.log_alpha * (log_pis_t.detach() + model.target_entropy)
    if model.discrete:
        # Discrete case: Multiply the action probs as weights with the original
        # loss terms and sum up weighted terms.
        weighted_alpha = torch.sum(policy_t.detach() * weighted_alpha, dim=-1)
    # Mean over all batch items
    alpha_loss = torch.mean(weighted_alpha)

    # Actor loss
    if model.discrete:
        actor_loss = torch.mean(
            torch.sum(
                torch.mul(
                    # NOTE: No stop_grad around policy output here
                    # (compare with q_t_det_policy for continuous case).
                    policy_t,
                    alpha.detach() * log_pis_t - q_t_with_reparam_policy.detach(),
                ),
                dim=-1,
            )
        )
    else:
        # Note: Do not detach q_t_det_policy here b/c is depends partly
        # on the policy vars (policy sample pushed through Q-net).
        # However, we must make sure `actor_loss` is not used to update
        # the Q-net(s)' variables.
        actor_loss = torch.mean(alpha.detach() * log_pis_t - q_t_with_reparam_policy)
    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    model.tower_stats["q_t"] = torch.stack(q_ts)
    model.tower_stats["policy_t"] = policy_t
    model.tower_stats["log_pis_t"] = log_pis_t
    model.tower_stats["actor_loss"] = actor_loss
    model.tower_stats["critic_loss"] = critic_loss
    model.tower_stats["alpha_loss"] = alpha_loss

    # TD-error tensor in final stats
    # will be concatenated and retrieved for each individual batch item.
    model.tower_stats["td_error"] = td_error

    # Return all loss terms corresponding to our optimizers.
    return tuple([actor_loss] + critic_loss + [alpha_loss])


def optimizer_fn(policy: Policy, config: AlgorithmConfigDict) -> Tuple[LocalOptimizer]:
    """Creates all necessary optimizers for REDQ learning.

    The ensemble_size + 2 optimizers returned here correspond to the
    number of loss terms returned by the loss function.

    Args:
        policy: The policy object to be trained.
        config: The Algorithm's config dict.

    Returns:
        Tuple[LocalOptimizer]: The local optimizers to use for policy training.
    """
    policy.actor_optim = torch.optim.Adam(
        params=policy.model.policy_variables(),
        lr=config["optimization"]["actor_learning_rate"],
        eps=1e-7,  # to match tf.keras.optimizers.Adam's epsilon default
    )

    # don't quite understand why separate here
    ensemble_size = config["ensemble_size"]
    critic_split = len(policy.model.q_variables()) // ensemble_size
    policy.critic_optims = []
    for idx in range(ensemble_size):
        policy.critic_optims.append(
            torch.optim.Adam(
                params=policy.model.q_variables()[
                    idx * critic_split : (idx + 1) * critic_split
                ],
                lr=config["optimization"]["critic_learning_rate"],
                eps=1e-7,  # to match tf.keras.optimizers.Adam's epsilon default
            )
        )

    policy.alpha_optim = torch.optim.Adam(
        params=[policy.model.log_alpha],
        lr=config["optimization"]["entropy_learning_rate"],
        eps=1e-7,  # to match tf.keras.optimizers.Adam's epsilon default
    )
    return tuple([policy.actor_optim] + policy.critic_optims + [policy.alpha_optim])


# Build a child class of `TorchPolicy`, given the custom functions defined
# above.
REDQTorchPolicy = build_policy_class(
    name="REDQTorchPolicy",
    framework="torch",
    loss_fn=actor_critic_loss,
    get_default_config=lambda: redq.DEFAULT_CONFIG,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=optimizer_fn,
    validate_spaces=validate_spaces,
    before_loss_init=setup_late_mixins,
    make_model_and_action_dist=build_redq_model_and_action_dist,
    extra_learn_fetches_fn=concat_multi_gpu_td_errors,
    mixins=[TargetNetworkMixin, ComputeTDErrorMixin],
    action_distribution_fn=action_distribution_fn,
)
