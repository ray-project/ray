import logging
import gym
from typing import Dict, Tuple

import ray
from ray.rllib.algorithms.ddpg.ddpg_tf_policy import (
    build_ddpg_models,
    get_distribution_inputs_and_class,
    validate_spaces,
)
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    postprocess_nstep_and_prio,
    PRIO_WEIGHTS,
)
from ray.rllib.algorithms.sac.sac_torch_policy import TargetNetworkMixin
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDeterministic, TorchDirichlet
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    concat_multi_gpu_td_errors,
    huber_loss,
    l2_loss,
)
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    TensorType,
    LocalOptimizer,
    GradInfoDict,
)

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def build_ddpg_models_and_action_dist(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> Tuple[ModelV2, ActionDistribution]:
    model = build_ddpg_models(policy, obs_space, action_space, config)

    if isinstance(action_space, Simplex):
        return model, TorchDirichlet
    else:
        return model, TorchDeterministic


def ddpg_actor_critic_loss(
    policy: Policy, model: ModelV2, _, train_batch: SampleBatch
) -> TensorType:

    target_model = policy.target_models[model]

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
    target_model_out_tp1, _ = target_model(input_dict_next, [], None)

    # Policy network evaluation.
    # prev_update_ops = set(tf1.get_collection(tf.GraphKeys.UPDATE_OPS))
    policy_t = model.get_policy_output(model_out_t)
    # policy_batchnorm_update_ops = list(
    #    set(tf1.get_collection(tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)

    policy_tp1 = target_model.get_policy_output(target_model_out_tp1)

    # Action outputs.
    if policy.config["smooth_target_policy"]:
        target_noise_clip = policy.config["target_noise_clip"]
        clipped_normal_sample = torch.clamp(
            torch.normal(
                mean=torch.zeros(policy_tp1.size()), std=policy.config["target_noise"]
            ).to(policy_tp1.device),
            -target_noise_clip,
            target_noise_clip,
        )

        policy_tp1_smoothed = torch.min(
            torch.max(
                policy_tp1 + clipped_normal_sample,
                torch.tensor(
                    policy.action_space.low,
                    dtype=torch.float32,
                    device=policy_tp1.device,
                ),
            ),
            torch.tensor(
                policy.action_space.high, dtype=torch.float32, device=policy_tp1.device
            ),
        )
    else:
        # No smoothing, just use deterministic actions.
        policy_tp1_smoothed = policy_tp1

    # Q-net(s) evaluation.
    # prev_update_ops = set(tf1.get_collection(tf.GraphKeys.UPDATE_OPS))
    # Q-values for given actions & observations in given current
    q_t = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS])

    # Q-values for current policy (no noise) in given current state
    q_t_det_policy = model.get_q_values(model_out_t, policy_t)

    actor_loss = -torch.mean(q_t_det_policy)

    if twin_q:
        twin_q_t = model.get_twin_q_values(
            model_out_t, train_batch[SampleBatch.ACTIONS]
        )
    # q_batchnorm_update_ops = list(
    #     set(tf1.get_collection(tf.GraphKeys.UPDATE_OPS)) - prev_update_ops)

    # Target q-net(s) evaluation.
    q_tp1 = target_model.get_q_values(target_model_out_tp1, policy_tp1_smoothed)

    if twin_q:
        twin_q_tp1 = target_model.get_twin_q_values(
            target_model_out_tp1, policy_tp1_smoothed
        )

    q_t_selected = torch.squeeze(q_t, axis=len(q_t.shape) - 1)
    if twin_q:
        twin_q_t_selected = torch.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 = torch.min(q_tp1, twin_q_tp1)

    q_tp1_best = torch.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    q_tp1_best_masked = (1.0 - train_batch[SampleBatch.DONES].float()) * q_tp1_best

    # Compute RHS of bellman equation.
    q_t_selected_target = (
        train_batch[SampleBatch.REWARDS] + gamma ** n_step * q_tp1_best_masked
    ).detach()

    # Compute the error (potentially clipped).
    if twin_q:
        td_error = q_t_selected - q_t_selected_target
        twin_td_error = twin_q_t_selected - q_t_selected_target
        if use_huber:
            errors = huber_loss(td_error, huber_threshold) + huber_loss(
                twin_td_error, huber_threshold
            )
        else:
            errors = 0.5 * (torch.pow(td_error, 2.0) + torch.pow(twin_td_error, 2.0))
    else:
        td_error = q_t_selected - q_t_selected_target
        if use_huber:
            errors = huber_loss(td_error, huber_threshold)
        else:
            errors = 0.5 * torch.pow(td_error, 2.0)

    critic_loss = torch.mean(train_batch[PRIO_WEIGHTS] * errors)

    # Add l2-regularization if required.
    if l2_reg is not None:
        for name, var in model.policy_variables(as_dict=True).items():
            if "bias" not in name:
                actor_loss += l2_reg * l2_loss(var)
        for name, var in model.q_variables(as_dict=True).items():
            if "bias" not in name:
                critic_loss += l2_reg * l2_loss(var)

    # Model self-supervised losses.
    if policy.config["use_state_preprocessor"]:
        # Expand input_dict in case custom_loss' need them.
        input_dict[SampleBatch.ACTIONS] = train_batch[SampleBatch.ACTIONS]
        input_dict[SampleBatch.REWARDS] = train_batch[SampleBatch.REWARDS]
        input_dict[SampleBatch.DONES] = train_batch[SampleBatch.DONES]
        input_dict[SampleBatch.NEXT_OBS] = train_batch[SampleBatch.NEXT_OBS]
        [actor_loss, critic_loss] = model.custom_loss(
            [actor_loss, critic_loss], input_dict
        )

    # Store values for stats function in model (tower), such that for
    # multi-GPU, we do not override them during the parallel loss phase.
    model.tower_stats["q_t"] = q_t
    model.tower_stats["actor_loss"] = actor_loss
    model.tower_stats["critic_loss"] = critic_loss
    # TD-error tensor in final stats
    # will be concatenated and retrieved for each individual batch item.
    model.tower_stats["td_error"] = td_error

    # Return two loss terms (corresponding to the two optimizers, we create).
    return actor_loss, critic_loss


def make_ddpg_optimizers(
    policy: Policy, config: AlgorithmConfigDict
) -> Tuple[LocalOptimizer]:
    """Create separate optimizers for actor & critic losses."""

    # Set epsilons to match tf.keras.optimizers.Adam's epsilon default.
    policy._actor_optimizer = torch.optim.Adam(
        params=policy.model.policy_variables(), lr=config["actor_lr"], eps=1e-7
    )

    policy._critic_optimizer = torch.optim.Adam(
        params=policy.model.q_variables(), lr=config["critic_lr"], eps=1e-7
    )

    # Return them in the same order as the respective loss terms are returned.
    return policy._actor_optimizer, policy._critic_optimizer


def apply_gradients_fn(policy: Policy, gradients: GradInfoDict) -> None:
    # For policy gradient, update policy net one time v.s.
    # update critic net `policy_delay` time(s).
    if policy.global_step % policy.config["policy_delay"] == 0:
        policy._actor_optimizer.step()

    policy._critic_optimizer.step()

    # Increment global step & apply ops.
    policy.global_step += 1


def build_ddpg_stats(policy: Policy, batch: SampleBatch) -> Dict[str, TensorType]:

    q_t = torch.stack(policy.get_tower_stats("q_t"))
    stats = {
        "actor_loss": torch.mean(torch.stack(policy.get_tower_stats("actor_loss"))),
        "critic_loss": torch.mean(torch.stack(policy.get_tower_stats("critic_loss"))),
        "mean_q": torch.mean(q_t),
        "max_q": torch.max(q_t),
        "min_q": torch.min(q_t),
    }
    return stats


def before_init_fn(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    # Create global step for counting the number of update operations.
    policy.global_step = 0


class ComputeTDErrorMixin:
    def __init__(self, loss_fn):
        def compute_td_error(
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights
        ):
            input_dict = self._lazy_tensor_dict(
                SampleBatch(
                    {
                        SampleBatch.CUR_OBS: obs_t,
                        SampleBatch.ACTIONS: act_t,
                        SampleBatch.REWARDS: rew_t,
                        SampleBatch.NEXT_OBS: obs_tp1,
                        SampleBatch.DONES: done_mask,
                        PRIO_WEIGHTS: importance_weights,
                    }
                )
            )
            # Do forward pass on loss to update td errors attribute
            # (one TD-error value per item in batch to update PR weights).
            loss_fn(self, self.model, None, input_dict)

            # `self.model.td_error` is set within actor_critic_loss call.
            return self.model.tower_stats["td_error"]

        self.compute_td_error = compute_td_error


def setup_late_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: AlgorithmConfigDict,
) -> None:
    ComputeTDErrorMixin.__init__(policy, ddpg_actor_critic_loss)
    TargetNetworkMixin.__init__(policy)


DDPGTorchPolicy = build_policy_class(
    name="DDPGTorchPolicy",
    framework="torch",
    loss_fn=ddpg_actor_critic_loss,
    get_default_config=lambda: ray.rllib.algorithms.ddpg.ddpg.DEFAULT_CONFIG,
    stats_fn=build_ddpg_stats,
    postprocess_fn=postprocess_nstep_and_prio,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=make_ddpg_optimizers,
    validate_spaces=validate_spaces,
    before_init=before_init_fn,
    before_loss_init=setup_late_mixins,
    action_distribution_fn=get_distribution_inputs_and_class,
    make_model_and_action_dist=build_ddpg_models_and_action_dist,
    extra_learn_fetches_fn=concat_multi_gpu_td_errors,
    apply_gradients_fn=apply_gradients_fn,
    mixins=[
        TargetNetworkMixin,
        ComputeTDErrorMixin,
    ],
)
