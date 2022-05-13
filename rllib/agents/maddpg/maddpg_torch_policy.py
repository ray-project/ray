import logging
from typing import Dict, Tuple

import numpy as np
from gym.spaces import Box, Discrete
from ray.rllib.agents.ddpg.ddpg_tf_policy import (
    build_ddpg_models,
    get_distribution_inputs_and_class,
)
from ray.rllib.agents.ddpg.ddpg_torch_policy import (
    TargetNetworkMixin,
    apply_gradients_fn,
    make_ddpg_optimizers,
)
from ray.rllib.agents.ddpg.noop_model import TorchNoopModel
from ray.rllib.evaluation.postprocessing import adjust_nstep
from ray.rllib.models import ModelCatalog
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import TorchDeterministic
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import apply_grad_clipping, huber_loss, l2_loss
from ray.rllib.utils.typing import LocalOptimizer, TensorType, TrainerConfigDict

from maddpg_torch_model import _make_continuous_space, build_maddpg_models

logger = logging.getLogger(__name__)

torch, nn = try_import_torch()


def validate_spaces(policy: Policy, obs_space, action_space, config) -> None:
    if isinstance(obs_space, Discrete) or isinstance(action_space, Discrete):
        logging.warning(
            "Discrete spaces may not work correctly with \
        pytorch MADDPG; consider using framework=tf instead"
        )
    policy.observation_space = _make_continuous_space(obs_space)
    policy.action_space = _make_continuous_space(action_space)


def build_maddpg_models_and_action_dist(
    policy: Policy, obs_space, action_space, config: TrainerConfigDict
) -> Tuple[ModelV2, ActionDistribution]:

    model = build_maddpg_models(policy, policy.observation_space, policy.action_space, config)
    device = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
    policy.model = policy.model.to(device)
    policy.target_model = policy.target_model.to(device)

    return model, TorchDeterministic


def maddpg_actor_critic_loss(
    policy: Policy, model: ModelV2, _, train_batch: SampleBatch
) -> TensorType:
    if not hasattr(policy, "td_error") or policy.td_error is None:
        policy.actor_loss = torch.zeros(len(train_batch))
        policy.critic_loss = torch.zeros(len(train_batch))
        policy.td_error = torch.zeros(len(train_batch))
        policy.q_t = torch.zeros(len(train_batch))
        return policy.actor_loss, policy.critic_loss

    twin_q = policy.config["twin_q"]
    gamma = policy.config["gamma"]
    n_step = policy.config["n_step"]
    use_huber = policy.config["use_huber"]
    huber_threshold = policy.config["huber_threshold"]
    l2_reg = policy.config["l2_reg"]
    agent_id = policy.config["agent_id"]
    n_agents = len(policy.config["multiagent"]["policies"])

    input_dict = {
        "obs": train_batch["_".join([SampleBatch.CUR_OBS, str(agent_id)])],
        "is_training": True,
    }

    input_dict_next = {
        "obs": train_batch["_".join([SampleBatch.NEXT_OBS, str(agent_id)])],
        "is_training": True,
    }

    model_out_t, _ = model(input_dict, [], None)

    policy_t = model.get_policy_output(model_out_t)

    target_model_out_tp1, _ = policy.target_model(input_dict_next, [], None)

    policy_tp1 = policy.target_model.get_policy_output(target_model_out_tp1)

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

    obs_n = [
        train_batch["_".join([SampleBatch.CUR_OBS, str(id)])] for id in range(n_agents)
    ]
    act_n = [
        train_batch["_".join([SampleBatch.ACTIONS, str(id)])] for id in range(n_agents)
    ]
    next_obs_n = [
        train_batch["_".join([SampleBatch.NEXT_OBS, str(id)])] for id in range(n_agents)
    ]
    next_policy_n = [train_batch["new_actions_{}".format(id)] for id in range(n_agents)]
    next_policy_n[agent_id] = policy_tp1_smoothed
    rewards = train_batch["rewards_{}".format(agent_id)]
    dones = train_batch["dones_{}".format(agent_id)]

    if policy.config["use_state_preprocessor"]:
        # Create all state preprocessors
        model_n = [
            ModelCatalog.get_model_v2(
                obs_space,
                act_space,
                1,
                policy.config["model"],
                default_model=TorchNoopModel,
            )
            for obs_space, act_space in zip(policy.obs_space_n, policy.act_space_n)
        ]
        # Get states from preprocessors
        model_out_n = [
            model.forward({SampleBatch.OBS: obs, "is_training": True}, [], None)[0]
            for model, obs in zip(model_n, obs_n)
        ]
        model_out_next_n = [
            model.forward({SampleBatch.OBS: next_obs, "is_training": True}, [], None)[0]
            for model, next_obs in zip(model_n, next_obs_n)
        ]
    else:
        model_out_n = obs_n
        model_out_next_n = next_obs_n

    # Q-values for given actions & observations in given current
    q_t = model.get_q_values(model_out_n, act_n)

    # Compute this here so policy_n can be modified without deepcopying act_n
    if twin_q:
        twin_q_t = model.get_twin_q_values(model_out_n, act_n)

    # Q-values for current policy (no noise) in given current state
    policy_n = act_n
    policy_n[agent_id] = policy_t

    q_t_det_policy = model.get_q_values(model_out_n, policy_n)

    actor_loss = -torch.mean(q_t_det_policy)

    # Target q-net(s) evaluation.
    q_tp1 = policy.target_model.get_q_values(model_out_next_n, next_policy_n)

    if twin_q:
        twin_q_tp1 = policy.target_model.get_twin_q_values(
            model_out_next_n, next_policy_n
        )

    q_t_selected = torch.squeeze(q_t, axis=len(q_t.shape) - 1)

    if twin_q:
        twin_q_t_selected = torch.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
        q_tp1 = torch.min(q_tp1, twin_q_tp1)

    q_tp1_best = torch.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
    q_tp1_best_masked = (~dones).float() * q_tp1_best

    q_t_selected_target = (rewards + gamma ** n_step * q_tp1_best_masked).detach()

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

    critic_loss = torch.mean(errors)

    # Add l2-regularization if required.
    if l2_reg is not None:
        for name, var in model.policy_variables(as_dict=True).items():
            if "bias" not in name:
                actor_loss += l2_reg * l2_loss(var)
        for name, var in model.q_variables(as_dict=True).items():
            if "bias" not in name:
                critic_loss += l2_reg * l2_loss(var)

    # Store values for stats function.
    policy.actor_loss = actor_loss
    policy.critic_loss = critic_loss
    policy.td_error = td_error
    policy.q_t = q_t

    # Return two loss terms (corresponding to the two optimizers, we create).
    return policy.actor_loss, policy.critic_loss


def build_maddpg_stats(policy: Policy, batch: SampleBatch) -> Dict[str, TensorType]:
    stats = {
        "actor_loss": policy.actor_loss,
        "critic_loss": policy.critic_loss,
        "mean_q": torch.mean(policy.q_t),
        "max_q": torch.max(policy.q_t),
        "min_q": torch.min(policy.q_t),
        "mean_td_error": torch.mean(policy.td_error),
        "td_error": policy.td_error,
    }
    return stats


def postprocess_nstep(
    policy: Policy, batch: SampleBatch, other_agent_batches=None, episode=None
):
    # N-step Q adjustments
    if policy.config["n_step"] > 1:
        adjust_nstep(
            policy.config["n_step"],
            policy.config["gamma"],
            batch[SampleBatch.CUR_OBS],
            batch[SampleBatch.ACTIONS],
            batch[SampleBatch.REWARDS],
            batch[SampleBatch.NEXT_OBS],
            batch[SampleBatch.DONES],
        )

    return batch


def make_maddpg_optimizers(
    policy: Policy, config: TrainerConfigDict
) -> Tuple[LocalOptimizer]:
    return make_ddpg_optimizers(policy, config)


def before_init_fn(
    policy: Policy, obs_space, action_space, config: TrainerConfigDict
) -> None:
    policy.global_step = 0
    # Check agent_id
    agent_id = config["agent_id"]
    if agent_id is None:
        raise ValueError("Must set `agent_id` in the policy config.")
    if type(agent_id) is not int:
        raise ValueError("Agent ids must be integers for MADDPG.")


class ComputeTDErrorMixin:
    def __init__(self, loss_fn):
        def compute_td_error(obs_t, act_t, rew_t, obs_tp1, done_mask):
            input_dict = self._lazy_tensor_dict(
                SampleBatch(
                    {
                        SampleBatch.CUR_OBS: obs_t,
                        SampleBatch.ACTIONS: act_t,
                        SampleBatch.REWARDS: rew_t,
                        SampleBatch.NEXT_OBS: obs_tp1,
                        SampleBatch.DONES: done_mask,
                    }
                )
            )
            # Do forward pass on loss to update td errors attribute
            loss_fn(self, self.model, None, input_dict)

            # Self.td_error is set within actor_critic_loss call.
            return self.td_error

        self.compute_td_error = compute_td_error


class SetJointSpacesMixin:
    def __init__(self, config: TrainerConfigDict):
        self.obs_space_n = [
            _make_continuous_space(space)
            for _, (_, space, _, _) in config["multiagent"]["policies"].items()
        ]
        self.act_space_n = [
            _make_continuous_space(space)
            for _, (_, _, space, _) in config["multiagent"]["policies"].items()
        ]


def setup_late_mixins(
    policy: Policy, obs_space, action_space, config: TrainerConfigDict
) -> None:
    ComputeTDErrorMixin.__init__(policy, maddpg_actor_critic_loss)
    TargetNetworkMixin.__init__(policy)
    SetJointSpacesMixin.__init__(policy, config)


def get_default_config():
    # Hacky workaround to fix imports
    import maddpg

    return maddpg.DEFAULT_CONFIG


MADDPGTorchPolicy = build_policy_class(
    name="MADDPGTorchPolicy",
    framework="torch",
    loss_fn=maddpg_actor_critic_loss,
    get_default_config=get_default_config,
    stats_fn=build_maddpg_stats,
    postprocess_fn=postprocess_nstep,
    action_distribution_fn=get_distribution_inputs_and_class,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=make_maddpg_optimizers,
    validate_spaces=validate_spaces,
    before_init=before_init_fn,
    before_loss_init=setup_late_mixins,
    make_model_and_action_dist=build_maddpg_models_and_action_dist,
    apply_gradients_fn=apply_gradients_fn,
    mixins=[TargetNetworkMixin, ComputeTDErrorMixin, SetJointSpacesMixin],
)
