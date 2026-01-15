"""
PyTorch policy class used for APPO.

Adapted from VTraceTFPolicy to use the PPO surrogate loss.
Keep in sync with changes to VTraceTFPolicy.
"""

import logging
from typing import Any, Dict, List, Optional, Type, Union

import gymnasium as gym
import numpy as np

import ray
import ray.rllib.algorithms.impala.vtrace_torch as vtrace
from ray.rllib.algorithms.appo.utils import make_appo_models
from ray.rllib.algorithms.impala.impala_torch_policy import (
    VTraceOptimizer,
    make_time_major,
)
from ray.rllib.evaluation.postprocessing import (
    Postprocessing,
    compute_bootstrap_value,
    compute_gae_for_sample_batch,
)
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDistributionWrapper,
)
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import (
    EntropyCoeffSchedule,
    KLCoeffMixin,
    LearningRateSchedule,
    TargetNetworkMixin,
    ValueNetworkMixin,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    explained_variance,
    global_norm,
    sequence_mask,
)
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


# TODO (sven): Deprecate once APPO and IMPALA fully on RLModules/Learner APIs.
class APPOTorchPolicy(
    VTraceOptimizer,
    LearningRateSchedule,
    EntropyCoeffSchedule,
    KLCoeffMixin,
    ValueNetworkMixin,
    TargetNetworkMixin,
    TorchPolicyV2,
):
    """PyTorch policy class used with APPO."""

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.algorithms.appo.appo.APPOConfig().to_dict(), **config)
        config["enable_rl_module_and_learner"] = False
        config["enable_env_runner_and_connector_v2"] = False

        # Although this is a no-op, we call __init__ here to make it clear
        # that base.__init__ will use the make_model() call.
        VTraceOptimizer.__init__(self)

        lr_schedule_additional_args = []
        if config.get("_separate_vf_optimizer"):
            lr_schedule_additional_args = (
                [config["_lr_vf"][0][1], config["_lr_vf"]]
                if isinstance(config["_lr_vf"], (list, tuple))
                else [config["_lr_vf"], None]
            )
        LearningRateSchedule.__init__(
            self, config["lr"], config["lr_schedule"], *lr_schedule_additional_args
        )

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )
        ValueNetworkMixin.__init__(self, config)
        KLCoeffMixin.__init__(self, config)

        self._initialize_loss_from_dummy_batch()

        # Initiate TargetNetwork ops after loss initialization.
        TargetNetworkMixin.__init__(self)

    @override(TorchPolicyV2)
    def init_view_requirements(self):
        self.view_requirements = self._get_default_view_requirements()

    @override(TorchPolicyV2)
    def make_model(self) -> ModelV2:
        return make_appo_models(self)

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[ActionDistribution],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Constructs the loss for APPO.

        With IS modifications and V-trace for Advantage Estimation.

        Args:
            model (ModelV2): The Model to calculate the loss for.
            dist_class (Type[ActionDistribution]): The action distr. class.
            train_batch: The training data.

        Returns:
            Union[TensorType, List[TensorType]]: A single loss tensor or a list
                of loss tensors.
        """
        target_model = self.target_models[model]

        model_out, _ = model(train_batch)
        action_dist = dist_class(model_out, model)

        if isinstance(self.action_space, gym.spaces.Discrete):
            is_multidiscrete = False
            output_hidden_shape = [self.action_space.n]
        elif isinstance(self.action_space, gym.spaces.multi_discrete.MultiDiscrete):
            is_multidiscrete = True
            output_hidden_shape = self.action_space.nvec.astype(np.int32)
        else:
            is_multidiscrete = False
            output_hidden_shape = 1

        def _make_time_major(*args, **kwargs):
            return make_time_major(
                self, train_batch.get(SampleBatch.SEQ_LENS), *args, **kwargs
            )

        actions = train_batch[SampleBatch.ACTIONS]
        dones = train_batch[SampleBatch.TERMINATEDS]
        rewards = train_batch[SampleBatch.REWARDS]
        behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]

        target_model_out, _ = target_model(train_batch)

        prev_action_dist = dist_class(behaviour_logits, model)
        values = model.value_function()
        values_time_major = _make_time_major(values)
        bootstrap_values_time_major = _make_time_major(
            train_batch[SampleBatch.VALUES_BOOTSTRAPPED]
        )
        bootstrap_value = bootstrap_values_time_major[-1]

        if self.is_recurrent():
            max_seq_len = torch.max(train_batch[SampleBatch.SEQ_LENS])
            mask = sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
            mask = torch.reshape(mask, [-1])
            mask = _make_time_major(mask)
            num_valid = torch.sum(mask)

            def reduce_mean_valid(t):
                return torch.sum(t[mask]) / num_valid

        else:
            reduce_mean_valid = torch.mean

        if self.config["vtrace"]:
            logger.debug("Using V-Trace surrogate loss (vtrace=True)")

            old_policy_behaviour_logits = target_model_out.detach()
            old_policy_action_dist = dist_class(old_policy_behaviour_logits, model)

            if isinstance(output_hidden_shape, (list, tuple, np.ndarray)):
                unpacked_behaviour_logits = torch.split(
                    behaviour_logits, list(output_hidden_shape), dim=1
                )
                unpacked_old_policy_behaviour_logits = torch.split(
                    old_policy_behaviour_logits, list(output_hidden_shape), dim=1
                )
            else:
                unpacked_behaviour_logits = torch.chunk(
                    behaviour_logits, output_hidden_shape, dim=1
                )
                unpacked_old_policy_behaviour_logits = torch.chunk(
                    old_policy_behaviour_logits, output_hidden_shape, dim=1
                )

            # Prepare actions for loss.
            loss_actions = (
                actions if is_multidiscrete else torch.unsqueeze(actions, dim=1)
            )

            # Prepare KL for loss.
            action_kl = _make_time_major(old_policy_action_dist.kl(action_dist))

            # Compute vtrace on the CPU for better perf.
            vtrace_returns = vtrace.multi_from_logits(
                behaviour_policy_logits=_make_time_major(unpacked_behaviour_logits),
                target_policy_logits=_make_time_major(
                    unpacked_old_policy_behaviour_logits
                ),
                actions=torch.unbind(_make_time_major(loss_actions), dim=2),
                discounts=(1.0 - _make_time_major(dones).float())
                * self.config["gamma"],
                rewards=_make_time_major(rewards),
                values=values_time_major,
                bootstrap_value=bootstrap_value,
                dist_class=TorchCategorical if is_multidiscrete else dist_class,
                model=model,
                clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
                clip_pg_rho_threshold=self.config["vtrace_clip_pg_rho_threshold"],
            )

            actions_logp = _make_time_major(action_dist.logp(actions))
            prev_actions_logp = _make_time_major(prev_action_dist.logp(actions))
            old_policy_actions_logp = _make_time_major(
                old_policy_action_dist.logp(actions)
            )
            is_ratio = torch.clamp(
                torch.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0
            )
            logp_ratio = is_ratio * torch.exp(actions_logp - prev_actions_logp)
            self._is_ratio = is_ratio

            advantages = vtrace_returns.pg_advantages.to(logp_ratio.device)
            surrogate_loss = torch.min(
                advantages * logp_ratio,
                advantages
                * torch.clamp(
                    logp_ratio,
                    1 - self.config["clip_param"],
                    1 + self.config["clip_param"],
                ),
            )

            mean_kl_loss = reduce_mean_valid(action_kl)
            mean_policy_loss = -reduce_mean_valid(surrogate_loss)

            # The value function loss.
            value_targets = vtrace_returns.vs.to(values_time_major.device)
            delta = values_time_major - value_targets
            mean_vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

            # The entropy loss.
            mean_entropy = reduce_mean_valid(_make_time_major(action_dist.entropy()))

        else:
            logger.debug("Using PPO surrogate loss (vtrace=False)")

            # Prepare KL for Loss
            action_kl = _make_time_major(prev_action_dist.kl(action_dist))

            actions_logp = _make_time_major(action_dist.logp(actions))
            prev_actions_logp = _make_time_major(prev_action_dist.logp(actions))
            logp_ratio = torch.exp(actions_logp - prev_actions_logp)

            advantages = _make_time_major(train_batch[Postprocessing.ADVANTAGES])
            surrogate_loss = torch.min(
                advantages * logp_ratio,
                advantages
                * torch.clamp(
                    logp_ratio,
                    1 - self.config["clip_param"],
                    1 + self.config["clip_param"],
                ),
            )

            mean_kl_loss = reduce_mean_valid(action_kl)
            mean_policy_loss = -reduce_mean_valid(surrogate_loss)

            # The value function loss.
            value_targets = _make_time_major(train_batch[Postprocessing.VALUE_TARGETS])
            delta = values_time_major - value_targets
            mean_vf_loss = 0.5 * reduce_mean_valid(torch.pow(delta, 2.0))

            # The entropy loss.
            mean_entropy = reduce_mean_valid(_make_time_major(action_dist.entropy()))

        # The summed weighted loss.
        total_loss = mean_policy_loss - mean_entropy * self.entropy_coeff
        # Optional additional KL Loss
        if self.config["use_kl_loss"]:
            total_loss += self.kl_coeff * mean_kl_loss

        # Optional vf loss (or in a separate term due to separate
        # optimizers/networks).
        loss_wo_vf = total_loss
        if not self.config["_separate_vf_optimizer"]:
            total_loss += mean_vf_loss * self.config["vf_loss_coeff"]

        # Store values for stats function in model (tower), such that for
        # multi-GPU, we do not override them during the parallel loss phase.
        model.tower_stats["total_loss"] = total_loss
        model.tower_stats["mean_policy_loss"] = mean_policy_loss
        model.tower_stats["mean_kl_loss"] = mean_kl_loss
        model.tower_stats["mean_vf_loss"] = mean_vf_loss
        model.tower_stats["mean_entropy"] = mean_entropy
        model.tower_stats["value_targets"] = value_targets
        model.tower_stats["vf_explained_var"] = explained_variance(
            torch.reshape(value_targets, [-1]),
            torch.reshape(values_time_major, [-1]),
        )

        # Return one total loss or two losses: vf vs rest (policy + kl).
        if self.config["_separate_vf_optimizer"]:
            return loss_wo_vf, mean_vf_loss
        else:
            return total_loss

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        """Stats function for APPO. Returns a dict with important loss stats.

        Args:
            policy: The Policy to generate stats for.
            train_batch: The SampleBatch (already) used for training.

        Returns:
            Dict[str, TensorType]: The stats dict.
        """
        stats_dict = {
            "cur_lr": self.cur_lr,
            "total_loss": torch.mean(torch.stack(self.get_tower_stats("total_loss"))),
            "policy_loss": torch.mean(
                torch.stack(self.get_tower_stats("mean_policy_loss"))
            ),
            "entropy": torch.mean(torch.stack(self.get_tower_stats("mean_entropy"))),
            "entropy_coeff": self.entropy_coeff,
            "var_gnorm": global_norm(self.model.trainable_variables()),
            "vf_loss": torch.mean(torch.stack(self.get_tower_stats("mean_vf_loss"))),
            "vf_explained_var": torch.mean(
                torch.stack(self.get_tower_stats("vf_explained_var"))
            ),
        }

        if self.config["vtrace"]:
            is_stat_mean = torch.mean(self._is_ratio, [0, 1])
            is_stat_var = torch.var(self._is_ratio, [0, 1])
            stats_dict["mean_IS"] = is_stat_mean
            stats_dict["var_IS"] = is_stat_var

        if self.config["use_kl_loss"]:
            stats_dict["kl"] = torch.mean(
                torch.stack(self.get_tower_stats("mean_kl_loss"))
            )
            stats_dict["KL_Coeff"] = self.kl_coeff

        return convert_to_numpy(stats_dict)

    @override(TorchPolicyV2)
    def extra_action_out(
        self,
        input_dict: Dict[str, TensorType],
        state_batches: List[TensorType],
        model: TorchModelV2,
        action_dist: TorchDistributionWrapper,
    ) -> Dict[str, TensorType]:
        return {SampleBatch.VF_PREDS: model.value_function()}

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[Dict[Any, SampleBatch]] = None,
        episode=None,
    ):
        # Call super's postprocess_trajectory first.
        # sample_batch = super().postprocess_trajectory(
        #    sample_batch, other_agent_batches, episode
        # )

        # Do all post-processing always with no_grad().
        # Not using this here will introduce a memory leak
        # in torch (issue #6962).
        with torch.no_grad():
            if not self.config["vtrace"]:
                sample_batch = compute_gae_for_sample_batch(
                    self, sample_batch, other_agent_batches, episode
                )
            else:
                # Add the SampleBatch.VALUES_BOOTSTRAPPED column, which we'll need
                # inside the loss for vtrace calculations.
                sample_batch = compute_bootstrap_value(sample_batch, self)

        return sample_batch

    @override(TorchPolicyV2)
    def extra_grad_process(
        self, optimizer: "torch.optim.Optimizer", loss: TensorType
    ) -> Dict[str, TensorType]:
        return apply_grad_clipping(self, optimizer, loss)

    @override(TorchPolicyV2)
    def get_batch_divisibility_req(self) -> int:
        return self.config["rollout_fragment_length"]
