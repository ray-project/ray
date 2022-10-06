import logging
import gym
from typing import Dict, Tuple, List, Optional, Any, Type

import ray
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    postprocess_nstep_and_prio,
    PRIO_WEIGHTS,
)
from ray.rllib.evaluation import Episode
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_action_dist import (
    TorchDeterministic,
    TorchDirichlet,
    TorchDistributionWrapper,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import TargetNetworkMixin
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    concat_multi_gpu_td_errors,
    huber_loss,
    l2_loss,
)
from ray.rllib.utils.typing import (
    ModelGradients,
    TensorType,
    AlgorithmConfigDict,
)
from ray.rllib.algorithms.ddpg.utils import make_ddpg_models, validate_spaces

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class ComputeTDErrorMixin:
    def __init__(self: TorchPolicyV2):
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
            self.loss(self.model, None, input_dict)

            # `self.model.td_error` is set within actor_critic_loss call.
            return self.model.tower_stats["td_error"]

        self.compute_td_error = compute_td_error


class DDPGTorchPolicy(TargetNetworkMixin, ComputeTDErrorMixin, TorchPolicyV2):
    def __init__(
        self,
        observation_space: gym.spaces.Space,
        action_space: gym.spaces.Space,
        config: AlgorithmConfigDict,
    ):
        config = dict(ray.rllib.algorithms.ddpg.ddpg.DDPGConfig().to_dict(), **config)

        # Create global step for counting the number of update operations.
        self.global_step = 0

        # Validate action space for DDPG
        validate_spaces(self, observation_space, action_space)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        ComputeTDErrorMixin.__init__(self)

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

        TargetNetworkMixin.__init__(self)

    @override(TorchPolicyV2)
    def make_model_and_action_dist(
        self,
    ) -> Tuple[ModelV2, Type[TorchDistributionWrapper]]:
        model = make_ddpg_models(self)
        if isinstance(self.action_space, Simplex):
            distr_class = TorchDirichlet
        else:
            distr_class = TorchDeterministic
        return model, distr_class

    @override(TorchPolicyV2)
    def optimizer(
        self,
    ) -> List["torch.optim.Optimizer"]:
        """Create separate optimizers for actor & critic losses."""

        # Set epsilons to match tf.keras.optimizers.Adam's epsilon default.
        self._actor_optimizer = torch.optim.Adam(
            params=self.model.policy_variables(), lr=self.config["actor_lr"], eps=1e-7
        )

        self._critic_optimizer = torch.optim.Adam(
            params=self.model.q_variables(), lr=self.config["critic_lr"], eps=1e-7
        )

        # Return them in the same order as the respective loss terms are returned.
        return [self._actor_optimizer, self._critic_optimizer]

    @override(TorchPolicyV2)
    def apply_gradients(self, gradients: ModelGradients) -> None:
        # For policy gradient, update policy net one time v.s.
        # update critic net `policy_delay` time(s).
        if self.global_step % self.config["policy_delay"] == 0:
            self._actor_optimizer.step()

        self._critic_optimizer.step()

        # Increment global step & apply ops.
        self.global_step += 1

    @override(TorchPolicyV2)
    def action_distribution_fn(
        self,
        model: ModelV2,
        *,
        obs_batch: TensorType,
        state_batches: TensorType,
        is_training: bool = False,
        **kwargs
    ) -> Tuple[TensorType, type, List[TensorType]]:
        model_out, _ = model(
            SampleBatch(obs=obs_batch[SampleBatch.CUR_OBS], _is_training=is_training)
        )
        dist_inputs = model.get_policy_output(model_out)

        if isinstance(self.action_space, Simplex):
            distr_class = TorchDirichlet
        else:
            distr_class = TorchDeterministic
        return dist_inputs, distr_class, []  # []=state out

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self,
        sample_batch: SampleBatch,
        other_agent_batches: Optional[Dict[Any, SampleBatch]] = None,
        episode: Optional[Episode] = None,
    ) -> SampleBatch:
        return postprocess_nstep_and_prio(
            self, sample_batch, other_agent_batches, episode
        )

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[TorchDistributionWrapper],
        train_batch: SampleBatch,
    ) -> List[TensorType]:
        target_model = self.target_models[model]

        twin_q = self.config["twin_q"]
        gamma = self.config["gamma"]
        n_step = self.config["n_step"]
        use_huber = self.config["use_huber"]
        huber_threshold = self.config["huber_threshold"]
        l2_reg = self.config["l2_reg"]

        input_dict = SampleBatch(
            obs=train_batch[SampleBatch.CUR_OBS], _is_training=True
        )
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
        if self.config["smooth_target_policy"]:
            target_noise_clip = self.config["target_noise_clip"]
            clipped_normal_sample = torch.clamp(
                torch.normal(
                    mean=torch.zeros(policy_tp1.size()), std=self.config["target_noise"]
                ).to(policy_tp1.device),
                -target_noise_clip,
                target_noise_clip,
            )

            policy_tp1_smoothed = torch.min(
                torch.max(
                    policy_tp1 + clipped_normal_sample,
                    torch.tensor(
                        self.action_space.low,
                        dtype=torch.float32,
                        device=policy_tp1.device,
                    ),
                ),
                torch.tensor(
                    self.action_space.high,
                    dtype=torch.float32,
                    device=policy_tp1.device,
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
                errors = 0.5 * (
                    torch.pow(td_error, 2.0) + torch.pow(twin_td_error, 2.0)
                )
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
        if self.config["use_state_preprocessor"]:
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
        return [actor_loss, critic_loss]

    @override(TorchPolicyV2)
    def extra_grad_process(
        self, optimizer: torch.optim.Optimizer, loss: TensorType
    ) -> Dict[str, TensorType]:
        # Clip grads if configured.
        return apply_grad_clipping(self, optimizer, loss)

    @override(TorchPolicyV2)
    def extra_compute_grad_fetches(self) -> Dict[str, Any]:
        fetches = convert_to_numpy(concat_multi_gpu_td_errors(self))
        return dict({LEARNER_STATS_KEY: {}}, **fetches)

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        q_t = torch.stack(self.get_tower_stats("q_t"))
        stats = {
            "actor_loss": torch.mean(torch.stack(self.get_tower_stats("actor_loss"))),
            "critic_loss": torch.mean(torch.stack(self.get_tower_stats("critic_loss"))),
            "mean_q": torch.mean(q_t),
            "max_q": torch.max(q_t),
            "min_q": torch.min(q_t),
        }
        return convert_to_numpy(stats)
