from dataclasses import dataclass
import numpy as np
from typing import Any, List, Mapping, Union
import tree

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
import ray.rllib.algorithms.impala.vtrace_torch as vtrace
from ray.rllib.core.learner.learner import LearnerHPs
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    ALL_MODULES,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ResultDict, TensorType

torch, nn = try_import_torch()


def make_time_major(
    tensor: Union["torch.Tensor", List["torch.Tensor"]],
    *,
    trajectory_len: int = None,
    recurrent_seq_len: int = None,
    drop_last: bool = False,
):
    """Swaps batch and trajectory axis.

    Args:
        tensor: A tensor or list of tensors to swap the axis of.
            NOTE: Each tensor must have the shape [B * T] where B is the batch size and
            T is the trajectory length.
        trajectory_len: The length of each trajectory being transformed.
            If None then `recurrent_seq_len` must be set.
        recurrent_seq_len: Sequence lengths if recurrent.
            If None then `trajectory_len` must be set.
        drop_last: A bool indicating whether to drop the last
            trajectory item.

    Returns:
        res: A tensor with swapped axes or a list of tensors with
        swapped axes.
    """
    if isinstance(tensor, (list, tuple)):
        return [
            make_time_major(_tensor, trajectory_len, recurrent_seq_len, drop_last)
            for _tensor in tensor
        ]

    assert (trajectory_len != recurrent_seq_len) and (
        trajectory_len is None or recurrent_seq_len is None
    ), "Either trajectory_len or recurrent_seq_len must be set."

    if recurrent_seq_len:
        B = recurrent_seq_len.shape[0]
        T = tensor.shape[0] // B
    else:
        # Important: chop the tensor into batches at known episode cut
        # boundaries.
        # TODO: (sven) this is kind of a hack and won't work for
        #  batch_mode=complete_episodes.
        T = trajectory_len
        B = tensor.shape[0] // T
    rs = torch.reshape(tensor, [B, T] + list(tensor.shape[1:]))

    # Swap B and T axes.
    res = torch.transpose(rs, 1, 0)

    if drop_last:
        return res[:-1]
    return res


@dataclass
class ImpalaHPs(LearnerHPs):
    """Hyper-parameters for IMPALA.

    Attributes:
        rollout_frag_or_episode_len: The length of a rollout fragment or episode.
            Used when making SampleBatches time major for computing loss.
        recurrent_seq_len: The length of a recurrent sequence. Used when making
            SampleBatches time major for computing loss.
        discount_factor: The discount factor to use for computing returns.
        vtrace_clip_rho_threshold: The rho threshold to use for clipping the
            importance weights.
        vtrace_clip_pg_rho_threshold: The rho threshold to use for clipping the
            importance weights when computing the policy_gradient loss.
        vtrace_drop_last_ts: Whether to drop the last timestep when computing the loss.
            This is useful for stabilizing the loss.
            NOTE: This shouldn't be True when training on environments where the rewards
            come at the end of the episode.
        vf_loss_coeff: The amount to weight the value function loss by when computing
            the total loss.
        entropy_coeff: The amount to weight the average entropy of the actions in the
            SampleBatch towards the total_loss for module updates. The higher this
            coefficient, the more that the policy network will be encouraged to output
            distributions with higher entropy/std deviation, which will encourage
            greater exploration.

    """

    rollout_frag_or_episode_len: int = None
    recurrent_seq_len: int = None
    discount_factor: float = 0.99
    vtrace_clip_rho_threshold: float = 1.0
    vtrace_clip_pg_rho_threshold: float = 1.0
    vtrace_drop_last_ts: bool = True
    vf_loss_coeff: float = 0.5
    entropy_coeff: float = 0.01


class ImpalaTLearner(TorchLearner):
    """Implements IMPALA loss / update logic on top of TorchLearner.

    This class implements the IMPALA loss under `_compute_loss_per_module()`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.vtrace_clip_rho_threshold = self._hps.vtrace_clip_rho_threshold
        self.vtrace_clip_pg_rho_threshold = self._hps.vtrace_clip_pg_rho_threshold
        self.vtrace_drop_last_ts = self._hps.vtrace_drop_last_ts
        self.vf_loss_coeff = self._hps.vf_loss_coeff
        self.entropy_coeff = self._hps.entropy_coeff
        self.rollout_frag_or_episode_len = self._hps.rollout_frag_or_episode_len
        self.recurrent_seq_len = self._hps.recurrent_seq_len
        self.discount_factor = self._hps.discount_factor
        assert (
            self.rollout_frag_or_episode_len is not None
            or self.recurrent_seq_len is not None
        ) and not (self.rollout_frag_or_episode_len and self.recurrent_seq_len), (
            "Either rollout_frag_or_episode_len or recurrent_seq_len"
            " must be set in the IMPALA HParams. "
        )

    @override(TorchLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        values = fwd_out[SampleBatch.VF_PREDS]
        target_policy_dist = fwd_out[SampleBatch.ACTION_DIST]
        target_logits = fwd_out[SampleBatch.ACTION_DIST_INPUTS]

        actions = batch[SampleBatch.ACTIONS]
        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        behaviour_logits = batch[SampleBatch.ACTION_DIST_INPUTS]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )
        bootstrap_value = values_time_major[-1]
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=self.rollout_frag_or_episode_len,
            recurrent_seq_len=self.recurrent_seq_len,
            drop_last=self.vtrace_drop_last_ts,
        )

        # the discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - make_time_major(
                batch[SampleBatch.TERMINATEDS],
                trajectory_len=self.rollout_frag_or_episode_len,
                recurrent_seq_len=self.recurrent_seq_len,
                drop_last=self.vtrace_drop_last_ts,
            ).type(dtype=torch.float32)
        ) * self.discount_factor

        def _make_time_major(*args, **kw):
            return make_time_major(self, batch.get(SampleBatch.SEQ_LENS), *args, **kw)

        drop_last = self.config["vtrace_drop_last_ts"]

        # In the old impala code, actions needed to be unsqueezed if they were
        # multi_discrete
        # actions = actions if is_multidiscrete else torch.unsqueeze(actions, dim=1)

        actions = _make_time_major(actions, drop_last=drop_last)
        actions_logp = _make_time_major(target_actions_logp, drop_last=drop_last)
        dones = _make_time_major(batch[SampleBatch.DONES], drop_last=drop_last)
        behaviour_logits_time_major = _make_time_major(
            behaviour_logits, drop_last=drop_last
        )
        target_logits_time_major = _make_time_major(target_logits, drop_last=drop_last)
        discount = discounts_time_major
        values = _make_time_major(values, drop_last=drop_last)
        dist_class = (
            target_policy_dist  # TorchCategorical if is_multidiscrete else dist_class,
        )
        model = self.module
        clip_rho_threshold = self.vtrace_clip_rho_threshold
        clip_pg_rho_threshold = self.vtrace_clip_pg_rho_threshold

        # Compute vtrace on the CPU for better perf
        # (devices handled inside `vtrace.multi_from_logits`).
        device = behaviour_actions_logp_time_major[0].device
        vtrace_returns = vtrace.multi_from_logits(
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            behaviour_policy_logits=behaviour_logits_time_major,
            target_policy_logits=target_logits_time_major,
            actions=torch.unbind(actions, dim=2),
            discounts=(1.0 - dones.float()) * discount,
            rewards=rewards_time_major,
            values=values,
            bootstrap_value=bootstrap_value,
            dist_class=dist_class,
            model=model,
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold,
        )
        # Move v-trace results back to GPU for actual loss computing.
        value_targets = vtrace_returns.vs.to(device)

        pg_advantages = vtrace_returns.pg_advantages.to(device)

        # The policy gradients loss.
        pi_loss = -torch.sum(actions_logp * pg_advantages)

        # The baseline loss.
        delta = values - value_targets
        vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0))

        batch_size = target_actions_logp_time_major.shape[-1].type(torch.float32)

        # The policy gradients loss.
        mean_pi_loss = pi_loss / batch_size

        # The baseline loss.
        mean_vf_loss = vf_loss / batch_size

        # The entropy loss.
        # or use actions_entropy
        entropy_loss = -torch.sum(target_actions_logp_time_major)

        # The summed weighted loss.
        total_loss = (
            pi_loss + vf_loss * self.vf_loss_coeff + entropy_loss * self.entropy_coeff
        )
        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "pi_loss": mean_pi_loss,
            "vf_loss": mean_vf_loss,
        }

    @override(TorchLearner)
    def compile_results(
        self,
        batch: MultiAgentBatch,
        fwd_out: Mapping[str, Any],
        postprocessed_loss: Mapping[str, Any],
        postprocessed_gradients: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        results = super().compile_results(
            batch, fwd_out, postprocessed_loss, postprocessed_gradients
        )
        results[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] = batch.agent_steps()
        results[ALL_MODULES][NUM_ENV_STEPS_TRAINED] = batch.env_steps()
        return results


def _reduce_impala_results(results: List[ResultDict]) -> ResultDict:
    """Reduce/Aggregate a list of results from Impala Learners.

    Average the values of the result dicts. Add keys for the number of agent and env
    steps trained.

    Args:
        results: result dicts to reduce.

    Returns:
        A reduced result dict.
    """
    result = tree.map_structure(lambda *x: np.mean(x), *results)
    agent_steps_trained = sum(
        [r[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] for r in results]
    )
    env_steps_trained = sum([r[ALL_MODULES][NUM_ENV_STEPS_TRAINED] for r in results])
    result[ALL_MODULES][NUM_AGENT_STEPS_TRAINED] = agent_steps_trained
    result[ALL_MODULES][NUM_ENV_STEPS_TRAINED] = env_steps_trained
    return result
