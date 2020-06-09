import gym
import logging
import numpy as np

import ray
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.agents.impala.vtrace_tf_policy import postprocess_trajectory
import ray.rllib.agents.impala.vtrace_torch as vtrace
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import LearningRateSchedule, \
    EntropyCoeffSchedule
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_ops import global_norm, sequence_mask

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class VTraceLoss:
    def __init__(self,
                 actions,
                 actions_logp,
                 actions_entropy,
                 dones,
                 behaviour_action_logp,
                 behaviour_logits,
                 target_logits,
                 discount,
                 rewards,
                 values,
                 bootstrap_value,
                 dist_class,
                 model,
                 valid_mask,
                 config,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01,
                 clip_rho_threshold=1.0,
                 clip_pg_rho_threshold=1.0):
        """Policy gradient loss with vtrace importance weighting.

        VTraceLoss takes tensors of shape [T, B, ...], where `B` is the
        batch_size. The reason we need to know `B` is for V-trace to properly
        handle episode cut boundaries.

        Args:
            actions: An int|float32 tensor of shape [T, B, ACTION_SPACE].
            actions_logp: A float32 tensor of shape [T, B].
            actions_entropy: A float32 tensor of shape [T, B].
            dones: A bool tensor of shape [T, B].
            behaviour_action_logp: Tensor of shape [T, B].
            behaviour_logits: A list with length of ACTION_SPACE of float32
                tensors of shapes
                [T, B, ACTION_SPACE[0]],
                ...,
                [T, B, ACTION_SPACE[-1]]
            target_logits: A list with length of ACTION_SPACE of float32
                tensors of shapes
                [T, B, ACTION_SPACE[0]],
                ...,
                [T, B, ACTION_SPACE[-1]]
            discount: A float32 scalar.
            rewards: A float32 tensor of shape [T, B].
            values: A float32 tensor of shape [T, B].
            bootstrap_value: A float32 tensor of shape [B].
            dist_class: action distribution class for logits.
            valid_mask: A bool tensor of valid RNN input elements (#2992).
            config: Trainer config dict.
        """

        if valid_mask is None:
            valid_mask = torch.ones_like(actions_logp)

        # Compute vtrace on the CPU for better perf
        # (devices handled inside `vtrace.multi_from_logits`).
        device = behaviour_action_logp[0].device
        self.vtrace_returns = vtrace.multi_from_logits(
            behaviour_action_log_probs=behaviour_action_logp,
            behaviour_policy_logits=behaviour_logits,
            target_policy_logits=target_logits,
            actions=torch.unbind(actions, dim=2),
            discounts=(1.0 - dones.float()) * discount,
            rewards=rewards,
            values=values,
            bootstrap_value=bootstrap_value,
            dist_class=dist_class,
            model=model,
            clip_rho_threshold=clip_rho_threshold,
            clip_pg_rho_threshold=clip_pg_rho_threshold)
        # Move v-trace results back to GPU for actual loss computing.
        self.value_targets = self.vtrace_returns.vs.to(device)

        # The policy gradients loss.
        self.pi_loss = -torch.sum(
            actions_logp * self.vtrace_returns.pg_advantages.to(device) *
            valid_mask)

        # The baseline loss.
        delta = (values - self.value_targets) * valid_mask
        self.vf_loss = 0.5 * torch.sum(torch.pow(delta, 2.0))

        # The entropy loss.
        self.entropy = torch.sum(actions_entropy * valid_mask)

        # The summed weighted loss.
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)


def build_vtrace_loss(policy, model, dist_class, train_batch):
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)

    if isinstance(policy.action_space, gym.spaces.Discrete):
        is_multidiscrete = False
        output_hidden_shape = [policy.action_space.n]
    elif isinstance(policy.action_space,
                    gym.spaces.multi_discrete.MultiDiscrete):
        is_multidiscrete = True
        output_hidden_shape = policy.action_space.nvec.astype(np.int32)
    else:
        is_multidiscrete = False
        output_hidden_shape = 1

    def _make_time_major(*args, **kw):
        return make_time_major(policy, train_batch.get("seq_lens"), *args,
                               **kw)

    actions = train_batch[SampleBatch.ACTIONS]
    dones = train_batch[SampleBatch.DONES]
    rewards = train_batch[SampleBatch.REWARDS]
    behaviour_action_logp = train_batch[SampleBatch.ACTION_LOGP]
    behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]
    if isinstance(output_hidden_shape, (list, tuple, np.ndarray)):
        unpacked_behaviour_logits = torch.split(
            behaviour_logits, list(output_hidden_shape), dim=1)
        unpacked_outputs = torch.split(
            model_out, list(output_hidden_shape), dim=1)
    else:
        unpacked_behaviour_logits = torch.chunk(
            behaviour_logits, output_hidden_shape, dim=1)
        unpacked_outputs = torch.chunk(model_out, output_hidden_shape, dim=1)
    values = model.value_function()

    if policy.is_recurrent():
        max_seq_len = torch.max(train_batch["seq_lens"])
        mask_orig = sequence_mask(train_batch["seq_lens"], max_seq_len)
        mask = torch.reshape(mask_orig, [-1])
    else:
        mask = torch.ones_like(rewards)

    # Prepare actions for loss.
    loss_actions = actions if is_multidiscrete else torch.unsqueeze(
        actions, dim=1)

    # Inputs are reshaped from [B * T] => [T - 1, B] for V-trace calc.
    policy.loss = VTraceLoss(
        actions=_make_time_major(loss_actions, drop_last=True),
        actions_logp=_make_time_major(
            action_dist.logp(actions), drop_last=True),
        actions_entropy=_make_time_major(
            action_dist.entropy(), drop_last=True),
        dones=_make_time_major(dones, drop_last=True),
        behaviour_action_logp=_make_time_major(
            behaviour_action_logp, drop_last=True),
        behaviour_logits=_make_time_major(
            unpacked_behaviour_logits, drop_last=True),
        target_logits=_make_time_major(unpacked_outputs, drop_last=True),
        discount=policy.config["gamma"],
        rewards=_make_time_major(rewards, drop_last=True),
        values=_make_time_major(values, drop_last=True),
        bootstrap_value=_make_time_major(values)[-1],
        dist_class=TorchCategorical if is_multidiscrete else dist_class,
        model=model,
        valid_mask=_make_time_major(mask, drop_last=True),
        config=policy.config,
        vf_loss_coeff=policy.config["vf_loss_coeff"],
        entropy_coeff=policy.entropy_coeff,
        clip_rho_threshold=policy.config["vtrace_clip_rho_threshold"],
        clip_pg_rho_threshold=policy.config["vtrace_clip_pg_rho_threshold"])

    return policy.loss.total_loss


def make_time_major(policy, seq_lens, tensor, drop_last=False):
    """Swaps batch and trajectory axis.

    Arguments:
        policy: Policy reference
        seq_lens: Sequence lengths if recurrent or None
        tensor: A tensor or list of tensors to reshape.
        drop_last: A bool indicating whether to drop the last
        trajectory item.

    Returns:
        res: A tensor with swapped axes or a list of tensors with
        swapped axes.
    """
    if isinstance(tensor, (list, tuple)):
        return [
            make_time_major(policy, seq_lens, t, drop_last) for t in tensor
        ]

    if policy.is_recurrent():
        B = seq_lens.shape[0]
        T = tensor.shape[0] // B
    else:
        # Important: chop the tensor into batches at known episode cut
        # boundaries. TODO(ekl) this is kind of a hack
        T = policy.config["rollout_fragment_length"]
        B = tensor.shape[0] // T
    rs = torch.reshape(tensor, [B, T] + list(tensor.shape[1:]))

    # Swap B and T axes.
    res = torch.transpose(rs, 1, 0)

    if drop_last:
        return res[:-1]
    return res


def stats(policy, train_batch):
    values_batched = make_time_major(
        policy,
        train_batch.get("seq_lens"),
        policy.model.value_function(),
        drop_last=policy.config["vtrace"])

    return {
        "cur_lr": policy.cur_lr,
        "policy_loss": policy.loss.pi_loss,
        "entropy": policy.loss.entropy,
        "entropy_coeff": policy.entropy_coeff,
        "var_gnorm": global_norm(policy.model.trainable_variables()),
        "vf_loss": policy.loss.vf_loss,
        "vf_explained_var": explained_variance(
            torch.reshape(policy.loss.value_targets, [-1]),
            torch.reshape(values_batched, [-1]),
            framework="torch"),
    }


def choose_optimizer(policy, config):
    if policy.config["opt_type"] == "adam":
        return torch.optim.Adam(
            params=policy.model.parameters(), lr=policy.cur_lr)
    else:
        return torch.optim.RMSProp(
            params=policy.model.parameters(),
            lr=policy.cur_lr,
            weight_decay=config["decay"],
            momentum=config["momentum"],
            eps=config["epsilon"])


def setup_mixins(policy, obs_space, action_space, config):
    EntropyCoeffSchedule.__init__(policy, config["entropy_coeff"],
                                  config["entropy_coeff_schedule"])
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


VTraceTorchPolicy = build_torch_policy(
    name="VTraceTorchPolicy",
    loss_fn=build_vtrace_loss,
    get_default_config=lambda: ray.rllib.agents.impala.impala.DEFAULT_CONFIG,
    stats_fn=stats,
    postprocess_fn=postprocess_trajectory,
    extra_grad_process_fn=apply_grad_clipping,
    optimizer_fn=choose_optimizer,
    before_init=setup_mixins,
    mixins=[LearningRateSchedule, EntropyCoeffSchedule],
    get_batch_divisibility_req=lambda p: p.config["rollout_fragment_length"])
