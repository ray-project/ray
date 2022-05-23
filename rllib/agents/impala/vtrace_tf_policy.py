"""Adapted from A3CTFPolicy to add V-trace.

Keep in sync with changes to A3CTFPolicy and VtraceSurrogatePolicy."""

import numpy as np
import logging
import gym

import ray
from ray.rllib.agents.impala import vtrace_tf as vtrace
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_mixins import LearningRateSchedule, EntropyCoeffSchedule
from ray.rllib.utils import force_list
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class VTraceLoss:
    def __init__(
        self,
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
        clip_pg_rho_threshold=1.0,
    ):
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

        # Compute vtrace on the CPU for better perf.
        with tf.device("/cpu:0"):
            self.vtrace_returns = vtrace.multi_from_logits(
                behaviour_action_log_probs=behaviour_action_logp,
                behaviour_policy_logits=behaviour_logits,
                target_policy_logits=target_logits,
                actions=tf.unstack(actions, axis=2),
                discounts=tf.cast(~tf.cast(dones, tf.bool), tf.float32) * discount,
                rewards=rewards,
                values=values,
                bootstrap_value=bootstrap_value,
                dist_class=dist_class,
                model=model,
                clip_rho_threshold=tf.cast(clip_rho_threshold, tf.float32),
                clip_pg_rho_threshold=tf.cast(clip_pg_rho_threshold, tf.float32),
            )
            self.value_targets = self.vtrace_returns.vs

        # The policy gradients loss.
        masked_pi_loss = tf.boolean_mask(
            actions_logp * self.vtrace_returns.pg_advantages, valid_mask
        )
        self.pi_loss = -tf.reduce_sum(masked_pi_loss)
        self.mean_pi_loss = -tf.reduce_mean(masked_pi_loss)

        # The baseline loss.
        delta = tf.boolean_mask(values - self.vtrace_returns.vs, valid_mask)
        delta_squarred = tf.math.square(delta)
        self.vf_loss = 0.5 * tf.reduce_sum(delta_squarred)
        self.mean_vf_loss = 0.5 * tf.reduce_mean(delta_squarred)

        # The entropy loss.
        masked_entropy = tf.boolean_mask(actions_entropy, valid_mask)
        self.entropy = tf.reduce_sum(masked_entropy)
        self.mean_entropy = tf.reduce_mean(masked_entropy)

        # The summed weighted loss.
        self.total_loss = self.pi_loss - self.entropy * entropy_coeff

        # Optional vf loss (or in a separate term due to separate
        # optimizers/networks).
        self.loss_wo_vf = self.total_loss
        if not config["_separate_vf_optimizer"]:
            self.total_loss += self.vf_loss * vf_loss_coeff


def _make_time_major(policy, seq_lens, tensor, drop_last=False):
    """Swaps batch and trajectory axis.

    Args:
        policy: Policy reference
        seq_lens: Sequence lengths if recurrent or None
        tensor: A tensor or list of tensors to reshape.
        drop_last: A bool indicating whether to drop the last
        trajectory item.

    Returns:
        res: A tensor with swapped axes or a list of tensors with
        swapped axes.
    """
    if isinstance(tensor, list):
        return [_make_time_major(policy, seq_lens, t, drop_last) for t in tensor]

    if policy.is_recurrent():
        B = tf.shape(seq_lens)[0]
        T = tf.shape(tensor)[0] // B
    else:
        # Important: chop the tensor into batches at known episode cut
        # boundaries.
        # TODO: (sven) this is kind of a hack and won't work for
        #  batch_mode=complete_episodes.
        T = policy.config["rollout_fragment_length"]
        B = tf.shape(tensor)[0] // T
    rs = tf.reshape(tensor, tf.concat([[B, T], tf.shape(tensor)[1:]], axis=0))

    # swap B and T axes
    res = tf.transpose(rs, [1, 0] + list(range(2, 1 + int(tf.shape(tensor).shape[0]))))

    if drop_last:
        return res[:-1]
    return res


def build_vtrace_loss(policy, model, dist_class, train_batch):
    model_out, _ = model(train_batch)
    action_dist = dist_class(model_out, model)

    if isinstance(policy.action_space, gym.spaces.Discrete):
        is_multidiscrete = False
        output_hidden_shape = [policy.action_space.n]
    elif isinstance(policy.action_space, gym.spaces.MultiDiscrete):
        is_multidiscrete = True
        output_hidden_shape = policy.action_space.nvec.astype(np.int32)
    else:
        is_multidiscrete = False
        output_hidden_shape = 1

    def make_time_major(*args, **kw):
        return _make_time_major(
            policy, train_batch.get(SampleBatch.SEQ_LENS), *args, **kw
        )

    actions = train_batch[SampleBatch.ACTIONS]
    dones = train_batch[SampleBatch.DONES]
    rewards = train_batch[SampleBatch.REWARDS]
    behaviour_action_logp = train_batch[SampleBatch.ACTION_LOGP]
    behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]
    unpacked_behaviour_logits = tf.split(behaviour_logits, output_hidden_shape, axis=1)
    unpacked_outputs = tf.split(model_out, output_hidden_shape, axis=1)
    values = model.value_function()

    if policy.is_recurrent():
        max_seq_len = tf.reduce_max(train_batch[SampleBatch.SEQ_LENS])
        mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
        mask = tf.reshape(mask, [-1])
    else:
        mask = tf.ones_like(rewards)

    # Prepare actions for loss
    loss_actions = actions if is_multidiscrete else tf.expand_dims(actions, axis=1)

    # Inputs are reshaped from [B * T] => [(T|T-1), B] for V-trace calc.
    drop_last = policy.config["vtrace_drop_last_ts"]
    policy.loss = VTraceLoss(
        actions=make_time_major(loss_actions, drop_last=drop_last),
        actions_logp=make_time_major(action_dist.logp(actions), drop_last=drop_last),
        actions_entropy=make_time_major(
            action_dist.multi_entropy(), drop_last=drop_last
        ),
        dones=make_time_major(dones, drop_last=drop_last),
        behaviour_action_logp=make_time_major(
            behaviour_action_logp, drop_last=drop_last
        ),
        behaviour_logits=make_time_major(
            unpacked_behaviour_logits, drop_last=drop_last
        ),
        target_logits=make_time_major(unpacked_outputs, drop_last=drop_last),
        discount=policy.config["gamma"],
        rewards=make_time_major(rewards, drop_last=drop_last),
        values=make_time_major(values, drop_last=drop_last),
        bootstrap_value=make_time_major(values)[-1],
        dist_class=Categorical if is_multidiscrete else dist_class,
        model=model,
        valid_mask=make_time_major(mask, drop_last=drop_last),
        config=policy.config,
        vf_loss_coeff=policy.config["vf_loss_coeff"],
        entropy_coeff=policy.entropy_coeff,
        clip_rho_threshold=policy.config["vtrace_clip_rho_threshold"],
        clip_pg_rho_threshold=policy.config["vtrace_clip_pg_rho_threshold"],
    )

    if policy.config.get("_separate_vf_optimizer"):
        return policy.loss.loss_wo_vf, policy.loss.vf_loss
    else:
        return policy.loss.total_loss


def stats(policy, train_batch):
    drop_last = policy.config["vtrace"] and policy.config["vtrace_drop_last_ts"]
    values_batched = _make_time_major(
        policy,
        train_batch.get(SampleBatch.SEQ_LENS),
        policy.model.value_function(),
        drop_last=drop_last,
    )

    return {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "policy_loss": policy.loss.mean_pi_loss,
        "entropy": policy.loss.mean_entropy,
        "entropy_coeff": tf.cast(policy.entropy_coeff, tf.float64),
        "var_gnorm": tf.linalg.global_norm(policy.model.trainable_variables()),
        "vf_loss": policy.loss.mean_vf_loss,
        "vf_explained_var": explained_variance(
            tf.reshape(policy.loss.value_targets, [-1]),
            tf.reshape(values_batched, [-1]),
        ),
    }


def grad_stats(policy, train_batch, grads):
    # We have support for more than one loss (list of lists of grads).
    if policy.config.get("_tf_policy_handles_more_than_one_loss"):
        grad_gnorm = [tf.linalg.global_norm(g) for g in grads]
    # Old case: We have a single list of grads (only one loss term and
    # optimizer).
    else:
        grad_gnorm = tf.linalg.global_norm(grads)

    return {
        "grad_gnorm": grad_gnorm,
    }


def choose_optimizer(policy, config):
    if policy.config["opt_type"] == "adam":
        if policy.config["framework"] in ["tf2", "tfe"]:
            optim = tf.keras.optimizers.Adam(policy.cur_lr)
            if policy.config["_separate_vf_optimizer"]:
                return optim, tf.keras.optimizers.Adam(policy.config["_lr_vf"])
        else:
            optim = tf1.train.AdamOptimizer(policy.cur_lr)
            if policy.config["_separate_vf_optimizer"]:
                return optim, tf1.train.AdamOptimizer(policy.config["_lr_vf"])
    else:
        if policy.config["_separate_vf_optimizer"]:
            raise ValueError(
                "RMSProp optimizer not supported for separate"
                "vf- and policy losses yet! Set `opt_type=adam`"
            )

        if tfv == 2:
            optim = tf.keras.optimizers.RMSprop(
                policy.cur_lr, config["decay"], config["momentum"], config["epsilon"]
            )
        else:
            optim = tf1.train.RMSPropOptimizer(
                policy.cur_lr, config["decay"], config["momentum"], config["epsilon"]
            )

    return optim


def clip_gradients(policy, optimizer, loss):
    # Supporting more than one loss/optimizer.
    if policy.config["_tf_policy_handles_more_than_one_loss"]:
        optimizers = force_list(optimizer)
        losses = force_list(loss)
        assert len(optimizers) == len(losses)
        clipped_grads_and_vars = []
        for optim, loss_ in zip(optimizers, losses):
            grads_and_vars = optim.compute_gradients(
                loss_, policy.model.trainable_variables()
            )
            clipped_g_and_v = []
            for g, v in grads_and_vars:
                if g is not None:
                    clipped_g, _ = tf.clip_by_global_norm(
                        [g], policy.config["grad_clip"]
                    )
                    clipped_g_and_v.append((clipped_g[0], v))
            clipped_grads_and_vars.append(clipped_g_and_v)

        policy.grads = [g for g_and_v in clipped_grads_and_vars for (g, v) in g_and_v]
    # Only one optimizer and and loss term.
    else:
        grads_and_vars = optimizer.compute_gradients(
            loss, policy.model.trainable_variables()
        )
        grads = [g for (g, v) in grads_and_vars]
        policy.grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
        clipped_grads_and_vars = list(
            zip(policy.grads, policy.model.trainable_variables())
        )

    return clipped_grads_and_vars


def setup_mixins(policy, obs_space, action_space, config):
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    EntropyCoeffSchedule.__init__(
        policy, config["entropy_coeff"], config["entropy_coeff_schedule"]
    )


VTraceTFPolicy = build_tf_policy(
    name="VTraceTFPolicy",
    get_default_config=lambda: ray.rllib.agents.impala.impala.DEFAULT_CONFIG,
    loss_fn=build_vtrace_loss,
    stats_fn=stats,
    grad_stats_fn=grad_stats,
    optimizer_fn=choose_optimizer,
    compute_gradients_fn=clip_gradients,
    before_loss_init=setup_mixins,
    mixins=[LearningRateSchedule, EntropyCoeffSchedule],
    get_batch_divisibility_req=lambda p: p.config["rollout_fragment_length"],
)
