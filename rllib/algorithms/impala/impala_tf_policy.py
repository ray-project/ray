"""Adapted from A3CTFPolicy to add V-trace.

Keep in sync with changes to A3CTFPolicy and VtraceSurrogatePolicy."""

import numpy as np
import logging
import gymnasium as gym
from typing import Dict, List, Optional, Type, Union

from ray.rllib.algorithms.impala import vtrace_tf as vtrace
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import compute_bootstrap_value
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import Categorical, TFActionDistribution
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import LearningRateSchedule, EntropyCoeffSchedule
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.policy.tf_mixins import GradStatsMixin, ValueNetworkMixin
from ray.rllib.utils.typing import (
    LocalOptimizer,
    ModelGradients,
    TensorType,
    TFPolicyV2Type,
)

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
            config: Algorithm config dict.
        """

        # Compute vtrace on the CPU for better performance.
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


def _make_time_major(policy, seq_lens, tensor):
    """Swaps batch and trajectory axis.

    Args:
        policy: Policy reference
        seq_lens: Sequence lengths if recurrent or None
        tensor: A tensor or list of tensors to reshape.
        trajectory item.

    Returns:
        res: A tensor with swapped axes or a list of tensors with
        swapped axes.
    """
    if isinstance(tensor, list):
        return [_make_time_major(policy, seq_lens, t) for t in tensor]

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

    return res


class VTraceClipGradients:
    """VTrace version of gradient computation logic."""

    def __init__(self):
        """No special initialization required."""
        pass

    def compute_gradients_fn(
        self, optimizer: LocalOptimizer, loss: TensorType
    ) -> ModelGradients:
        # Supporting more than one loss/optimizer.
        if self.config.get("enable_rl_module_and_learner", False):
            # In order to access the variables for rl modules, we need to
            # use the underlying keras api model.trainable_variables.
            trainable_variables = self.model.trainable_variables
        else:
            trainable_variables = self.model.trainable_variables()
        if self.config["_tf_policy_handles_more_than_one_loss"]:
            optimizers = force_list(optimizer)
            losses = force_list(loss)
            assert len(optimizers) == len(losses)
            clipped_grads_and_vars = []
            for optim, loss_ in zip(optimizers, losses):
                grads_and_vars = optim.compute_gradients(loss_, trainable_variables)
                clipped_g_and_v = []
                for g, v in grads_and_vars:
                    if g is not None:
                        clipped_g, _ = tf.clip_by_global_norm(
                            [g], self.config["grad_clip"]
                        )
                        clipped_g_and_v.append((clipped_g[0], v))
                clipped_grads_and_vars.append(clipped_g_and_v)

            self.grads = [g for g_and_v in clipped_grads_and_vars for (g, v) in g_and_v]
        # Only one optimizer and and loss term.
        else:
            grads_and_vars = optimizer.compute_gradients(
                loss, self.model.trainable_variables()
            )
            grads = [g for (g, v) in grads_and_vars]
            self.grads, _ = tf.clip_by_global_norm(grads, self.config["grad_clip"])
            clipped_grads_and_vars = list(zip(self.grads, trainable_variables))

        return clipped_grads_and_vars


class VTraceOptimizer:
    """Optimizer function for VTrace policies."""

    def __init__(self):
        pass

    # TODO: maybe standardize this function, so the choice of optimizers are more
    #  predictable for common algorithms.
    def optimizer(
        self,
    ) -> Union["tf.keras.optimizers.Optimizer", List["tf.keras.optimizers.Optimizer"]]:
        config = self.config
        if config["opt_type"] == "adam":
            if config["framework"] == "tf2":
                optim = tf.keras.optimizers.Adam(self.cur_lr)
                if config["_separate_vf_optimizer"]:
                    return optim, tf.keras.optimizers.Adam(config["_lr_vf"])
            else:
                optim = tf1.train.AdamOptimizer(self.cur_lr)
                if config["_separate_vf_optimizer"]:
                    return optim, tf1.train.AdamOptimizer(config["_lr_vf"])
        else:
            if config["_separate_vf_optimizer"]:
                raise ValueError(
                    "RMSProp optimizer not supported for separate"
                    "vf- and policy losses yet! Set `opt_type=adam`"
                )

            if tfv == 2:
                optim = tf.keras.optimizers.RMSprop(
                    self.cur_lr, config["decay"], config["momentum"], config["epsilon"]
                )
            else:
                optim = tf1.train.RMSPropOptimizer(
                    self.cur_lr, config["decay"], config["momentum"], config["epsilon"]
                )

        return optim


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_impala_tf_policy(name: str, base: TFPolicyV2Type) -> TFPolicyV2Type:
    """Construct an ImpalaTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with Impala.
    """
    # VTrace mixins are placed in front of more general mixins to make sure
    # their functions like optimizer() overrides all the other implementations
    # (e.g., LearningRateSchedule.optimizer())
    class ImpalaTFPolicy(
        VTraceClipGradients,
        VTraceOptimizer,
        LearningRateSchedule,
        EntropyCoeffSchedule,
        GradStatsMixin,
        ValueNetworkMixin,
        base,
    ):
        def __init__(
            self,
            observation_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            # Initialize base class.
            base.__init__(
                self,
                observation_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )
            ValueNetworkMixin.__init__(self, config)

            # If Learner API is used, we don't need any loss-specific mixins.
            # However, we also would like to avoid creating special Policy-subclasses
            # for this as the entire Policy concept will soon not be used anymore with
            # the new Learner- and RLModule APIs.
            if not self.config.get("enable_rl_module_and_learner"):
                GradStatsMixin.__init__(self)
                VTraceClipGradients.__init__(self)
                VTraceOptimizer.__init__(self)
                LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
                EntropyCoeffSchedule.__init__(
                    self, config["entropy_coeff"], config["entropy_coeff_schedule"]
                )

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            model_out, _ = model(train_batch)
            action_dist = dist_class(model_out, model)

            if isinstance(self.action_space, gym.spaces.Discrete):
                is_multidiscrete = False
                output_hidden_shape = [self.action_space.n]
            elif isinstance(self.action_space, gym.spaces.MultiDiscrete):
                is_multidiscrete = True
                output_hidden_shape = self.action_space.nvec.astype(np.int32)
            else:
                is_multidiscrete = False
                output_hidden_shape = 1

            def make_time_major(*args, **kw):
                return _make_time_major(
                    self, train_batch.get(SampleBatch.SEQ_LENS), *args, **kw
                )

            actions = train_batch[SampleBatch.ACTIONS]
            dones = train_batch[SampleBatch.TERMINATEDS]
            rewards = train_batch[SampleBatch.REWARDS]
            behaviour_action_logp = train_batch[SampleBatch.ACTION_LOGP]
            behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]
            unpacked_behaviour_logits = tf.split(
                behaviour_logits, output_hidden_shape, axis=1
            )
            unpacked_outputs = tf.split(model_out, output_hidden_shape, axis=1)
            values = model.value_function()
            values_time_major = make_time_major(values)
            bootstrap_values_time_major = make_time_major(
                train_batch[SampleBatch.VALUES_BOOTSTRAPPED]
            )
            bootstrap_value = bootstrap_values_time_major[-1]

            if self.is_recurrent():
                max_seq_len = tf.reduce_max(train_batch[SampleBatch.SEQ_LENS])
                mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
                mask = tf.reshape(mask, [-1])
            else:
                mask = tf.ones_like(rewards)

            # Prepare actions for loss
            loss_actions = (
                actions if is_multidiscrete else tf.expand_dims(actions, axis=1)
            )

            # Inputs are reshaped from [B * T] => [(T|T-1), B] for V-trace calc.
            self.vtrace_loss = VTraceLoss(
                actions=make_time_major(loss_actions),
                actions_logp=make_time_major(action_dist.logp(actions)),
                actions_entropy=make_time_major(action_dist.multi_entropy()),
                dones=make_time_major(dones),
                behaviour_action_logp=make_time_major(behaviour_action_logp),
                behaviour_logits=make_time_major(unpacked_behaviour_logits),
                target_logits=make_time_major(unpacked_outputs),
                discount=self.config["gamma"],
                rewards=make_time_major(rewards),
                values=values_time_major,
                bootstrap_value=bootstrap_value,
                dist_class=Categorical if is_multidiscrete else dist_class,
                model=model,
                valid_mask=make_time_major(mask),
                config=self.config,
                vf_loss_coeff=self.config["vf_loss_coeff"],
                entropy_coeff=self.entropy_coeff,
                clip_rho_threshold=self.config["vtrace_clip_rho_threshold"],
                clip_pg_rho_threshold=self.config["vtrace_clip_pg_rho_threshold"],
            )

            if self.config.get("_separate_vf_optimizer"):
                return self.vtrace_loss.loss_wo_vf, self.vtrace_loss.vf_loss
            else:
                return self.vtrace_loss.total_loss

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            values_batched = _make_time_major(
                self,
                train_batch.get(SampleBatch.SEQ_LENS),
                self.model.value_function(),
            )

            return {
                "cur_lr": tf.cast(self.cur_lr, tf.float64),
                "policy_loss": self.vtrace_loss.mean_pi_loss,
                "entropy": self.vtrace_loss.mean_entropy,
                "entropy_coeff": tf.cast(self.entropy_coeff, tf.float64),
                "var_gnorm": tf.linalg.global_norm(self.model.trainable_variables()),
                "vf_loss": self.vtrace_loss.mean_vf_loss,
                "vf_explained_var": explained_variance(
                    tf.reshape(self.vtrace_loss.value_targets, [-1]),
                    tf.reshape(values_batched, [-1]),
                ),
            }

        @override(base)
        def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[SampleBatch] = None,
            episode: Optional["Episode"] = None,
        ):
            # Call super's postprocess_trajectory first.
            # sample_batch = super().postprocess_trajectory(
            #    sample_batch, other_agent_batches, episode
            # )

            if self.config["vtrace"]:
                # Add the SampleBatch.VALUES_BOOTSTRAPPED column, which we'll need
                # inside the loss for vtrace calculations.
                sample_batch = compute_bootstrap_value(sample_batch, self)

            return sample_batch

        @override(base)
        def get_batch_divisibility_req(self) -> int:
            return self.config["rollout_fragment_length"]

    ImpalaTFPolicy.__name__ = name
    ImpalaTFPolicy.__qualname__ = name

    return ImpalaTFPolicy


ImpalaTF1Policy = get_impala_tf_policy("ImpalaTF1Policy", DynamicTFPolicyV2)
ImpalaTF2Policy = get_impala_tf_policy("ImpalaTF2Policy", EagerTFPolicyV2)
