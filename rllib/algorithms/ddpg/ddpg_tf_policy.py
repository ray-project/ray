from functools import partial
import logging
import gym
from typing import Dict, Tuple, List, Type, Union, Optional, Any

import ray
import ray.experimental.tf_utils
from ray.rllib.algorithms.ddpg.utils import make_ddpg_models, validate_spaces
from ray.rllib.algorithms.dqn.dqn_tf_policy import (
    postprocess_nstep_and_prio,
    PRIO_WEIGHTS,
)
from ray.rllib.evaluation import Episode
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import (
    Deterministic,
    Dirichlet,
    TFActionDistribution,
)
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.tf_mixins import TargetNetworkMixin
from ray.rllib.utils.annotations import override
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import get_variable, try_import_tf
from ray.rllib.utils.spaces.simplex import Simplex
from ray.rllib.utils.tf_utils import huber_loss, make_tf_callable
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    TensorType,
    LocalOptimizer,
    ModelGradients,
)
from ray.util.debug import log_once

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class ComputeTDErrorMixin:
    def __init__(self: Union[DynamicTFPolicyV2, EagerTFPolicyV2]):
        @make_tf_callable(self.get_session(), dynamic_shape=True)
        def compute_td_error(
            obs_t, act_t, rew_t, obs_tp1, done_mask, importance_weights
        ):
            input_dict = SampleBatch(
                {
                    SampleBatch.CUR_OBS: tf.convert_to_tensor(obs_t),
                    SampleBatch.ACTIONS: tf.convert_to_tensor(act_t),
                    SampleBatch.REWARDS: tf.convert_to_tensor(rew_t),
                    SampleBatch.NEXT_OBS: tf.convert_to_tensor(obs_tp1),
                    SampleBatch.DONES: tf.convert_to_tensor(done_mask),
                    PRIO_WEIGHTS: tf.convert_to_tensor(importance_weights),
                }
            )
            # Do forward pass on loss to update td errors attribute
            # (one TD-error value per item in batch to update PR weights).
            self.loss(self.model, None, input_dict)
            # `self.td_error` is set in loss_fn.
            return self.td_error

        self.compute_td_error = compute_td_error


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_ddpg_tf_policy(
    name: str, base: Type[Union[DynamicTFPolicyV2, EagerTFPolicyV2]]
) -> Type:
    """Construct a DDPGTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.
    Returns:
        A TF Policy to be used with DDPG.
    """

    class DDPGTFPolicy(TargetNetworkMixin, ComputeTDErrorMixin, base):
        def __init__(
            self,
            observation_space: gym.spaces.Space,
            action_space: gym.spaces.Space,
            config: AlgorithmConfigDict,
            *,
            existing_inputs: Optional[Dict[str, "tf1.placeholder"]] = None,
            existing_model: Optional[ModelV2] = None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            config = dict(
                ray.rllib.algorithms.ddpg.ddpg.DDPGConfig().to_dict(), **config
            )

            # Validate action space for DDPG
            validate_spaces(self, observation_space, action_space)

            base.__init__(
                self,
                observation_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            ComputeTDErrorMixin.__init__(self)

            self.maybe_initialize_optimizer_and_loss()

            TargetNetworkMixin.__init__(self)

        @override(base)
        def make_model(self) -> ModelV2:
            return make_ddpg_models(self)

        @override(base)
        def optimizer(
            self,
        ) -> List["tf.keras.optimizers.Optimizer"]:
            """Create separate optimizers for actor & critic losses."""
            if self.config["framework"] in ["tf2", "tfe"]:
                self.global_step = get_variable(0, tf_name="global_step")
                self._actor_optimizer = tf.keras.optimizers.Adam(
                    learning_rate=self.config["actor_lr"]
                )
                self._critic_optimizer = tf.keras.optimizers.Adam(
                    learning_rate=self.config["critic_lr"]
                )
            # Static graph mode.
            else:
                self.global_step = tf1.train.get_or_create_global_step()
                self._actor_optimizer = tf1.train.AdamOptimizer(
                    learning_rate=self.config["actor_lr"]
                )
                self._critic_optimizer = tf1.train.AdamOptimizer(
                    learning_rate=self.config["critic_lr"]
                )
            return [self._actor_optimizer, self._critic_optimizer]

        @override(base)
        def compute_gradients_fn(
            self, optimizer: LocalOptimizer, loss: TensorType
        ) -> ModelGradients:
            if self.config["framework"] in ["tf2", "tfe"]:
                tape = optimizer.tape
                pol_weights = self.model.policy_variables()
                actor_grads_and_vars = list(
                    zip(tape.gradient(self.actor_loss, pol_weights), pol_weights)
                )
                q_weights = self.model.q_variables()
                critic_grads_and_vars = list(
                    zip(tape.gradient(self.critic_loss, q_weights), q_weights)
                )
            else:
                actor_grads_and_vars = self._actor_optimizer.compute_gradients(
                    self.actor_loss, var_list=self.model.policy_variables()
                )
                critic_grads_and_vars = self._critic_optimizer.compute_gradients(
                    self.critic_loss, var_list=self.model.q_variables()
                )

            # Clip if necessary.
            if self.config["grad_clip"]:
                clip_func = partial(tf.clip_by_norm, clip_norm=self.config["grad_clip"])
            else:
                clip_func = tf.identity

            # Save grads and vars for later use in `build_apply_op`.
            self._actor_grads_and_vars = [
                (clip_func(g), v) for (g, v) in actor_grads_and_vars if g is not None
            ]
            self._critic_grads_and_vars = [
                (clip_func(g), v) for (g, v) in critic_grads_and_vars if g is not None
            ]

            grads_and_vars = self._actor_grads_and_vars + self._critic_grads_and_vars

            return grads_and_vars

        @override(base)
        def apply_gradients_fn(
            self,
            optimizer: "tf.keras.optimizers.Optimizer",
            grads: ModelGradients,
        ) -> "tf.Operation":
            # For policy gradient, update policy net one time v.s.
            # update critic net `policy_delay` time(s).
            should_apply_actor_opt = tf.equal(
                tf.math.floormod(self.global_step, self.config["policy_delay"]), 0
            )

            def make_apply_op():
                return self._actor_optimizer.apply_gradients(self._actor_grads_and_vars)

            actor_op = tf.cond(
                should_apply_actor_opt,
                true_fn=make_apply_op,
                false_fn=lambda: tf.no_op(),
            )
            critic_op = self._critic_optimizer.apply_gradients(
                self._critic_grads_and_vars
            )
            # Increment global step & apply ops.
            if self.config["framework"] in ["tf2", "tfe"]:
                self.global_step.assign_add(1)
                return tf.no_op()
            else:
                with tf1.control_dependencies([tf1.assign_add(self.global_step, 1)]):
                    return tf.group(actor_op, critic_op)

        @override(base)
        def action_distribution_fn(
            self,
            model: ModelV2,
            *,
            obs_batch: TensorType,
            state_batches: TensorType,
            is_training: bool = False,
            **kwargs,
        ) -> Tuple[TensorType, type, List[TensorType]]:
            model_out, _ = model(SampleBatch(obs=obs_batch, _is_training=is_training))
            dist_inputs = model.get_policy_output(model_out)

            if isinstance(self.action_space, Simplex):
                distr_class = Dirichlet
            else:
                distr_class = Deterministic
            return dist_inputs, distr_class, []  # []=state out

        @override(base)
        def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[Dict[Any, SampleBatch]] = None,
            episode: Optional[Episode] = None,
        ) -> SampleBatch:
            return postprocess_nstep_and_prio(
                self, sample_batch, other_agent_batches, episode
            )

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> TensorType:
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
            target_model_out_tp1, _ = self.target_model(input_dict_next, [], None)

            self._target_q_func_vars = self.target_model.variables()

            # Policy network evaluation.
            policy_t = model.get_policy_output(model_out_t)
            policy_tp1 = self.target_model.get_policy_output(target_model_out_tp1)

            # Action outputs.
            if self.config["smooth_target_policy"]:
                target_noise_clip = self.config["target_noise_clip"]
                clipped_normal_sample = tf.clip_by_value(
                    tf.random.normal(
                        tf.shape(policy_tp1), stddev=self.config["target_noise"]
                    ),
                    -target_noise_clip,
                    target_noise_clip,
                )
                policy_tp1_smoothed = tf.clip_by_value(
                    policy_tp1 + clipped_normal_sample,
                    self.action_space.low * tf.ones_like(policy_tp1),
                    self.action_space.high * tf.ones_like(policy_tp1),
                )
            else:
                # No smoothing, just use deterministic actions.
                policy_tp1_smoothed = policy_tp1

            # Q-net(s) evaluation.
            # prev_update_ops = set(tf.get_collection(tf.GraphKeys.UPDATE_OPS))
            # Q-values for given actions & observations in given current
            q_t = model.get_q_values(model_out_t, train_batch[SampleBatch.ACTIONS])

            # Q-values for current policy (no noise) in given current state
            q_t_det_policy = model.get_q_values(model_out_t, policy_t)

            if twin_q:
                twin_q_t = model.get_twin_q_values(
                    model_out_t, train_batch[SampleBatch.ACTIONS]
                )

            # Target q-net(s) evaluation.
            q_tp1 = self.target_model.get_q_values(
                target_model_out_tp1, policy_tp1_smoothed
            )

            if twin_q:
                twin_q_tp1 = self.target_model.get_twin_q_values(
                    target_model_out_tp1, policy_tp1_smoothed
                )

            q_t_selected = tf.squeeze(q_t, axis=len(q_t.shape) - 1)
            if twin_q:
                twin_q_t_selected = tf.squeeze(twin_q_t, axis=len(q_t.shape) - 1)
                q_tp1 = tf.minimum(q_tp1, twin_q_tp1)

            q_tp1_best = tf.squeeze(input=q_tp1, axis=len(q_tp1.shape) - 1)
            q_tp1_best_masked = (
                1.0 - tf.cast(train_batch[SampleBatch.DONES], tf.float32)
            ) * q_tp1_best

            # Compute RHS of bellman equation.
            q_t_selected_target = tf.stop_gradient(
                tf.cast(train_batch[SampleBatch.REWARDS], tf.float32)
                + gamma ** n_step * q_tp1_best_masked
            )

            # Compute the error (potentially clipped).
            if twin_q:
                td_error = q_t_selected - q_t_selected_target
                twin_td_error = twin_q_t_selected - q_t_selected_target
                if use_huber:
                    errors = huber_loss(td_error, huber_threshold) + huber_loss(
                        twin_td_error, huber_threshold
                    )
                else:
                    errors = 0.5 * tf.math.square(td_error) + 0.5 * tf.math.square(
                        twin_td_error
                    )
            else:
                td_error = q_t_selected - q_t_selected_target
                if use_huber:
                    errors = huber_loss(td_error, huber_threshold)
                else:
                    errors = 0.5 * tf.math.square(td_error)

            critic_loss = tf.reduce_mean(
                tf.cast(train_batch[PRIO_WEIGHTS], tf.float32) * errors
            )
            actor_loss = -tf.reduce_mean(q_t_det_policy)

            # Add l2-regularization if required.
            if l2_reg is not None:
                for var in self.model.policy_variables():
                    if "bias" not in var.name:
                        actor_loss += l2_reg * tf.nn.l2_loss(var)
                for var in self.model.q_variables():
                    if "bias" not in var.name:
                        critic_loss += l2_reg * tf.nn.l2_loss(var)

            # Model self-supervised losses.
            if self.config["use_state_preprocessor"]:
                # Expand input_dict in case custom_loss' need them.
                input_dict[SampleBatch.ACTIONS] = train_batch[SampleBatch.ACTIONS]
                input_dict[SampleBatch.REWARDS] = train_batch[SampleBatch.REWARDS]
                input_dict[SampleBatch.DONES] = train_batch[SampleBatch.DONES]
                input_dict[SampleBatch.NEXT_OBS] = train_batch[SampleBatch.NEXT_OBS]
                if log_once("ddpg_custom_loss"):
                    logger.warning(
                        "You are using a state-preprocessor with DDPG and "
                        "therefore, `custom_loss` will be called on your Model! "
                        "Please be aware that DDPG now uses the ModelV2 API, which "
                        "merges all previously separate sub-models (policy_model, "
                        "q_model, and twin_q_model) into one ModelV2, on which "
                        "`custom_loss` is called, passing it "
                        "[actor_loss, critic_loss] as 1st argument. "
                        "You may have to change your custom loss function to handle "
                        "this."
                    )
                [actor_loss, critic_loss] = model.custom_loss(
                    [actor_loss, critic_loss], input_dict
                )

            # Store values for stats function.
            self.actor_loss = actor_loss
            self.critic_loss = critic_loss
            self.td_error = td_error
            self.q_t = q_t

            # Return one loss value (even though we treat them separately in our
            # 2 optimizers: actor and critic).
            return self.critic_loss + self.actor_loss

        @override(base)
        def extra_learn_fetches_fn(self) -> Dict[str, Any]:
            return {"td_error": self.td_error}

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            stats = {
                "mean_q": tf.reduce_mean(self.q_t),
                "max_q": tf.reduce_max(self.q_t),
                "min_q": tf.reduce_min(self.q_t),
            }
            return stats

    DDPGTFPolicy.__name__ = name
    DDPGTFPolicy.__qualname__ = name

    return DDPGTFPolicy


DDPGTF1Policy = get_ddpg_tf_policy("DDPGTF1Policy", DynamicTFPolicyV2)
DDPGTF2Policy = get_ddpg_tf_policy("DDPGTF2Policy", EagerTFPolicyV2)
