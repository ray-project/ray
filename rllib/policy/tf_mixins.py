import logging
from typing import Dict, List

import numpy as np


from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.eager_tf_policy import EagerTFPolicy
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.policy import Policy, PolicyState
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import get_variable, try_import_tf
from ray.rllib.utils.schedules import PiecewiseSchedule
from ray.rllib.utils.tf_utils import make_tf_callable
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    LocalOptimizer,
    ModelGradients,
    TensorType,
)


logger = logging.getLogger(__name__)
tf1, tf, tfv = try_import_tf()


@Deprecated(error=False)
class LearningRateSchedule:
    """Mixin for TFPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self._lr_schedule = None
        # Disable any scheduling behavior related to learning if Learner API is active.
        # Schedules are handled by Learner class.
        if lr_schedule is None or self.config.get("_enable_learner_api", False):
            self.cur_lr = tf1.get_variable("lr", initializer=lr, trainable=False)
        else:
            self._lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None
            )
            self.cur_lr = tf1.get_variable(
                "lr", initializer=self._lr_schedule.value(0), trainable=False
            )
            if self.framework == "tf":
                self._lr_placeholder = tf1.placeholder(dtype=tf.float32, name="lr")
                self._lr_update = self.cur_lr.assign(
                    self._lr_placeholder, read_value=False
                )

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super().on_global_var_update(global_vars)
        if self._lr_schedule is not None:
            new_val = self._lr_schedule.value(global_vars["timestep"])
            if self.framework == "tf":
                self.get_session().run(
                    self._lr_update, feed_dict={self._lr_placeholder: new_val}
                )
            else:
                self.cur_lr.assign(new_val, read_value=False)
                # This property (self._optimizer) is (still) accessible for
                # both TFPolicy and any TFPolicy_eager.
                self._optimizer.learning_rate.assign(self.cur_lr)

    @override(TFPolicy)
    def optimizer(self):
        if self.framework == "tf":
            return tf1.train.AdamOptimizer(learning_rate=self.cur_lr)
        else:
            return tf.keras.optimizers.Adam(self.cur_lr)


@Deprecated(error=False)
class EntropyCoeffSchedule:
    """Mixin for TFPolicy that adds entropy coeff decay."""

    @DeveloperAPI
    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self._entropy_coeff_schedule = None
        # Disable any scheduling behavior related to learning if Learner API is active.
        # Schedules are handled by Learner class.
        if entropy_coeff_schedule is None or (
            self.config.get("_enable_learner_api", False)
        ):
            self.entropy_coeff = get_variable(
                entropy_coeff, framework="tf", tf_name="entropy_coeff", trainable=False
            )
        else:
            # Allows for custom schedule similar to lr_schedule format
            if isinstance(entropy_coeff_schedule, list):
                self._entropy_coeff_schedule = PiecewiseSchedule(
                    entropy_coeff_schedule,
                    outside_value=entropy_coeff_schedule[-1][-1],
                    framework=None,
                )
            else:
                # Implements previous version but enforces outside_value
                self._entropy_coeff_schedule = PiecewiseSchedule(
                    [[0, entropy_coeff], [entropy_coeff_schedule, 0.0]],
                    outside_value=0.0,
                    framework=None,
                )

            self.entropy_coeff = get_variable(
                self._entropy_coeff_schedule.value(0),
                framework="tf",
                tf_name="entropy_coeff",
                trainable=False,
            )
            if self.framework == "tf":
                self._entropy_coeff_placeholder = tf1.placeholder(
                    dtype=tf.float32, name="entropy_coeff"
                )
                self._entropy_coeff_update = self.entropy_coeff.assign(
                    self._entropy_coeff_placeholder, read_value=False
                )

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super().on_global_var_update(global_vars)
        if self._entropy_coeff_schedule is not None:
            new_val = self._entropy_coeff_schedule.value(global_vars["timestep"])
            if self.framework == "tf":
                self.get_session().run(
                    self._entropy_coeff_update,
                    feed_dict={self._entropy_coeff_placeholder: new_val},
                )
            else:
                self.entropy_coeff.assign(new_val, read_value=False)


@Deprecated(error=False)
class KLCoeffMixin:
    """Assigns the `update_kl()` and other KL-related methods to a TFPolicy.

    This is used in Algorithms to update the KL coefficient after each
    learning step based on `config.kl_target` and the measured KL value
    (from the train_batch).
    """

    def __init__(self, config: AlgorithmConfigDict):
        # The current KL value (as python float).
        self.kl_coeff_val = config["kl_coeff"]
        # The current KL value (as tf Variable for in-graph operations).
        self.kl_coeff = get_variable(
            float(self.kl_coeff_val),
            tf_name="kl_coeff",
            trainable=False,
            framework=config["framework"],
        )
        # Constant target value.
        self.kl_target = config["kl_target"]
        if self.framework == "tf":
            self._kl_coeff_placeholder = tf1.placeholder(
                dtype=tf.float32, name="kl_coeff"
            )
            self._kl_coeff_update = self.kl_coeff.assign(
                self._kl_coeff_placeholder, read_value=False
            )

    def update_kl(self, sampled_kl):
        # Update the current KL value based on the recently measured value.
        # Increase.
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff_val *= 1.5
        # Decrease.
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5
        # No change.
        else:
            return self.kl_coeff_val

        # Make sure, new value is also stored in graph/tf variable.
        self._set_kl_coeff(self.kl_coeff_val)

        # Return the current KL value.
        return self.kl_coeff_val

    def _set_kl_coeff(self, new_kl_coeff):
        # Set the (off graph) value.
        self.kl_coeff_val = new_kl_coeff

        # Update the tf/tf2 Variable (via session call for tf or `assign`).
        if self.framework == "tf":
            self.get_session().run(
                self._kl_coeff_update,
                feed_dict={self._kl_coeff_placeholder: self.kl_coeff_val},
            )
        else:
            self.kl_coeff.assign(self.kl_coeff_val, read_value=False)

    @override(Policy)
    def get_state(self) -> PolicyState:
        state = super().get_state()
        # Add current kl-coeff value.
        state["current_kl_coeff"] = self.kl_coeff_val
        return state

    @override(Policy)
    def set_state(self, state: PolicyState) -> None:
        # Set current kl-coeff value first.
        self._set_kl_coeff(state.pop("current_kl_coeff", self.config["kl_coeff"]))
        # Call super's set_state with rest of the state dict.
        super().set_state(state)


@Deprecated(error=False)
class TargetNetworkMixin:
    """Assign the `update_target` method to the policy.

    The function is called every `target_network_update_freq` steps by the
    master learner.
    """

    def __init__(self):
        if not self.config.get("_enable_rl_module_api", False):
            model_vars = self.model.trainable_variables()
            target_model_vars = self.target_model.trainable_variables()

            @make_tf_callable(self.get_session())
            def update_target_fn(tau):
                tau = tf.convert_to_tensor(tau, dtype=tf.float32)
                update_target_expr = []
                assert len(model_vars) == len(target_model_vars), (
                    model_vars,
                    target_model_vars,
                )
                for var, var_target in zip(model_vars, target_model_vars):
                    update_target_expr.append(
                        var_target.assign(tau * var + (1.0 - tau) * var_target)
                    )
                    logger.debug("Update target op {}".format(var_target))
                return tf.group(*update_target_expr)

            # Hard initial update.
            self._do_update = update_target_fn
            # TODO: The previous SAC implementation does an update(1.0) here.
            # If this is changed to tau != 1.0 the sac_loss_function test fails. Why?
            # Also the test is not very maintainable, we need to change that unittest
            # anyway.
            self.update_target(tau=1.0)  # self.config.get("tau", 1.0))

    @property
    def q_func_vars(self):
        if not hasattr(self, "_q_func_vars"):
            if self.config.get("_enable_rl_module_api", False):
                self._q_func_vars = self.model.variables
            else:
                self._q_func_vars = self.model.variables()
        return self._q_func_vars

    @property
    def target_q_func_vars(self):
        if not hasattr(self, "_target_q_func_vars"):
            if self.config.get("_enable_rl_module_api", False):
                self._target_q_func_vars = self.target_model.variables
            else:
                self._target_q_func_vars = self.target_model.variables()
        return self._target_q_func_vars

    # Support both hard and soft sync.
    def update_target(self, tau: int = None) -> None:
        self._do_update(np.float32(tau or self.config.get("tau", 1.0)))

    @override(TFPolicy)
    def variables(self) -> List[TensorType]:
        if self.config.get("_enable_rl_module_api", False):
            return self.model.variables
        else:
            return self.model.variables()

    def set_weights(self, weights):
        if isinstance(self, TFPolicy):
            TFPolicy.set_weights(self, weights)
        elif isinstance(self, EagerTFPolicyV2):  # Handle TF2V2 policies.
            EagerTFPolicyV2.set_weights(self, weights)
        elif isinstance(self, EagerTFPolicy):  # Handle TF2 policies.
            EagerTFPolicy.set_weights(self, weights)
        if not self.config.get("_enable_rl_module_api", False):
            self.update_target(self.config.get("tau", 1.0))


@Deprecated(error=False)
class ValueNetworkMixin:
    """Assigns the `_value()` method to a TFPolicy.

    This way, Policy can call `_value()` to get the current VF estimate on a
    single(!) observation (as done in `postprocess_trajectory_fn`).
    Note: When doing this, an actual forward pass is being performed.
    This is different from only calling `model.value_function()`, where
    the result of the most recent forward pass is being used to return an
    already calculated tensor.
    """

    def __init__(self, config):
        # When doing GAE or vtrace, we need the value function estimate on the
        # observation.
        if config.get("use_gae") or config.get("vtrace"):

            # Input dict is provided to us automatically via the Model's
            # requirements. It's a single-timestep (last one in trajectory)
            # input_dict.
            @make_tf_callable(self.get_session())
            def value(**input_dict):
                input_dict = SampleBatch(input_dict)
                if isinstance(self.model, tf.keras.Model):
                    _, _, extra_outs = self.model(input_dict)
                    return extra_outs[SampleBatch.VF_PREDS][0]
                else:
                    model_out, _ = self.model(input_dict)
                    # [0] = remove the batch dim.
                    return self.model.value_function()[0]

        # When not doing GAE, we do not require the value function's output.
        else:

            @make_tf_callable(self.get_session())
            def value(*args, **kwargs):
                return tf.constant(0.0)

        self._value = value
        self._should_cache_extra_action = config["framework"] == "tf"
        self._cached_extra_action_fetches = None

    def _extra_action_out_impl(self) -> Dict[str, TensorType]:
        extra_action_out = super().extra_action_out_fn()
        # Keras models return values for each call in third return argument
        # (dict).
        if isinstance(self.model, tf.keras.Model):
            return extra_action_out
        # Return value function outputs. VF estimates will hence be added to the
        # SampleBatches produced by the sampler(s) to generate the train batches
        # going into the loss function.
        extra_action_out.update(
            {
                SampleBatch.VF_PREDS: self.model.value_function(),
            }
        )
        return extra_action_out

    def extra_action_out_fn(self) -> Dict[str, TensorType]:
        if not self._should_cache_extra_action:
            return self._extra_action_out_impl()

        # Note: there are 2 reasons we are caching the extra_action_fetches for
        # TF1 static graph here.
        # 1. for better performance, so we don't query base class and model for
        #    extra fetches every single time.
        # 2. for correctness. TF1 is special because the static graph may contain
        #    two logical graphs. One created by DynamicTFPolicy for action
        #    computation, and one created by MultiGPUTower for GPU training.
        #    Depending on which logical graph ran last time,
        #    self.model.value_function() will point to the output tensor
        #    of the specific logical graph, causing problem if we try to
        #    fetch action (run inference) using the training output tensor.
        #    For that reason, we cache the action output tensor from the
        #    vanilla DynamicTFPolicy once and call it a day.
        if self._cached_extra_action_fetches is not None:
            return self._cached_extra_action_fetches

        self._cached_extra_action_fetches = self._extra_action_out_impl()
        return self._cached_extra_action_fetches


@Deprecated(error=False)
class GradStatsMixin:
    def __init__(self):
        pass

    def grad_stats_fn(
        self, train_batch: SampleBatch, grads: ModelGradients
    ) -> Dict[str, TensorType]:
        # We have support for more than one loss (list of lists of grads).
        if self.config.get("_tf_policy_handles_more_than_one_loss"):
            grad_gnorm = [tf.linalg.global_norm(g) for g in grads]
        # Old case: We have a single list of grads (only one loss term and
        # optimizer).
        else:
            grad_gnorm = tf.linalg.global_norm(grads)

        return {
            "grad_gnorm": grad_gnorm,
        }


# TODO: find a better place for this util, since it's not technically MixIns.
@Deprecated(error=False)
def compute_gradients(
    policy, optimizer: LocalOptimizer, loss: TensorType
) -> ModelGradients:
    # Compute the gradients.
    variables = policy.model.trainable_variables
    if isinstance(policy.model, ModelV2):
        variables = variables()
    grads_and_vars = optimizer.compute_gradients(loss, variables)

    # Clip by global norm, if necessary.
    if policy.config.get("grad_clip") is not None:
        # Defuse inf gradients (due to super large losses).
        grads = [g for (g, v) in grads_and_vars]
        grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
        # If the global_norm is inf -> All grads will be NaN. Stabilize this
        # here by setting them to 0.0. This will simply ignore destructive loss
        # calculations.
        policy.grads = []
        for g in grads:
            if g is not None:
                policy.grads.append(tf.where(tf.math.is_nan(g), tf.zeros_like(g), g))
            else:
                policy.grads.append(None)
        clipped_grads_and_vars = list(zip(policy.grads, variables))
        return clipped_grads_and_vars
    else:
        return grads_and_vars
