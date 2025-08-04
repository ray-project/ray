from ray.rllib.policy.policy import PolicyState
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.schedules import PiecewiseSchedule

torch, nn = try_import_torch()


@OldAPIStack
class LearningRateSchedule:
    """Mixin for TorchPolicy that adds a learning rate schedule."""

    def __init__(self, lr, lr_schedule, lr2=None, lr2_schedule=None):
        self._lr_schedule = None
        self._lr2_schedule = None
        # Disable any scheduling behavior related to learning if Learner API is active.
        # Schedules are handled by Learner class.
        if lr_schedule is None:
            self.cur_lr = lr
        else:
            self._lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None
            )
            self.cur_lr = self._lr_schedule.value(0)
        if lr2_schedule is None:
            self.cur_lr2 = lr2
        else:
            self._lr2_schedule = PiecewiseSchedule(
                lr2_schedule, outside_value=lr2_schedule[-1][-1], framework=None
            )
            self.cur_lr2 = self._lr2_schedule.value(0)

    def on_global_var_update(self, global_vars):
        super().on_global_var_update(global_vars)
        if self._lr_schedule:
            self.cur_lr = self._lr_schedule.value(global_vars["timestep"])
            for opt in self._optimizers:
                for p in opt.param_groups:
                    p["lr"] = self.cur_lr
        if self._lr2_schedule:
            assert len(self._optimizers) == 2
            self.cur_lr2 = self._lr2_schedule.value(global_vars["timestep"])
            opt = self._optimizers[1]
            for p in opt.param_groups:
                p["lr"] = self.cur_lr2


@OldAPIStack
class EntropyCoeffSchedule:
    """Mixin for TorchPolicy that adds entropy coeff decay."""

    def __init__(self, entropy_coeff, entropy_coeff_schedule):
        self._entropy_coeff_schedule = None
        # Disable any scheduling behavior related to learning if Learner API is active.
        # Schedules are handled by Learner class.
        if entropy_coeff_schedule is None:
            self.entropy_coeff = entropy_coeff
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
            self.entropy_coeff = self._entropy_coeff_schedule.value(0)

    def on_global_var_update(self, global_vars):
        super(EntropyCoeffSchedule, self).on_global_var_update(global_vars)
        if self._entropy_coeff_schedule is not None:
            self.entropy_coeff = self._entropy_coeff_schedule.value(
                global_vars["timestep"]
            )


@OldAPIStack
class KLCoeffMixin:
    """Assigns the `update_kl()` method to a TorchPolicy.

    This is used by Algorithms to update the KL coefficient
    after each learning step based on `config.kl_target` and
    the measured KL value (from the train_batch).
    """

    def __init__(self, config):
        # The current KL value (as python float).
        self.kl_coeff = config["kl_coeff"]
        # Constant target value.
        self.kl_target = config["kl_target"]

    def update_kl(self, sampled_kl):
        # Update the current KL value based on the recently measured value.
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff *= 0.5
        # Return the current KL value.
        return self.kl_coeff

    def get_state(self) -> PolicyState:
        state = super().get_state()
        # Add current kl-coeff value.
        state["current_kl_coeff"] = self.kl_coeff
        return state

    def set_state(self, state: PolicyState) -> None:
        # Set current kl-coeff value first.
        self.kl_coeff = state.pop("current_kl_coeff", self.config["kl_coeff"])
        # Call super's set_state with rest of the state dict.
        super().set_state(state)


@OldAPIStack
class ValueNetworkMixin:
    """Assigns the `_value()` method to a TorchPolicy.

    This way, Policy can call `_value()` to get the current VF estimate on a
    single(!) observation (as done in `postprocess_trajectory_fn`).
    Note: When doing this, an actual forward pass is being performed.
    This is different from only calling `model.value_function()`, where
    the result of the most recent forward pass is being used to return an
    already calculated tensor.
    """

    def __init__(self, config):
        # When doing GAE, we need the value function estimate on the
        # observation.
        if config.get("use_gae") or config.get("vtrace"):
            # Input dict is provided to us automatically via the Model's
            # requirements. It's a single-timestep (last one in trajectory)
            # input_dict.

            def value(**input_dict):
                input_dict = SampleBatch(input_dict)
                input_dict = self._lazy_tensor_dict(input_dict)
                model_out, _ = self.model(input_dict)
                # [0] = remove the batch dim.
                return self.model.value_function()[0].item()

        # When not doing GAE, we do not require the value function's output.
        else:

            def value(*args, **kwargs):
                return 0.0

        self._value = value

    def extra_action_out(self, input_dict, state_batches, model, action_dist):
        """Defines extra fetches per action computation.

        Args:
            input_dict (Dict[str, TensorType]): The input dict used for the action
                computing forward pass.
            state_batches (List[TensorType]): List of state tensors (empty for
                non-RNNs).
            model (ModelV2): The Model object of the Policy.
            action_dist: The instantiated distribution
                object, resulting from the model's outputs and the given
                distribution class.

        Returns:
            Dict[str, TensorType]: Dict with extra tf fetches to perform per
                action computation.
        """
        # Return value function outputs. VF estimates will hence be added to
        # the SampleBatches produced by the sampler(s) to generate the train
        # batches going into the loss function.
        return {
            SampleBatch.VF_PREDS: model.value_function(),
        }


@OldAPIStack
class TargetNetworkMixin:
    """Mixin class adding a method for (soft) target net(s) synchronizations.

    - Adds the `update_target` method to the policy.
      Calling `update_target` updates all target Q-networks' weights from their
      respective "main" Q-networks, based on tau (smooth, partial updating).
    """

    def __init__(self):
        # Hard initial update from Q-net(s) to target Q-net(s).
        tau = self.config.get("tau", 1.0)
        self.update_target(tau=tau)

    def update_target(self, tau=None):
        # Update_target_fn will be called periodically to copy Q network to
        # target Q network, using (soft) tau-synching.
        tau = tau or self.config.get("tau", 1.0)

        model_state_dict = self.model.state_dict()

        # Support partial (soft) synching.
        # If tau == 1.0: Full sync from Q-model to target Q-model.
        # Support partial (soft) synching.
        # If tau == 1.0: Full sync from Q-model to target Q-model.
        target_state_dict = next(iter(self.target_models.values())).state_dict()
        model_state_dict = {
            k: tau * model_state_dict[k] + (1 - tau) * v
            for k, v in target_state_dict.items()
        }

        for target in self.target_models.values():
            target.load_state_dict(model_state_dict)

    def set_weights(self, weights):
        # Makes sure that whenever we restore weights for this policy's
        # model, we sync the target network (from the main model)
        # at the same time.
        TorchPolicy.set_weights(self, weights)
        self.update_target()
