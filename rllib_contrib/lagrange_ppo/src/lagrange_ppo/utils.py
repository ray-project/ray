import logging
from typing import List

from lagrange_ppo.cost_postprocessing import CostValuePostprocessing

from ray.rllib.execution.common import _check_sample_batch_type
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.policy.torch_mixins import ValueNetworkMixin
from ray.rllib.utils.annotations import DeveloperAPI, override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.schedules import PiecewiseSchedule
from ray.rllib.utils.typing import AlgorithmConfigDict, SampleBatchType

logger = logging.getLogger(__name__)


@Deprecated(error=False)
class CostAndValueNetworkMixins(ValueNetworkMixin):
    """Assigns the `_value()` method to a TorchPolicy.

    This way, Policy can call `_value()` to get the current VF estimate on a
    single(!) observation (as done in `postprocess_trajectory_fn`).
    Note: When doing this, an actual forward pass is being performed.
    This is different from only calling `model.value_function()`, where
    the result of the most recent forward pass is being used to return an
    already calculated tensor.
    """

    def __init__(self, config):
        super().__init__(config)

        # Repeating the process for the cost value function
        # When doing GAE, we need the cost value function estimate on the
        # observation.
        if config.get("use_cost_gae") or config.get("cvtrace"):
            # Input dict is provided to us automatically via the Model's
            # requirements. It's a single-timestep (last one in trajectory)
            # input_dict.

            def cost_value(**input_dict):
                input_dict = SampleBatch(input_dict)
                input_dict = self._lazy_tensor_dict(input_dict)
                model_out, _ = self.model(input_dict)
                # [0] = remove the batch dim.
                return self.model.cost_value_function()[0].item()

        # When not doing GAE, we do not require the value function's output.
        else:

            def cost_value(*args, **kwargs):
                return 0.0

        self._cost_value = cost_value

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
        # Return value function outputs. VF and CVF estimates will hence be added to
        # the SampleBatches produced by the sampler(s) to generate the train
        # batches going into the loss function.
        res_dict = super().extra_action_out(
            input_dict, state_batches, model, action_dist
        )
        res_dict.update(
            {
                CostValuePostprocessing.VF_PREDS: model.cost_value_function(),
            }
        )
        return res_dict


@Deprecated(error=False)
class LagrangeCoeffLearningRateSchedule:
    """Mixin for TorchPolicy that adds a learning rate schedule."""

    @DeveloperAPI
    def __init__(self, lr, lr_schedule):
        self._lr_schedule = None
        # Disable any scheduling behavior related to learning if Learner API is active.
        # Schedules are handled by Learner class.
        if lr_schedule is None:
            self.cur_lr = lr
        else:
            self._lr_schedule = PiecewiseSchedule(
                lr_schedule, outside_value=lr_schedule[-1][-1], framework=None
            )
            self.cur_lr = self._lr_schedule.value(0)

    @override(Policy)
    def on_global_var_update(self, global_vars):
        super().on_global_var_update(global_vars)
        if self._lr_schedule and not self.config.get("_enable_learner_api", False):
            self.cur_lr = self._lr_schedule.value(global_vars["timestep"])
            for opt in self._optimizers:
                for p in opt.param_groups:
                    p["lr"] = self.cur_lr


def substract_average(samples: SampleBatchType, fields: List[str]) -> SampleBatchType:
    """Substract its average from the given SampleBatch"""
    _check_sample_batch_type(samples)
    wrapped = False

    if isinstance(samples, SampleBatch):
        samples = samples.as_multi_agent()
        wrapped = True

    for policy_id in samples.policy_batches:
        batch = samples.policy_batches[policy_id]
        for field in fields:
            if field in batch:
                batch[field] -= batch[field].mean()

    if wrapped:
        samples = samples.policy_batches[DEFAULT_POLICY_ID]

    return samples


def validate_config(config: AlgorithmConfigDict) -> None:
    """Executed before Policy is "initialized" (at beginning of constructor).
    Args:
        config: The Policy's config.
    """
    # If vf_share_layers is True, inform about the need to tune vf_loss_coeff.
    if config.get("model", {}).get("vf_share_layers") is True:
        logger.info(
            "`vf_share_layers=True` in your model. "
            "Therefore, remember to tune the value of `vf_loss_coeff`!"
        )
