from typing import Any, Dict

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.apis import ValueFunctionAPI
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDiagGaussian,
    TorchMultiCategorical,
    TorchMultiDistribution,
    TorchSquashedGaussian,
)
from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical as OldTorchCategorical,
    TorchDiagGaussian as OldTorchDiagGaussian,
    TorchMultiActionDistribution as OldTorchMultiActionDistribution,
    TorchMultiCategorical as OldTorchMultiCategorical,
    TorchSquashedGaussian as OldTorchSquashedGaussian,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override


class ModelV2ToRLModule(TorchRLModule, ValueFunctionAPI):
    """An RLModule containing a (old stack) ModelV2, provided by a policy checkpoint."""

    @override(TorchRLModule)
    def setup(self):
        super().setup()

        # Get the policy checkpoint from the `model_config_dict`.
        policy_checkpoint_dir = self.config.model_config_dict.get(
            "policy_checkpoint_dir"
        )
        if policy_checkpoint_dir is None:
            raise ValueError(
                "The `model_config_dict` of your RLModule must contain a "
                "`policy_checkpoint_dir` key pointing to the policy checkpoint "
                "directory! You can find this dir under the Algorithm's checkpoint dir "
                "in subdirectory: [algo checkpoint dir]/policies/[policy ID, e.g. "
                "`default_policy`]."
            )

        # Create a temporary policy object.
        policy = TorchPolicyV2.from_checkpoint(policy_checkpoint_dir)
        self._model_v2 = policy.model

        # Translate the action dist classes from the old API stack to the new.
        self._action_dist_class = self._translate_dist_class(policy.dist_class)

        # Erase the torch policy from memory, so it can be garbage collected.
        del policy

    def _forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        nn_output, state_out = self._model_v2(batch)
        # Interpret the NN output as action logits.
        output = {Columns.ACTION_DIST_INPUTS: nn_output}
        # Add the `state_out` to the `output`, new API stack style.
        if state_out:
            output[Columns.STATE_OUT] = {}
        for i, o in enumerate(state_out):
            output[Columns.STATE_OUT][i] = o

        return output

    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return self._forward_inference(batch, **kwargs)

    def _forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        out = self._forward_inference(batch, **kwargs)
        out[Columns.ACTION_LOGP] = self._action_dist_class(
            out[Columns.ACTION_DIST_INPUTS]
        ).logp(batch[Columns.ACTIONS])
        out[Columns.VF_PREDS] = self._model_v2.value_function()
        return out

    def compute_values(self, batch: Dict[str, Any]):
        self._model_v2(batch)
        return self._model_v2.value_function()

    def get_inference_action_dist_cls(self):
        return self._action_dist_class

    def get_exploration_action_dist_cls(self):
        return self._action_dist_class

    def get_train_action_dist_cls(self):
        return self._action_dist_class

    def _translate_dist_class(self, old_dist_class):
        map_ = {
            OldTorchCategorical: TorchCategorical,
            OldTorchDiagGaussian: TorchDiagGaussian,
            OldTorchMultiActionDistribution: TorchMultiDistribution,
            OldTorchMultiCategorical: TorchMultiCategorical,
            OldTorchSquashedGaussian: TorchSquashedGaussian,
        }
        if old_dist_class not in map_:
            raise ValueError(
                f"ModelV2ToRLModule does NOT support {old_dist_class} action "
                f"distributions yet!"
            )

        return map_[old_dist_class]
