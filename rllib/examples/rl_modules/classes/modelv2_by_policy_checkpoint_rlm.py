from typing import Any, Dict

from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.torch import TorchRLModule
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override


class ModelV2ByPolicyCheckpointRLModule(TorchRLModule):
    """An RLModule containing a (old stack) ModelV2 through a policy checkpoint."""

    @override(TorchRLModule)
    def setup(self):
        super().setup()

        # Get the policy checkpoint from the `model_config_dict`.
        policy_checkpoint_dir = (
            self.config.model_config_dict.get("policy_checkpoint_dir")
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
        torch_policy = TorchPolicyV2.from_checkpoint(policy_checkpoint_dir)
        self._model_v2 = torch_policy.model
        # TODO (sven) What about custom dist classes?

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
        raise NotImplementedError
