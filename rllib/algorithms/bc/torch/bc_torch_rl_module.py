from typing import Any, Mapping

from ray.rllib.algorithms.bc.bc_rl_module import BCRLModule
from ray.rllib.core.models.base import ENCODER_OUT, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class BCTorchRLModule(TorchRLModule, BCRLModule):
    framework: str = "torch"

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        """BC forward pass during inference. 
        
        See the `BCTorchRLModule._forward_exploration` method for 
        implementation details.
        """
        return self._forward_exploration(batch)

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        """BC forward pass during exploration.
        
        Besides the action distribution this method also returns a possible
        state in case a stateful encoder is used. 

        Note that for BC `_forward_train`, `_forward_exploration`, and 
        `_forward_inference` return the same items and therefore only 
        `_forward_exploration` is implemented and is used by the two other 
        forward methods.
        """
        output = {}

        # State encodings.
        encoder_outs = self.encoder(batch)
        # Stateful encoder case.
        if STATE_OUT in encoder_outs:
            output[STATE_OUT] = encoder_outs[STATE_OUT]

        # Actions.
        action_logits = self.pi(encoder_outs[ENCODER_OUT])
        output[SampleBatch.ACTION_DIST_INPUTS] = action_logits

        return output

    @override(RLModule)
    def _forward_train(self, batch: NestedDict):
        """BC forward pass during training. 
        
        See the `BCTorchRLModule._forward_exploration` method for 
        implementation details.
        """
        return self._forward_exploration(batch)
