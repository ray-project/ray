from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import deprecation_warning
from ray.util import log_once


class TorchNoopModel(TorchModelV2):
    """Trivial model that just returns the obs flattened.

    This is the model used if use_state_preprocessor=False."""

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        if log_once("torch_noop_model_deprecation"):
            deprecation_warning(old="ray.rllib.models.torch.noop.TorchNoopModel")
        return input_dict["obs_flat"].float(), state
