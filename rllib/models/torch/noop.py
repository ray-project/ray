from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated


@Deprecated(error=False)
class TorchNoopModel(TorchModelV2):
    """Trivial model that just returns the obs flattened.

    This is the model used if use_state_preprocessor=False."""

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        return input_dict["obs_flat"].float(), state
