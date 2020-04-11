from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchNoopModel(TorchModelV2, nn.Module):
    """Trivial model that just returns the obs flattened.

    This is the model used if use_state_preprocessor=False."""
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

    @override(TorchModelV2)
    def forward(self, input_dict, state, seq_lens):
        return input_dict["obs_flat"].float(), state
