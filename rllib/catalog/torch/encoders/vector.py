from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.torch.model import TorchModel
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.utils import get_activation_fn
from typing import TYPE_CHECKING

from torch import nn
import torch

if TYPE_CHECKING:
    from rllib.catalog.configs.encoder import VectorEncoderConfig


class TorchVectorEncoder(TorchModel):
    """An MLP encoder"""

    @property
    def input_spec(self) -> ModelSpec:
        return self._input_spec

    @property
    def output_spec(self) -> ModelSpec:
        return self._output_spec

    def __init__(
        self,
        input_spec: ModelSpec,
        output_spec: ModelSpec,
        config: "VectorEncoderConfig",
    ):
        self.config = config
        self._input_spec = input_spec
        self._output_spec = output_spec
        # Returns the size of the feature dimension for the input tensors
        prev_size = sum([v.shape()[-1] for v in input_spec().values()])
        layers = []
        act = (
            nn.Identity()
            if config.activation == "linear"
            else get_activation_fn(config.activation)()
        )
        for size in self.layer_sizes[:-1]:
            layers += [nn.Linear(prev_size, size), act]
            prev_size = size
        # final layer
        layers += [nn.Linear(self.layer_sizes[-1], self.output_spec.shape[-1])]
        layers += config.final_activation

        self.net = nn.Sequential(layers)

    def _forward(self, inputs: NestedDict) -> NestedDict:
        # Concatenate all input along the feature dim
        x = torch.cat(inputs.values(), dim=-1)
        [out_key] = self.output_spec.keys()
        inputs[out_key] = self.net(x)
        return inputs
