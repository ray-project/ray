from ray.rllib.utils.nested_dict import NestedDict
from rllib.models.torch.model import TorchModel
from torch import nn
from ray.rllib.models.specs.specs_dict import ModelSpec
from dataclasses import dataclass
from typing import Any, Callable, List


def get_feature_size(spec: ModelSpec) -> int:
    # TODO: Make this work for multiple inputs
    assert len(spec) == 1, "Not yet implemented for multiple inputs"
    shape = spec.values()[0].get_shape()[-1]
    return shape


class TorchVectorEncoder(TorchModel):
    @property
    def input_spec(self) -> ModelSpec:
        return self._in_spec

    @property
    def output_spec(self) -> ModelSpec:
        return self.config.out_spec

    def __init__(self, in_spec: ModelSpec, config: "TorchVectorEncoderConfig"):
        self.config = config
        self._in_spec = in_spec
        self._out_spec = self.config.output_spec
        prev_size = get_feature_size(self.input_spec)
        layers = []
        for size in self.layer_sizes[:-1]:
            layers += [
                nn.Linear(prev_size, size),
                config.activation,
            ]
            prev_size = size
        # final layer
        layers += [nn.Linear(self.layer_sizes[-1], get_feature_size(self.output_spec))]
        layers += config.final_activation

        self.net = nn.Sequential(layers)

    def _forward(self, inputs: NestedDict) -> NestedDict:
        inputs[self.output_spec.keys()[0]] = self.net(inputs.values())
        return inputs


@dataclass
class TorchVectorEncoderConfig:
    out_spec: ModelSpec
    activation: Callable[[Any], Any] = nn.ReLU()
    final_activation: Callable[[Any], Any] = nn.Identity()
    hidden_layer_sizes: List[int] = [128, 128]
    out_spec: ModelSpec = ModelSpec({"b, f"}, f=128)

    def build(self, in_spec: ModelSpec) -> TorchVectorEncoder:
        return TorchVectorEncoder(in_spec, self)
