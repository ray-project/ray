from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.torch.model import TorchModel
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.utils import get_activation_fn
from typing import List, TYPE_CHECKING

from torch import nn
import torch

if TYPE_CHECKING:
    from rllib.catalog.configs.encoder import VectorEncoderConfig


def input_to_output_spec(
    input_spec: ModelSpec, output_key: str, feature_dims: List[int]
) -> ModelSpec:
    """Convert an input spec to an output spec by replacing the input dims and
    key with the output dims and output key.

    E.g.
    input_to_output_spec(
        input_spec=ModelSpec({
            "bork": "a, b, c, d",
            "dork": "e, f, g, h"
            }, h=2, d=3
        ),
        output_key="foo",
        feature_dims=(2,)
    )

    should return:
    ModelSpec({"foo": "a, b, c, d"}, d=2)
    """
    num_feat_dims = len(feature_dims)
    assert num_feat_dims >= 1, "Must specify at least one feature dim"
    num_dims = [len(v.shape()) != len for v in input_spec.values()]
    assert all(
        [nd == num_dims[0] for nd in num_dims]
    ), "Inputs must all have the same number of dimensions"

    tspec = input_spec.rdrop(len(feature_dims)).append(feature_dims)
    return ModelSpec({output_key: tspec})


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
        config: "VectorEncoderConfig",
    ):
        self.config = config
        self._input_spec = input_spec
        self._output_spec = input_to_output_spec(
            input_spec, self.config.output_key, self.config.hidden_layer_sizes[-1]
        )
        # Returns the size of the feature dimension for the input tensors
        prev_size = sum([v.shape()[-1] for v in input_spec().values()])
        layers = []
        act = (
            None
            if config.activation == "linear"
            else get_activation_fn(config.activation)()
        )
        for size in self.layer_sizes[:-1]:
            layers += [nn.Linear(prev_size, size)]
            layers += [act] if act is not None else []
            prev_size = size
        # final layer
        layers += [
            nn.Linear(
                self.config.hidden_layer_sizes[-2], self.config.hidden_layer_sizes[-1]
            )
        ]
        layers += config.final_activation

        self.net = nn.Sequential(layers)

    def _forward(self, inputs: NestedDict) -> NestedDict:
        # Concatenate all input along the feature dim
        x = torch.cat(inputs.values(), dim=-1)
        [out_key] = self.output_spec.keys()
        inputs[out_key] = self.net(x)
        return inputs
